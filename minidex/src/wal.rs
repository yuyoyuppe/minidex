use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use crate::entry::IndexEntry;

const WAL_RECORD_INSERT: u8 = 0;
const WAL_RECORD_TOMBSTONE: u8 = 1;

pub struct Wal {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
}

impl Wal {
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            path,
            writer: Some(BufWriter::new(file)),
        })
    }

    pub(crate) fn append(
        &mut self,
        path: &str,
        volume: &str,
        entry: &IndexEntry,
    ) -> std::io::Result<()> {
        let writer = self.writer.as_mut().expect("WAL writer missing");

        let path_bytes = path.as_bytes();
        let path_len = path_bytes.len() as u32;

        let volume_bytes = volume.as_bytes();
        let volume_len = volume_bytes.len() as u32;

        writer.write_all(&[WAL_RECORD_INSERT])?;
        writer.write_all(&path_len.to_le_bytes())?;
        writer.write_all(path_bytes)?;
        writer.write_all(&volume_len.to_le_bytes())?;
        writer.write_all(volume_bytes)?;
        writer.write_all(&entry.to_bytes())?;
        Ok(())
    }

    pub fn write_prefix_tombstone(&mut self, prefix: &str, seq: u64) -> std::io::Result<()> {
        let writer = self.writer.as_mut().expect("WAL writer missing");
        let prefix_bytes = prefix.as_bytes();

        writer.write_all(&[WAL_RECORD_TOMBSTONE])?;

        writer.write_all(&seq.to_le_bytes())?;

        writer.write_all(&(prefix_bytes.len() as u32).to_le_bytes())?;

        writer.write_all(prefix_bytes)?;

        writer.flush()?;
        writer.get_ref().sync_data()?;

        Ok(())
    }

    pub(crate) fn flush(&mut self) -> std::io::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        Ok(())
    }

    pub(crate) fn replay<P: AsRef<Path>>(path: P) -> std::io::Result<ReplayData> {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ReplayData::new()),
            Err(e) => return Err(e),
        };

        let mut reader = BufReader::new(file);
        let mut results = ReplayData::new();
        let mut len_buf = [0u8; 4];

        loop {
            let mut type_buf = [0u8; 1];

            if let Err(e) = reader.read_exact(&mut type_buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break; // Reached the end of the WAL normally
                }
                return Err(e);
            }

            match type_buf[0] {
                WAL_RECORD_INSERT => {
                    match reader.read_exact(&mut len_buf) {
                        Ok(_) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e),
                    }
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut path_buf = vec![0u8; len];

                    if let Err(e) = reader.read_exact(&mut path_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            break;
                        }
                        return Err(e);
                    }
                    let path = String::from_utf8_lossy(&path_buf).to_string();

                    if let Err(e) = reader.read_exact(&mut len_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            log::warn!(
                                "Wal parser hit EOF early! Stopped at {}",
                                results.inserts.len()
                            );
                            break;
                        }
                        return Err(e);
                    }
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut vol_buf = vec![0u8; len];

                    if let Err(e) = reader.read_exact(&mut vol_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            break;
                        }
                        return Err(e);
                    }

                    let volume = String::from_utf8_lossy(&vol_buf).to_string();

                    let mut entry_buf = [0u8; IndexEntry::SIZE];
                    if let Err(e) = reader.read_exact(&mut entry_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            log::warn!(
                                "Wal parser hit EOF early! Stopped at {}",
                                results.inserts.len()
                            );
                            break;
                        }
                        return Err(e);
                    }
                    let entry = IndexEntry::from_bytes(&entry_buf);

                    results.inserts.push((path, volume, entry));
                }
                WAL_RECORD_TOMBSTONE => {
                    let mut seq_buf = [0u8; 8];
                    reader.read_exact(&mut seq_buf)?;
                    let seq = u64::from_le_bytes(seq_buf);

                    let mut len_buf = [0u8; 4];
                    reader.read_exact(&mut len_buf)?;
                    let len = u32::from_le_bytes(len_buf) as usize;

                    let mut prefix_buf = vec![0u8; len];
                    reader.read_exact(&mut prefix_buf)?;
                    let prefix = String::from_utf8(prefix_buf)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    results.tombstones.push((prefix, seq));
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Corrupted WAL",
                    ));
                }
            }
        }

        log::debug!(
            "Wal replay complete, recovered {} entries",
            results.inserts.len()
        );
        Ok(results)
    }

    pub(crate) fn rotate<P: AsRef<Path>>(&mut self, path: P) -> std::io::Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        std::fs::rename(&self.path, path)?;

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        self.writer = Some(BufWriter::new(new_file));

        Ok(())
    }
}

pub(crate) struct ReplayData {
    pub inserts: Vec<(String, String, IndexEntry)>,
    pub tombstones: Vec<(String, u64)>,
}

impl ReplayData {
    fn new() -> Self {
        Self {
            inserts: Vec::new(),
            tombstones: Vec::new(),
        }
    }
}
