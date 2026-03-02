use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{Path, PathBuf, entry::IndexEntry, payload::FstPayload};
use fs4::fs_std::FileExt;
use fst::{Map, MapBuilder};
use memmap2::Mmap;
use thiserror::Error;

pub(crate) mod compactor;

const LOCK_FILE: &str = ".minidex.lock";

pub const SEGMENT_EXT: &str = "seg";
pub const DATA_EXT: &str = "dat";

pub(crate) struct Segment {
    map: Option<Map<Vec<u8>>>,
    data: Option<Mmap>,
    path: PathBuf,
    deleted: AtomicBool,
}

impl Segment {
    pub fn load(path: PathBuf) -> Result<Self, SegmentedIndexError> {
        let entry_file_path = path.with_extension(SEGMENT_EXT);
        let mut entry_file = File::open(&entry_file_path).map_err(SegmentedIndexError::Io)?;
        let mut buf = Vec::new();
        entry_file
            .read_to_end(&mut buf)
            .map_err(SegmentedIndexError::Io)?;

        let map = Map::new(buf).map_err(SegmentedIndexError::Fst)?;

        // Load the data file for the same segment
        let dat_file_path = path.with_extension(DATA_EXT);
        let dat_file = File::open(dat_file_path).map_err(SegmentedIndexError::Io)?;
        let data = unsafe { Mmap::map(&dat_file).map_err(SegmentedIndexError::Io)? };
        Ok(Self {
            map: Some(map),
            data: Some(data),
            path,
            deleted: AtomicBool::new(false),
        })
    }

    pub fn mark_deleted(&self) {
        self.deleted.store(true, Ordering::SeqCst);
    }

    pub(crate) fn get_entry(&self, offset: u64) -> Option<IndexEntry> {
        let data = self.data.as_ref()?;
        let start = offset as usize;
        let end = start + IndexEntry::SIZE;

        if end > data.len() {
            None
        } else {
            Some(IndexEntry::from_bytes(&data[start..end]))
        }
    }
}

impl AsRef<Map<Vec<u8>>> for Segment {
    fn as_ref(&self) -> &Map<Vec<u8>> {
        &self.map.as_ref().unwrap()
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::SeqCst) {
            self.map.take();
            self.data.take();

            let _ = std::fs::remove_file(self.path.with_extension(SEGMENT_EXT));
            let _ = std::fs::remove_file(self.path.with_extension(DATA_EXT));
        }
    }
}

/// A `SegmentedIndex` contains the (on-disk) segments
/// that are committed with index data.
pub(crate) struct SegmentedIndex {
    segments: Vec<Arc<Segment>>,
    _lockfile: File,
}

impl SegmentedIndex {
    /// Open an on-disk index, locking the target directory and reading all
    /// segment files found in it.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, SegmentedIndexError> {
        std::fs::create_dir_all(&dir)?;
        let lock_path = dir.as_ref().join(LOCK_FILE);
        let lockfile = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)
            .map_err(SegmentedIndexError::Io)?;

        lockfile
            .try_lock_exclusive()
            .map_err(SegmentedIndexError::LockfileError)?;

        let entries = std::fs::read_dir(&dir)?;

        let mut result = Self {
            segments: Vec::new(),
            _lockfile: lockfile,
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == SEGMENT_EXT) {
                let file_name = path.file_name().unwrap_or_default().to_string_lossy();
                if file_name.contains(".tmp") {
                    println!("Cleaning up orphaned temporary file: {}", file_name);

                    // We can safely delete the .seg and its matching .dat file
                    let _ = std::fs::remove_file(&path);
                    let _ = std::fs::remove_file(path.with_extension(DATA_EXT));

                    continue; // Skip loading!
                }
                result.load(entry.path())?;
            }
        }

        Ok(result)
    }

    pub fn load<P: AsRef<Path>>(&mut self, path: P) -> Result<(), SegmentedIndexError> {
        let segment = Segment::load(path.as_ref().to_path_buf())?;

        Ok(self.segments.push(Arc::new(segment)))
    }

    pub fn snapshot(&self) -> Vec<Arc<Segment>> {
        self.segments.clone()
    }

    pub fn segments(&self) -> impl Iterator<Item = &Arc<Segment>> {
        self.segments.iter()
    }

    pub fn write_segment<I>(&self, segment_path: &PathBuf, it: I) -> Result<(), SegmentedIndexError>
    where
        I: Iterator<Item = (String, IndexEntry)>,
    {
        let seg_path = segment_path.with_added_extension(SEGMENT_EXT);
        let data_path = segment_path.with_added_extension(DATA_EXT);

        let seg_file = File::create_new(seg_path).map_err(SegmentedIndexError::Io)?;
        let mut seg_writer = BufWriter::with_capacity(4 * 1024 * 1024, seg_file);

        let dat_file = File::create(&data_path).map_err(SegmentedIndexError::Io)?;
        let mut dat_writer = BufWriter::with_capacity(4 * 1024 * 1024, dat_file);

        let mut builder = MapBuilder::new(&mut seg_writer).map_err(SegmentedIndexError::Fst)?;

        let mut current_offset = 0u64;
        for (path, entry) in it {
            let bytes = entry.to_bytes();
            dat_writer.write_all(&bytes)?;

            let ext = std::path::Path::new(&path)
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("");

            let payload = FstPayload::pack(current_offset, entry.kind as u8, &ext.to_lowercase());

            builder
                .insert(path, payload)
                .map_err(SegmentedIndexError::Fst)?;
            current_offset += bytes.len() as u64;
        }

        dat_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()
            .map_err(SegmentedIndexError::Io)?;

        builder.finish().map_err(SegmentedIndexError::Fst)?;
        seg_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()
            .map_err(SegmentedIndexError::Io)?;

        Ok(())
    }

    /// Atomically swaps out old segments for a newly compacted segment,
    /// and cleans up the old files from disk.
    pub fn apply_compaction(
        &mut self,
        old_segments: &[Arc<Segment>],
        tmp_path: PathBuf,
    ) -> Result<(), SegmentedIndexError> {
        let tmp_seg = tmp_path.with_added_extension(SEGMENT_EXT);
        let tmp_dat = tmp_path.with_added_extension(DATA_EXT);

        let final_path_str = tmp_path.to_string_lossy().replace(".tmp", "");
        let final_path = PathBuf::from(final_path_str);

        let final_seg = final_path.with_extension(SEGMENT_EXT);
        let final_dat = final_path.with_extension(DATA_EXT);

        std::fs::rename(tmp_seg, &final_seg).map_err(SegmentedIndexError::Io)?;

        std::fs::rename(tmp_dat, &final_dat).map_err(SegmentedIndexError::Io)?;

        let new_seg = Arc::new(Segment::load(final_path)?);

        self.segments
            .retain(|active_seg| !old_segments.iter().any(|old| Arc::ptr_eq(active_seg, old)));

        self.segments.push(new_seg);

        for old_seg in old_segments {
            old_seg.mark_deleted();
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SegmentedIndexError {
    #[error(
        "failed to create lockfile, this typically means there is another instance of an index running in the same directory"
    )]
    LockfileError(std::io::Error),
    #[error(transparent)]
    Io(std::io::Error),
    #[error(transparent)]
    Fst(fst::Error),
}

impl From<std::io::Error> for SegmentedIndexError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
