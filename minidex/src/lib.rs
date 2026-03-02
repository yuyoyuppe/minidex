use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, RwLock, atomic::AtomicU64},
    thread::JoinHandle,
    time::SystemTime,
};

use fst::{IntoStreamer, Streamer};

use payload::FstPayload;
use thiserror::Error;

mod common;
pub use common::Kind;
use common::*;
mod entry;
pub use entry::FilesystemEntry;
use entry::*;
mod matcher;
use matcher::*;
mod segmented_index;
use segmented_index::{compactor::CompactorConfig, *};
mod opstamp;
use opstamp::*;
use wal::Wal;
mod payload;
mod wal;

/// A Minidex Index, managing both the in-memory and disk data.
/// Data that is never `commit`ted is transient and will be lost
/// if the `Index` is dropped.
pub struct Index {
    path: PathBuf,
    base: Arc<RwLock<SegmentedIndex>>,
    next_op_seq: AtomicU64,
    mem_idx: RwLock<BTreeMap<String, IndexEntry>>,
    wal: RwLock<Wal>,
    compactor_config: segmented_index::compactor::CompactorConfig,
    compactor: Arc<RwLock<Option<JoinHandle<()>>>>,
    flusher: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl Index {
    /// Open the index on disk with a default compactor configuration.
    /// This function will:
    /// 1. Create (if it doesn't exist) the directory at `path`
    /// 2. Try to obtain a lock on the directory
    /// 3. Obtain the last commited Opstamp (if possible)
    /// 4. Load the discovered segments.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, IndexError> {
        Self::open_with_config(path, CompactorConfig::default())
    }

    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        compactor_config: CompactorConfig,
    ) -> Result<Self, IndexError> {
        let base = SegmentedIndex::open(&path).map_err(IndexError::SegmentedIndex)?;

        let last_op = Self::generate_op_seq();

        let base = Arc::new(RwLock::new(base));
        let next_op_seq = AtomicU64::new(last_op);
        let mut mem_idx = BTreeMap::new();
        let wal_path = path.as_ref().join("journal.wal");

        let recovered = Wal::replay(&wal_path).map_err(IndexError::Io)?;
        for (k, v) in recovered {
            mem_idx.insert(k, v);
        }

        let wal = Wal::open(&wal_path).map_err(IndexError::Io)?;

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            base,
            next_op_seq,
            mem_idx: RwLock::new(mem_idx),
            wal: RwLock::new(wal),
            compactor_config,
            compactor: Arc::new(RwLock::new(None)),
            flusher: Arc::new(RwLock::new(None)),
        })
    }

    fn next_op_seq(&self) -> u64 {
        self.next_op_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn insert(&self, item: FilesystemEntry) -> Result<(), IndexError> {
        let seq = self.next_op_seq();
        let path_str = item.path.to_string_lossy().to_string();
        let entry = IndexEntry {
            opstamp: Opstamp::insertion(seq),
            kind: item.kind,
            last_modified: item.last_modified,
            last_accessed: item.last_accessed,
        };

        {
            let mut wal = self.wal.write().map_err(|_| IndexError::WriteLock)?;
            wal.append(&path_str, &entry).map_err(IndexError::Io)?;
        }

        {
            self.mem_idx
                .write()
                .map_err(|_| IndexError::WriteLock)?
                .insert(path_str, entry);
        }

        if self.should_flush() {
            let _ = self.trigger_flush();
        }

        Ok(())
    }

    pub fn delete(&self, item: &PathBuf) -> Result<(), IndexError> {
        let seq = self.next_op_seq();

        let path_str = item.to_string_lossy().to_string();
        let entry = IndexEntry {
            opstamp: Opstamp::deletion(seq),
            kind: Kind::File,
            last_modified: 0,
            last_accessed: 0,
        };

        {
            let mut wal = self.wal.write().map_err(|_| IndexError::WriteLock)?;
            wal.append(&path_str, &entry).map_err(IndexError::Io)?;
        }

        {
            self.mem_idx
                .write()
                .map_err(|_| IndexError::WriteLock)?
                .insert(path_str, entry);
        }

        if self.should_flush() {
            let _ = self.trigger_flush();
        }

        Ok(())
    }

    /// Writes the in-memory index to disk.
    /// This method can fail if the disk is not writable.
    pub fn commit(&self) -> Result<(), IndexError> {
        let mut wal = self.wal.write().map_err(|_| IndexError::WriteLock)?;
        wal.flush().map_err(IndexError::Io)?;

        Ok(())
    }

    /// Deletes modifications that exist in memory and have not yet
    /// been committed.
    pub fn rollback(&self) -> Result<(), IndexError> {
        let mut mem = self.mem_idx.write().map_err(|_| IndexError::WriteLock)?;

        mem.clear();

        Ok(())
    }

    pub fn search(&self, query: &str) -> Result<Vec<SearchResult>, IndexError> {
        let mut pattern = String::from("(?i)(?s).*");

        for word in query.split_whitespace() {
            for ch in word.chars() {
                let escaped = regex_syntax::escape(&ch.to_string());
                pattern.push_str(&escaped);
            }
            pattern.push_str(".*");
        }

        let matcher = RegexMatcher::new(&pattern).map_err(|e| IndexError::Regex(e.to_string()))?;
        let segments = self.base.read().map_err(|_| IndexError::ReadLock)?;
        let mem = self.mem_idx.read().map_err(|_| IndexError::ReadLock)?;

        let mut candidates: HashMap<String, (String, IndexEntry)> = HashMap::new();

        for (path, entry) in mem.iter() {
            if matcher.is_match(path) {
                let normalized = path.to_lowercase();

                candidates
                    .entry(normalized)
                    .and_modify(|(current_path, current_entry)| {
                        if entry.opstamp.sequence() > current_entry.opstamp.sequence() {
                            *current_entry = *entry;
                            *current_path = path.clone()
                        }
                    })
                    .or_insert((path.clone(), *entry));
            }
        }

        for segment in segments.segments() {
            let mut stream = segment.as_ref().as_ref().search(&matcher).into_stream();
            while let Some((term, payload)) = stream.next() {
                let offset = FstPayload::unpack_offset(payload);

                if let Some(entry) = segment.get_entry(offset) {
                    let path = std::str::from_utf8(term).expect("invalid term").to_string();

                    let key = path.to_lowercase();
                    candidates
                        .entry(key)
                        .and_modify(|(current_path, current_entry)| {
                            let current_seq = current_entry.opstamp.sequence();
                            let new_seq = entry.opstamp.sequence();
                            if new_seq > current_seq {
                                *current_entry = entry;
                                *current_path = path.clone();
                            }
                        })
                        .or_insert((path, entry));
                }
            }
        }

        let mut results = Vec::new();
        for (_, (path, entry)) in candidates {
            if !entry.opstamp.is_deletion() {
                results.push(SearchResult {
                    path: PathBuf::from(path),
                    kind: entry.kind,
                    last_modified: entry.last_modified,
                    last_accessed: entry.last_accessed,
                });
            }
        }

        results.sort();
        Ok(results)
    }

    pub fn force_compact_all(&self) -> Result<(), IndexError> {
        if let Ok(mut flusher) = self.flusher.write() {
            if let Some(handle) = flusher.take() {
                println!("Waiting for background flush to finish...");
                let _ = handle.join();
            }
        }

        if let Ok(mut compactor) = self.compactor.write() {
            if let Some(handle) = compactor.take() {
                println!("Waiting for background compactor to finish...");
                let _ = handle.join();
            }
        }

        let snapshot = {
            let base = self.base.read().map_err(|_| IndexError::ReadLock)?;
            let segments = base.snapshot();

            // If we have 1 or 0 segments, the database is already perfectly compacted!
            if segments.len() <= 1 {
                println!("Database is already fully compacted.");
                return Ok(());
            }
            segments
        };

        println!("Forcing full compaction of {} segments...", snapshot.len());

        let compactor_seq = Self::generate_op_seq();

        let tmp_path = self.path.join(format!("{}.tmp", compactor_seq));

        compactor::merge_segments(&snapshot, tmp_path.clone())
            .map_err(|e| IndexError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        let mut base_guard = self.base.write().map_err(|_| IndexError::WriteLock)?;
        base_guard
            .apply_compaction(&snapshot, tmp_path)
            .map_err(|e| IndexError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        println!("Full compaction complete");
        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.mem_idx.read().unwrap().len() > 50_000
    }

    fn trigger_flush(&self) -> Result<(), IndexError> {
        if let Some(ref flusher) = *self.flusher.read().expect("failed to read flusher")
            && !flusher.is_finished()
        {
            return Ok(());
        }
        let mut mem = self.mem_idx.write().expect("failed to lock memory");
        let mut wal = self.wal.write().expect("failed to lock wal");

        if mem.is_empty() {
            return Ok(());
        }

        let snapshot = std::mem::take(&mut *mem);
        let path = self.path.clone();
        let next_seq = self.next_op_seq();

        let flushing_path = path.join(format!("journal.{}.flushing.wal", next_seq));
        wal.rotate(&flushing_path).map_err(IndexError::Io)?;

        drop(wal);
        drop(mem);

        let base = Arc::clone(&self.base);
        let min_merge_count = self.compactor_config.min_merge_count;
        let compactor_lock = Arc::clone(&self.compactor);

        let flusher = std::thread::Builder::new()
            .name("minidex-flush".to_owned())
            .spawn(move || {
                let final_segment_path = path.join(format!("{}", next_seq));
                let tmp_segment_path = path.join(format!("{}.tmp", next_seq));

                {
                    let mut base_guard = base.write().expect("failed to lock base");

                    if let Err(e) =
                        base_guard.write_segment(&tmp_segment_path, snapshot.into_iter())
                    {
                        eprintln!("flush failed to write: {}", e);
                        let _ = std::fs::remove_file(tmp_segment_path.with_extension(SEGMENT_EXT));
                        let _ = std::fs::remove_file(tmp_segment_path.with_extension(DATA_EXT));
                        return;
                    }

                    let tmp_seg = tmp_segment_path.with_added_extension(SEGMENT_EXT);
                    let tmp_dat = tmp_segment_path.with_added_extension(DATA_EXT);

                    let final_seg = final_segment_path.with_added_extension(SEGMENT_EXT);
                    let final_dat = final_segment_path.with_added_extension(DATA_EXT);

                    let _ = std::fs::rename(tmp_seg, final_seg);
                    let _ = std::fs::rename(tmp_dat, final_dat);
                    base_guard
                        .load(&final_segment_path)
                        .expect("failed to reload segment during flush");
                }

                if let Err(e) = std::fs::remove_file(&flushing_path) {
                    eprintln!("failed to delete rotated WAL: {}", e);
                }

                let snapshot = {
                    let base = base.read().expect("failed to read-lock base");
                    if base.segments().count() <= min_merge_count {
                        return;
                    }

                    base.snapshot()
                };

                let mut compactor_guard = compactor_lock
                    .write()
                    .expect("failed to acquire compactor write-lock");
                if let Some(handle) = compactor_guard.as_ref()
                    && !handle.is_finished()
                {
                    return;
                }

                *compactor_guard = Self::compact(base, path, snapshot);
            })
            .map_err(IndexError::Io)?;

        *self.flusher.write().unwrap() = Some(flusher);
        Ok(())
    }

    fn compact(
        base: Arc<RwLock<SegmentedIndex>>,
        path: PathBuf,
        snapshot: Vec<Arc<Segment>>,
    ) -> Option<JoinHandle<()>> {
        if snapshot.is_empty() {
            return None;
        }

        std::thread::Builder::new()
            .name("minidex-compactor".to_string())
            .spawn(move || {
                let next_seq = Self::generate_op_seq();
                let tmp_path = path.join(&format!("{}.tmp", next_seq));

                println!("Starting compaction with {} segments", snapshot.len());
                match compactor::merge_segments(&snapshot, tmp_path.clone()) {
                    Ok(_) => {
                        let mut base_guard = base
                            .write()
                            .expect("failed to lock base for compaction apply");
                        if let Err(e) = base_guard.apply_compaction(&snapshot, tmp_path) {
                            eprintln!("Failed to apply compaction: {}", e);
                        }
                        println!("Compaction finished");
                    }
                    Err(e) => eprintln!("Compaction failed: {}", e),
                }
            })
            .ok()
    }

    fn generate_op_seq() -> u64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        let _ = self.commit();

        if let Ok(mut flusher) = self.flusher.write() {
            if let Some(flusher) = flusher.take() {
                let _ = flusher.join();
            }
        }

        if let Ok(mut compactor) = self.compactor.write() {
            if let Some(compactor) = compactor.take() {
                let _ = compactor.join();
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SearchResult {
    pub path: PathBuf,
    pub kind: Kind,
    pub last_modified: u64,
    pub last_accessed: u64,
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .last_modified
            .cmp(&self.last_modified)
            .then_with(|| self.kind.cmp(&other.kind))
            .then_with(|| self.path.cmp(&other.path))
    }
}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("failed to open index on disk: {0}")]
    Open(std::io::Error),
    #[error("failed to read lock data")]
    ReadLock,
    #[error("failed to write lock data")]
    WriteLock,
    #[error(transparent)]
    SegmentedIndex(SegmentedIndexError),
    #[error("failed to compile matching regex: {0}")]
    Regex(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
