use std::path::PathBuf;

use crate::{common::Kind, opstamp::Opstamp};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexEntry {
    pub(crate) opstamp: Opstamp,
    pub(crate) kind: Kind,
    pub(crate) last_modified: u64,
    pub(crate) last_accessed: u64,
    pub(crate) category: u16,
}

impl IndexEntry {
    pub(crate) const SIZE: usize = std::mem::size_of::<Self>();

    pub(crate) fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.opstamp.to_bytes());
        buf[8] = self.kind as u8;
        buf[9..17].copy_from_slice(&self.last_modified.to_le_bytes());
        buf[17..25].copy_from_slice(&self.last_accessed.to_le_bytes());
        buf[25..27].copy_from_slice(&self.category.to_le_bytes()); // Serialize
        buf
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            opstamp: Opstamp::from_bytes(&bytes[0..8]),
            kind: Kind::from(bytes[8]),
            last_modified: u64::from_le_bytes(bytes[9..17].try_into().unwrap()),
            last_accessed: u64::from_le_bytes(bytes[17..25].try_into().unwrap()),
            category: u16::from_le_bytes(bytes[25..27].try_into().unwrap()), // Deserialize
        }
    }
}

/// A filesystem entry in Minidex, containing information extracted
/// from files, directories or symlinks by systems populating the index.
pub struct FilesystemEntry {
    /// Path of the entry
    pub path: PathBuf,
    /// Volume mount where the entry exists. On Windows this can be a
    /// letter drive, or a UNC path prefix. On UNIX this should be the
    /// volume mount path
    pub volume: String,
    /// Entry kind (File, Directory or Symlink)
    pub kind: Kind,
    /// Last modified timestamp
    pub last_modified: u64,
    /// Last accessed timestamp
    pub last_accessed: u64,
    /// File category as a u16
    pub category: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_entry_serialization() {
        let entry = IndexEntry {
            opstamp: Opstamp::insertion(123),
            kind: Kind::File,
            last_modified: 456,
            last_accessed: 789,
            category: 0xABCD,
        };

        let bytes = entry.to_bytes();
        let entry2 = IndexEntry::from_bytes(&bytes);

        assert_eq!(entry.opstamp.sequence(), entry2.opstamp.sequence());
        assert_eq!(entry.kind, entry2.kind);
        assert_eq!(entry.last_modified, entry2.last_modified);
        assert_eq!(entry.last_accessed, entry2.last_accessed);
        assert_eq!(entry.category, entry2.category);
    }

    #[test]
    fn test_index_entry_deletion_serialization() {
        let entry = IndexEntry {
            opstamp: Opstamp::deletion(123),
            kind: Kind::File,
            last_modified: 0,
            last_accessed: 0,
            category: 0,
        };

        let bytes = entry.to_bytes();
        let entry2 = IndexEntry::from_bytes(&bytes);

        assert!(entry2.opstamp.is_deletion());
        assert_eq!(entry2.opstamp.sequence(), 123);
    }
}
