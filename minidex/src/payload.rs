pub struct FstPayload;

impl FstPayload {
    // Pack offset, kind and extension hash into a single u64.
    pub fn pack(offset: u64, kind: u8, ext: &str) -> u64 {
        let ext_hash = hash_extension(ext);
        let kind_bits = (kind as u64) & 0x0F;

        (ext_hash << 44) | (kind_bits << 40) | offset
    }

    pub fn unpack_offset(payload: u64) -> u64 {
        payload & 0x00FF_FFFF_FFFF
    }

    pub fn unpack_kind(payload: u64) -> u8 {
        ((payload >> 40) & 0x0F) as u8
    }

    pub fn unpack_extension_hash(payload: u64) -> u64 {
        payload >> 44
    }
}

/// Basic FNV1-a implementation for hashing file extensions
/// Hashes extensions into a 20-bit value that can be packed
/// together with the offset.
fn hash_extension(extension: &str) -> u64 {
    if extension.is_empty() {
        return 0;
    }

    let mut hash: u32 = 2166136261;
    for byte in extension.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    (hash & 0x000F_FFFF) as u64
}
