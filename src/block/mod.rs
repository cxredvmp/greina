pub mod storage;

use zerocopy::{FromBytes, Immutable, IntoBytes};

/// Address of a block.
pub type BlockAddr = u64;

/// Block size in bytes.
pub const BLOCK_SIZE: u64 = 4096;

/// Fixed-sized byte sequence.
#[repr(C)]
#[derive(Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Block {
    pub data: [u8; BLOCK_SIZE as usize],
}

impl Block {
    /// Constructs a `Block` with given data.
    /// Length of `data` must be smaller or equal to `BLOCK_SIZE`.
    ///
    /// # Panics
    /// Panics if:
    /// - `data` is larger than `BLOCK_SIZE`
    pub fn new(data: &[u8]) -> Self {
        let mut block = Self::default();
        block.data[..data.len()].copy_from_slice(data);
        block
    }

    /// Casts a byte slice into a `Block` slice without copying.
    ///
    /// # Panics
    /// Panics if `bytes.len()` is not a multiple of `BLOCK_SIZE`.
    pub fn slice_from_bytes(bytes: &[u8]) -> &[Self] {
        <[Self]>::ref_from_bytes(bytes).unwrap()
    }
}

impl Default for Block {
    fn default() -> Self {
        Self {
            data: [0u8; BLOCK_SIZE as usize],
        }
    }
}
