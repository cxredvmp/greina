pub mod allocator;
pub mod storage;

pub use allocator::{Allocator, bitmap::BitmapAllocator};

use core::ops::{Deref, DerefMut};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned, little_endian::U64};

/// An address of a block.
pub type BlockAddr = u64;

impl From<BlockAddrStored> for BlockAddr {
    fn from(addr: BlockAddrStored) -> Self {
        addr.0.get()
    }
}

/// A stored address of a block.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(FromBytes, IntoBytes, Immutable, Unaligned)]
pub struct BlockAddrStored(U64);

impl From<BlockAddr> for BlockAddrStored {
    fn from(addr: BlockAddr) -> Self {
        Self(U64::new(addr))
    }
}

/// Block size in bytes.
pub const BLOCK_SIZE: u64 = 4096;

/// Fixed-sized byte sequence.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[cfg_attr(test, derive(PartialEq, Eq))]
#[derive(FromBytes, IntoBytes, Immutable, Unaligned, KnownLayout)]
pub struct Block([u8; BLOCK_SIZE as usize]);

impl Block {
    /// Constructs a [Block] with given data.
    /// Length of `data` must be smaller or equal to [BLOCK_SIZE].
    ///
    /// # Panics
    /// Panics if `data` is larger than [BLOCK_SIZE].
    pub fn new(data: &[u8]) -> Self {
        let mut block = Self::default();
        block[..data.len()].copy_from_slice(data);
        block
    }

    /// Casts a byte slice into a [Block] slice.
    ///
    /// # Panics
    /// Panics if `bytes.len()` is not a multiple of [BLOCK_SIZE].
    pub fn slice_from_bytes(bytes: &[u8]) -> &[Self] {
        <[Self]>::ref_from_bytes(bytes).unwrap()
    }
}

impl Deref for Block {
    type Target = [u8; BLOCK_SIZE as usize];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Block {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Default for Block {
    fn default() -> Self {
        Self([0u8; BLOCK_SIZE as usize])
    }
}
