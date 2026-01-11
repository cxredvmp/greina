use bitvec::prelude::*;
use zerocopy::{FromBytes, IntoBytes, little_endian::U64};

use crate::block::{
    BlockAddr,
    allocator::{Allocator, Error, Result},
};

/// A bitmap-backed `Allocator`.
#[derive(Clone)]
pub struct BitmapAllocator {
    bits: BitBox<u64>,
    count: usize,
    available: usize,
    last_cursor: usize,
}

impl BitmapAllocator {
    /// Constructs a bitmap for `count` blocks.
    pub fn new(count: u64) -> Self {
        let count = usize::try_from(count).expect("'count' must be addressable");
        let bits = bitbox![u64, Lsb0; 0; count];
        BitmapAllocator {
            count,
            bits,
            available: count,
            last_cursor: 0,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        self.bits.as_raw_slice().as_bytes()
    }

    fn from_bytes(count: u64, bytes: &[u8]) -> Self {
        let count = usize::try_from(count).expect("'count' must be addressable");
        let slice = <[U64]>::ref_from_bytes(bytes).expect("'bytes' must be a valid bitmap");
        let bits: BitBox<u64> = slice.iter().map(|v| v.get()).collect();
        let available = bits[..count].count_zeros();
        Self {
            bits,
            count,
            available,
            last_cursor: 0,
        }
    }

    /// Attempts to find a contiguous span of `count` free blocks.
    /// Returns the starting address of the span.
    fn find_free(&self, count: usize) -> Option<usize> {
        if count == 0 {
            return None;
        }

        let mut start = self.last_cursor;
        let before_last = 0..self.last_cursor;
        let after_last = self.last_cursor..self.count;

        for i in after_last.chain(before_last) {
            if i == 0 {
                // Wrap around
                start = 0;
            }

            if self.bits[i] {
                start = i + 1;
                continue;
            }

            if ((i + 1) - start) == count {
                return Some(start);
            }
        }
        None
    }
}

impl Allocator for BitmapAllocator {
    fn allocate(&mut self, count: u64) -> Result<BlockAddr> {
        let count = usize::try_from(count).expect("'count' must be addressable");

        let start = self.find_free(count).ok_or(Error::NoSpace)?;
        let end = start + count;
        self.bits[start..end].fill(true);
        self.available -= count;
        self.last_cursor = end;

        Ok(start as u64)
    }

    fn deallocate(&mut self, start: BlockAddr, count: u64) -> Result<()> {
        let start = usize::try_from(start).expect("'start' must be addressable");
        let count = usize::try_from(count).expect("'count' must be addressable");

        let end = start + count;
        if end > self.count {
            return Err(Error::AddrOutOfBounds);
        }

        if self.bits[start..end].not_all() {
            return Err(Error::NotAllocated);
        }

        self.bits[start..end].fill(false);
        self.available += count;

        Ok(())
    }

    fn available(&self) -> u64 {
        self.available as u64
    }
}
