use std::sync::Mutex;

use bitvec::prelude::*;
use zerocopy::{FromBytes, IntoBytes, little_endian::U64};

use crate::block::{
    BlockAddr,
    allocator::{Allocator, Error, Result},
};

/// A bitmap-backed `Allocator`.
pub struct BitmapAllocator {
    inner: Mutex<BitmapAllocatorInner>,
}

impl BitmapAllocator {
    /// Constructs a bitmap for `block_count` blocks.
    pub fn new(block_count: u64) -> Self {
        Self {
            inner: Mutex::new(BitmapAllocatorInner::new(block_count)),
        }
    }

    /// Constructs a bitmap for `block_count` blocks from bytes.
    pub fn from_bytes(block_count: u64, bytes: &[u8]) -> Self {
        Self {
            inner: Mutex::new(BitmapAllocatorInner::from_bytes(block_count, bytes)),
        }
    }

    /// Provides access to the bitmap as bytes.
    /// Holds the lock for the duration of `f`.
    pub fn with_bytes<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        let inner = self.inner.lock().unwrap();
        f(inner.as_bytes())
    }
}

impl Allocator for BitmapAllocator {
    fn allocate(&self, count: u64) -> Result<BlockAddr> {
        let mut inner = self.inner.lock().unwrap();
        inner.allocate(count)
    }

    fn deallocate(&self, start: BlockAddr, count: u64) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.deallocate(start, count)
    }

    fn available(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.available()
    }
}

struct BitmapAllocatorInner {
    bits: BitBox<u64>,
    count: usize,
    available: usize,
    last_cursor: usize,
}

impl BitmapAllocatorInner {
    fn new(count: u64) -> Self {
        let count = usize::try_from(count).expect("'count' must be addressable");
        let bits = bitbox![u64, Lsb0; 0; count];
        Self {
            count,
            bits,
            available: count,
            last_cursor: 0,
        }
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

    fn as_bytes(&self) -> &[u8] {
        self.bits.as_raw_slice().as_bytes()
    }

    /// Attempts to find a contiguous span of `count` free blocks.
    /// Returns the starting address of the span.
    fn find_free(&self, count: usize) -> Option<usize> {
        assert!(count != 0, "cannot allocate zero blocks");

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

impl BitmapAllocatorInner {
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

#[cfg(test)]
mod tests {
    use crate::{block::allocator::tests::TestableAllocator, test_allocator};

    use super::*;

    impl TestableAllocator for BitmapAllocator {
        fn new_for_test(block_count: u64) -> Self {
            Self::new(block_count)
        }
    }

    test_allocator!(BitmapAllocator);

    #[test]
    fn test_serde() {
        let original = BitmapAllocator::new(16);

        let addr_1 = original.allocate(8).unwrap();
        let addr_2 = original.allocate(8).unwrap();

        original.with_bytes(|bytes| {
            let restored = BitmapAllocator::from_bytes(16, bytes);

            assert_eq!(restored.available(), original.available());

            restored.deallocate(addr_1, 8).unwrap();
            restored.deallocate(addr_2, 8).unwrap();
        })
    }
}
