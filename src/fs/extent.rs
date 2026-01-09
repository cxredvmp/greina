use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::block::BlockAddr;

/// Represents a contiguous span of blocks.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Extent {
    pub start: BlockAddr,
    pub end: BlockAddr,
}

impl Extent {
    /// Returns the number of blocks this extent covers.
    pub fn len(&self) -> u64 {
        self.end - self.start
    }

    /// Checks whether the extent does not point to any blocks.
    pub fn is_empty(&self) -> bool {
        self.start == 0 && self.end == 0
    }

    /// Checks whether the extent represents a sparse region.
    pub fn is_sparse(&self) -> bool {
        self.start == 0 && self.end > 0
    }

    /// Clears the extent.
    pub fn clear(&mut self) {
        self.start = 0;
        self.end = 0;
    }

    /// Returns the extent as a (start, end) span.
    pub fn span(&self) -> (u64, u64) {
        (self.start, self.end)
    }
}
