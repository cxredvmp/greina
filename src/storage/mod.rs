use crate::block::{Block, BlockAddr};

pub mod file;

/// Represents a block device.
pub trait Storage {
    /// Reads the block at `addr` into `block`.
    fn read_block_at(&self, block: &mut Block, addr: BlockAddr) -> Result<()>;

    /// Writes `block` into the block at `addr`.
    fn write_block_at(&mut self, block: &Block, addr: BlockAddr) -> Result<()>;

    /// Returns the number of block device's blocks.
    fn block_count(&mut self) -> Result<u64>;
}

pub type Result<T> = core::result::Result<T, libc::c_int>;
