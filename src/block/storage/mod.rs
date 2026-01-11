use crate::block::{Block, BlockAddr};

pub mod file;

/// An implementation of `Storage` allows reading and writing blocks, as well as determining the
/// block capacity.
pub trait Storage {
    /// Reads the block at `addr` into `block`.
    fn read_at(&self, block: &mut Block, addr: BlockAddr) -> Result<()>;

    /// Writes `block` into the block at `addr`.
    fn write_at(&mut self, block: &Block, addr: BlockAddr) -> Result<()>;

    /// Returns the number of blocks the storage can hold.
    fn capacity(&mut self) -> Result<u64>;
}

pub type Result<T> = core::result::Result<T, libc::c_int>;
