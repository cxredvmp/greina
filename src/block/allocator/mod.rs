pub mod bitmap;

use crate::block::BlockAddr;

/// An implementation of `Allocator` can manage block allocation.
pub trait Allocator {
    /// Allocates `count` blocks, returning the starting address.
    fn allocate(&mut self, count: u64) -> Result<BlockAddr>;

    /// Deallocates `count` blocks starting at `addr`.
    fn deallocate(&mut self, start: BlockAddr, count: u64) -> Result<()>;

    /// Returns the number of blocks available for allocation.
    fn available(&self) -> u64;
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    NoSpace,
    AddrOutOfBounds,
    NotAllocated,
}

impl From<Error> for libc::c_int {
    fn from(err: Error) -> Self {
        match err {
            Error::NoSpace => libc::ENOSPC,
            Error::AddrOutOfBounds => libc::EIO,
            Error::NotAllocated => libc::EIO,
        }
    }
}
