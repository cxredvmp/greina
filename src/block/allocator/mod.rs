#[cfg(test)]
pub mod set;

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

pub type Result<T> = std::result::Result<T, Error>;

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

#[cfg(test)]
mod tests {
    use super::*;

    pub trait TestableAllocator: Allocator {
        fn new_for_test(block_count: u64) -> Self;
    }

    pub fn test_allocate<A: TestableAllocator>() {
        let mut allocator = A::new_for_test(16);
        allocator.allocate(4).unwrap();
    }

    pub fn test_allocate_all<A: TestableAllocator>() {
        let mut allocator = A::new_for_test(16);

        for _ in 0..4 {
            allocator.allocate(4).unwrap();
        }
    }

    pub fn test_allocate_fragmented<A: TestableAllocator>() {
        let mut allocator = A::new_for_test(16);

        allocator.allocate(4).unwrap();
        let addr = allocator.allocate(4).unwrap();
        allocator.allocate(8).unwrap();

        allocator.deallocate(addr, 2).unwrap();

        allocator.allocate(2).unwrap();
    }

    pub fn test_allocate_no_space<A: TestableAllocator>() {
        let mut allocator = A::new_for_test(16);
        assert!(matches!(allocator.allocate(32), Err(Error::NoSpace)));
    }

    pub fn test_allocate_zero<A: TestableAllocator>() {
        let mut allocator = A::new_for_test(16);
        allocator.allocate(0).unwrap();
    }

    pub fn test_deallocate<A: TestableAllocator>() {
        let mut allocator = A::new_for_test(16);
        let addr = allocator.allocate(8).unwrap();
        allocator.deallocate(addr, 8).unwrap();
    }

    pub fn test_deallocate_part<A: TestableAllocator>() {
        let mut allocator = A::new_for_test(16);
        let addr = allocator.allocate(12).unwrap();
        allocator.deallocate(addr, 4).unwrap();
        assert_eq!(allocator.available(), 8);
    }

    pub fn test_deallocate_out_of_bounds<A: TestableAllocator>() {
        let mut allocator = A::new_for_test(16);
        assert!(matches!(
            allocator.deallocate(17, 1),
            Err(Error::AddrOutOfBounds)
        ));
    }

    pub fn test_deallocate_not_allocated<A: TestableAllocator>() {
        let mut allocator = A::new_for_test(16);
        assert!(matches!(
            allocator.deallocate(0, 8),
            Err(Error::NotAllocated)
        ))
    }

    pub fn test_available<A: TestableAllocator>() {
        let mut allocator = A::new_for_test(16);
        assert_eq!(allocator.available(), 16);

        let addr = allocator.allocate(8).unwrap();
        assert_eq!(allocator.available(), 8);

        allocator.deallocate(addr, 8).unwrap();
        assert_eq!(allocator.available(), 16);
    }

    #[macro_export]
    macro_rules! test_allocator {
        ($allocator:ty) => {
            use crate::block::allocator;

            #[test]
            fn test_allocate() {
                allocator::tests::test_allocate::<$allocator>();
            }

            #[test]
            fn test_allocate_all() {
                allocator::tests::test_allocate_all::<$allocator>();
            }

            #[test]
            fn test_allocate_fragmented() {
                allocator::tests::test_allocate_fragmented::<$allocator>();
            }

            #[test]
            fn test_allocate_no_space() {
                allocator::tests::test_allocate_no_space::<$allocator>();
            }

            #[test]
            #[should_panic]
            fn test_allocate_zero() {
                allocator::tests::test_allocate_zero::<$allocator>();
            }

            #[test]
            fn test_deallocate() {
                allocator::tests::test_deallocate::<$allocator>();
            }

            #[test]
            fn test_deallocate_part() {
                allocator::tests::test_deallocate_part::<$allocator>();
            }

            #[test]
            fn test_deallocate_out_of_bounds() {
                allocator::tests::test_deallocate_out_of_bounds::<$allocator>();
            }

            #[test]
            fn test_deallocate_not_allocated() {
                allocator::tests::test_deallocate_not_allocated::<$allocator>();
            }

            #[test]
            fn test_available() {
                allocator::tests::test_available::<$allocator>();
            }
        };
    }
}
