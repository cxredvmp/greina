#[cfg(test)]
pub mod map;

pub mod file;

use crate::block::{Block, BlockAddr};

/// An implementation of `Storage` allows reading and writing blocks, as well as determining the
/// block capacity.
pub trait Storage {
    /// Reads the block at `addr` into `block`.
    fn read_at(&self, block: &mut Block, addr: BlockAddr) -> Result<()>;

    /// Writes `block` into the block at `addr`.
    fn write_at(&mut self, block: &Block, addr: BlockAddr) -> Result<()>;

    /// Returns the number of blocks the storage can hold.
    fn capacity(&self) -> Result<u64>;
}

pub type Result<T> = core::result::Result<T, libc::c_int>;

#[cfg(test)]
pub mod tests {
    use super::*;

    pub trait TestableStorage: Storage {
        fn new_for_test(block_count: u64) -> Self;
    }

    pub fn capacity<S: TestableStorage>() {
        let storage = S::new_for_test(4);
        assert_eq!(storage.capacity().unwrap(), 4);
    }

    pub fn write_and_read<S: TestableStorage>() {
        let mut storage = S::new_for_test(4);
        let mut write_block = Block::default();
        write_block.data.fill(0xAB);

        storage.write_at(&write_block, 2).unwrap();

        let mut read_block = Block::default();
        storage.read_at(&mut read_block, 2).unwrap();

        assert_eq!(read_block.data, write_block.data);
    }

    pub fn no_interference<S: TestableStorage>() {
        let mut storage = S::new_for_test(2);

        let mut write_block_0 = Block::default();
        write_block_0.data.fill(0xAB);
        storage.write_at(&write_block_0, 0).unwrap();

        let mut write_block_1 = Block::default();
        write_block_1.data.fill(0xCD);
        storage.write_at(&write_block_1, 1).unwrap();

        let mut read_block_0 = Block::default();
        storage.read_at(&mut read_block_0, 0).unwrap();
        assert_eq!(read_block_0.data, write_block_0.data);

        let mut read_block_1 = Block::default();
        storage.read_at(&mut read_block_1, 1).unwrap();
        assert_eq!(read_block_1.data, write_block_1.data);
    }

    pub fn out_of_bounds<S: TestableStorage>() {
        let mut storage = S::new_for_test(4);
        let mut write_block = Block::default();
        write_block.data.fill(0xAB);

        assert!(storage.read_at(&mut write_block, 4).is_err());
        assert!(storage.write_at(&write_block, 4).is_err());
    }

    pub fn overwrite<S: TestableStorage>() {
        let mut storage = S::new_for_test(4);
        let mut write_block_1 = Block::default();
        write_block_1.data.fill(0xAB);

        storage.write_at(&write_block_1, 0).unwrap();

        let mut write_block_2 = Block::default();
        write_block_2.data.fill(0xCD);

        storage.write_at(&write_block_2, 0).unwrap();

        let mut read_block = Block::default();
        storage.read_at(&mut read_block, 0).unwrap();

        assert_eq!(read_block.data, write_block_2.data);
    }
}

#[macro_export]
macro_rules! test_storage {
    ($storage:ty) => {
        #[test]
        fn capacity() {
            $crate::block::storage::tests::capacity::<$storage>();
        }

        #[test]
        fn write_and_read() {
            $crate::block::storage::tests::write_and_read::<$storage>();
        }

        #[test]
        fn no_interference() {
            $crate::block::storage::tests::no_interference::<$storage>();
        }

        #[test]
        fn out_of_bounds() {
            $crate::block::storage::tests::out_of_bounds::<$storage>();
        }

        #[test]
        fn overwrite() {
            $crate::block::storage::tests::overwrite::<$storage>();
        }
    };
}
