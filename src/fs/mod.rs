pub mod error;
use error::*;
pub mod node;
pub mod superblock;
pub mod transaction;

use zerocopy::{FromBytes, IntoBytes};

use crate::{
    block::{
        BLOCK_SIZE, Block,
        allocator::{Allocator, bitmap::BitmapAllocator},
        storage::{self, Storage},
    },
    fs::{
        node::NodeId,
        superblock::{SUPER_ADDR, Superblock},
        transaction::Transaction,
    },
    tree::Tree,
};

/// An in-memory view of the filesystem.
pub struct Filesystem<S: Storage> {
    storage: S,
    superblock: Superblock,
    allocator: BitmapAllocator,
}

impl<S: Storage> Filesystem<S> {
    /// Formats a storage device with a filesystem.
    ///
    /// # Panics
    /// ...
    pub fn format(mut storage: S) -> Result<Self> {
        let block_count = storage.capacity()?;

        let mut allocator = BitmapAllocator::new(block_count);
        Self::allocate_superblock(&mut allocator);
        Self::allocate_allocator(&mut allocator, block_count);

        let mut superblock = Superblock::new(block_count);
        Self::format_root(&mut storage, &mut allocator, &mut superblock)?;

        Self::write_superblock(&mut storage, &mut superblock)?;
        Self::write_allocator(&mut storage, &mut superblock, &allocator)?;

        // Create filesystem
        let mut fs = Filesystem {
            storage,
            superblock,
            allocator,
        };

        {
            // Initialize the root directory
            let mut tx = Transaction::new(&mut fs);
            let root_id = tx
                .create_root_dir()
                .expect("Must be able to create the root node");
            assert!(root_id == NodeId::ROOT);
            tx.commit()?;
        }

        Ok(fs)
    }

    fn allocate_superblock(allocator: &mut BitmapAllocator) {
        let addr = allocator.allocate(1).expect("superblock must be allocated");
        assert_eq!(addr, 0, "superblock must be at address 0");
    }

    fn write_superblock(storage: &mut S, superblock: &Superblock) -> storage::Result<()> {
        let block = Block::new(superblock.as_bytes());
        storage.write_at(&block, SUPER_ADDR)
    }

    fn allocate_allocator(allocator: &mut BitmapAllocator, block_count: u64) {
        let bytes = block_count.div_ceil(8);
        let blocks = bytes.div_ceil(BLOCK_SIZE);
        let addr = allocator
            .allocate(blocks)
            .expect("allocator must be allocated");
        assert_eq!(addr, 1, "allocator must start at address 1");
    }

    fn write_allocator(
        storage: &mut S,
        superblock: &Superblock,
        allocator: &BitmapAllocator,
    ) -> storage::Result<()> {
        let mut addr = superblock.allocator_start;
        let bytes = allocator.as_bytes();
        let (chunks, remainder) = bytes.as_chunks::<{ BLOCK_SIZE as usize }>();

        for chunk in chunks {
            let block = Block::ref_from_bytes(chunk).expect("'Block' is unaligned");
            storage.write_at(block, addr)?;
            addr += 1;
        }

        if !remainder.is_empty() {
            let block = Block::new(remainder);
            storage.write_at(&block, addr)?;
        }

        Ok(())
    }

    fn format_root(
        storage: &mut S,
        allocator: &mut BitmapAllocator,
        superblock: &mut Superblock,
    ) -> storage::Result<()> {
        let root_addr = allocator.allocate(1).expect("must allocate root");
        Tree::format(storage, root_addr).expect("must format root");
        superblock.root_addr = root_addr;
        Ok(())
    }

    /// Mounts the filesystem from a storage device.
    ///
    /// # Panics
    /// ...
    pub fn mount(mut storage: S) -> storage::Result<Self> {
        let superblock = Self::read_superblock(&mut storage)?;
        if superblock.signature != *superblock::SIGNATURE {
            return Err(libc::EINVAL);
        }
        let allocator = Self::read_allocator(&mut storage, &superblock)?;
        Ok(Self {
            storage,
            superblock,
            allocator,
        })
    }

    fn read_superblock(storage: &mut S) -> storage::Result<Superblock> {
        let mut block = Block::default();
        storage.read_at(&mut block, 0)?;
        let (superblock, _) = Superblock::read_from_prefix(&block.data)
            .expect("'block.data' must be a valid 'Superblock'");
        Ok(superblock)
    }

    fn read_allocator(
        storage: &mut S,
        superblock: &Superblock,
    ) -> storage::Result<BitmapAllocator> {
        let bytes = superblock.block_count.div_ceil(8);
        let blocks = bytes.div_ceil(BLOCK_SIZE);

        let mut blocks = vec![Block::default(); blocks as usize];
        let mut addr = superblock.allocator_start;
        for block in &mut blocks {
            storage.read_at(block, addr)?;
            addr += 1;
        }

        let allocator = BitmapAllocator::from_bytes(superblock.block_count, blocks.as_bytes());
        Ok(allocator)
    }

    /// Executes a given closure within the context of a transaction.
    /// If the closure returns `Ok`, the transaction is commited to storage.
    /// Else if `Err` is returned, the transaction is discarded and no changes are made.
    pub fn tx<F, T>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Transaction<S>) -> Result<T>,
    {
        let mut tx = Transaction::new(self);
        let res = f(&mut tx)?;
        tx.commit()?;
        Ok(res)
    }

    pub fn superblock(&self) -> &Superblock {
        &self.superblock
    }

    pub fn allocator(&self) -> &impl Allocator {
        &self.allocator
    }
}
