pub mod error;
use error::*;

pub mod node;
pub mod superblock;
pub mod transaction;

use zerocopy::{FromBytes, IntoBytes};

use crate::{
    block::{
        self, Allocator, BLOCK_SIZE, Block,
        allocator::bitmap::BitmapAllocator,
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
    block_alloc: BitmapAllocator,
}

impl<S: Storage> Filesystem<S> {
    /// Formats a storage device with a filesystem.
    ///
    /// # Panics
    /// ...
    pub fn format(mut storage: S) -> Result<Self> {
        let block_count = storage.capacity()?;

        let mut block_alloc = BitmapAllocator::new(block_count);
        Self::allocate_superblock(&mut block_alloc);
        Self::allocate_block_alloc(&mut block_alloc, block_count);

        let mut superblock = Superblock::new(block_count);
        Self::format_root(&mut storage, &mut block_alloc, &mut superblock)?;

        Self::write_superblock(&mut storage, &superblock)?;
        Self::write_block_alloc(&mut storage, &superblock, &block_alloc)?;

        // Create filesystem
        let mut fs = Filesystem {
            storage,
            superblock,
            block_alloc,
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

    fn allocate_superblock(block_alloc: &mut BitmapAllocator) {
        let addr = block_alloc
            .allocate(1)
            .expect("superblock must be allocated");
        assert_eq!(addr, 0, "superblock must be at address 0");
    }

    fn write_superblock(storage: &mut S, superblock: &Superblock) -> storage::Result<()> {
        let block = Block::new(superblock.as_bytes());
        storage.write_at(&block, SUPER_ADDR)
    }

    fn allocate_block_alloc(block_alloc: &mut BitmapAllocator, block_count: u64) {
        let bytes = block_count.div_ceil(8);
        let blocks = bytes.div_ceil(BLOCK_SIZE);
        let addr = block_alloc
            .allocate(blocks)
            .expect("allocator must be allocated");
        assert_eq!(addr, 1, "allocator must start at address 1");
    }

    fn write_block_alloc(
        storage: &mut S,
        superblock: &Superblock,
        block_alloc: &BitmapAllocator,
    ) -> storage::Result<()> {
        let mut addr = superblock.block_alloc_start;
        let bytes = block_alloc.as_bytes();
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
        block_alloc: &mut BitmapAllocator,
        superblock: &mut Superblock,
    ) -> storage::Result<()> {
        let root_addr = block_alloc.allocate(1).expect("must allocate root");
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
        let block_alloc = Self::read_block_alloc(&mut storage, &superblock)?;
        Ok(Self {
            storage,
            superblock,
            block_alloc,
        })
    }

    fn read_superblock(storage: &mut S) -> storage::Result<Superblock> {
        let mut block = Block::default();
        storage.read_at(&mut block, 0)?;
        let (superblock, _) = Superblock::read_from_prefix(&block.data)
            .expect("'block.data' must be a valid 'Superblock'");
        Ok(superblock)
    }

    fn read_block_alloc(
        storage: &mut S,
        superblock: &Superblock,
    ) -> storage::Result<BitmapAllocator> {
        let bytes = superblock.block_count.div_ceil(8);
        let blocks = bytes.div_ceil(BLOCK_SIZE);

        let mut blocks = vec![Block::default(); blocks as usize];
        let mut addr = superblock.block_alloc_start;
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

    pub fn block_alloc(&self) -> &impl block::Allocator {
        &self.block_alloc
    }
}
