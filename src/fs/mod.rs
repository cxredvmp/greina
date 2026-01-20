use zerocopy::{FromBytes, IntoBytes, TryFromBytes};

use crate::{
    block::{
        BLOCK_SIZE, Block, BlockAddr,
        allocator::{Allocator, bitmap::BitmapAllocator},
        storage::{self, Storage},
    },
    fs::{
        alloc_map::{AllocFlag, AllocMap},
        dir::Dir,
        node::{FileType, NodePtr},
        superblock::Superblock,
        transaction::{IntoTransactionResult, Transaction},
    },
};

pub mod alloc_map;
pub mod dir;
pub mod extent;
pub mod node;
pub mod superblock;
pub mod transaction;

/// An in-memory view of the filesystem.
pub struct Filesystem<S: Storage> {
    storage: S,
    superblock: Superblock,
    allocator: BitmapAllocator,
    node_map: AllocMap,
}

impl<S: Storage> Filesystem<S> {
    /// Creates a filesystem on a storage device.
    ///
    /// # Panics
    /// ...
    pub fn create(mut storage: S, node_count: u64) -> transaction::Result<Self> {
        let block_count = storage.capacity().into_transaction_res()?;

        // Superblock
        let superblock = Superblock::new(block_count, node_count);

        // Allocation maps
        let mut allocator = BitmapAllocator::new(block_count);
        let mut node_map = AllocMap::new(node_count);

        // Allocate metadata regions
        let addr = allocator
            .allocate(superblock.data_start)
            .expect("metadata regions must be allocated");
        assert_eq!(addr, 0, "metadata regions must start at address 0");

        // Allocate the null node
        node_map
            .allocate_at(0)
            .expect("null node must not be allocated");

        // Create filesystem
        let mut fs = Filesystem {
            storage,
            superblock,
            allocator,
            node_map,
        };

        {
            // Write superblock
            let superblock = Block::from(&fs.superblock);
            let mut tx = Transaction::new(&mut fs);
            tx.write_block_at(&superblock, superblock::SUPER_ADDR);

            // Initialize the root directory
            let (_, root_id) = tx
                .create_node(FileType::Dir, 0o777u16, 0, 0)
                .expect("Must be able to create the root node");
            assert!(root_id == NodePtr::ROOT);
            let root = Dir::new(root_id, root_id);
            tx.write_dir(root_id, &root)
                .expect("Must be able to write the root directory");

            tx.commit()?;
        }

        Ok(fs)
    }

    /// Mounts the filesystem from a storage device.
    ///
    /// # Panics
    /// ...
    pub fn mount(mut storage: S) -> storage::Result<Self> {
        // Read the superblock
        let mut block = Block::default();
        storage
            .read_at(&mut block, 0)
            .expect("must be able to read the superblock");
        let (superblock, _) = Superblock::read_from_prefix(&block.data)
            .expect("'block.data' must be a valid 'Superblock'");

        // Verify signature
        if superblock.signature != *superblock::SIGNATURE {
            return Err(libc::EINVAL);
        }

        // Read the allocator
        let allocator = Self::read_allocator(&mut storage, &superblock)?;

        // Read the node allocation map
        let node_map = Self::read_map(
            &mut storage,
            superblock.node_map_start,
            superblock.node_table_start,
            superblock.node_count,
        )?;

        Ok(Self {
            storage,
            superblock,
            allocator,
            node_map,
        })
    }

    fn read_allocator(
        storage: &mut S,
        superblock: &Superblock,
    ) -> storage::Result<BitmapAllocator> {
        let bytes_to_read = superblock.block_count.div_ceil(8);
        let blocks_to_read = bytes_to_read.div_ceil(BLOCK_SIZE);

        let mut blocks = vec![Block::default(); blocks_to_read as usize];
        let mut addr = superblock.allocator_start;
        for block in &mut blocks {
            storage.read_at(block, addr)?;
            addr += 1;
        }

        let allocator = BitmapAllocator::from_bytes(superblock.block_count, blocks.as_bytes());
        Ok(allocator)
    }

    fn read_map(
        storage: &mut S,
        map_start: BlockAddr,
        map_end: BlockAddr,
        count: u64,
    ) -> storage::Result<AllocMap> {
        let addrs: Vec<BlockAddr> = (map_start..map_end).collect();
        let mut blocks = vec![Block::default(); addrs.len()];
        for (i, &addr) in addrs.iter().enumerate() {
            storage.read_at(&mut blocks[i], addr)?
        }
        let bytes = blocks.as_bytes();
        let flags = <[AllocFlag]>::try_ref_from_bytes(bytes)
            .expect("'bytes' must be a valid '<[AllocFlag]>'");
        Ok(AllocMap::from_slice(&flags[..count as usize]))
    }

    /// Executes a given closure within the context of a transaction.
    /// If the closure returns `Ok`, the transaction is commited to storage.
    /// Else if `Err` is returned, the transaction is discarded and no changes are made.
    pub fn tx<F, T>(&mut self, f: F) -> transaction::Result<T>
    where
        F: FnOnce(&mut Transaction<S>) -> transaction::Result<T>,
    {
        let mut tx = Transaction::new(self);
        let res = f(&mut tx)?;
        tx.commit()?;
        Ok(res)
    }

    pub fn superblock(&self) -> &Superblock {
        &self.superblock
    }

    pub fn node_map(&self) -> &AllocMap {
        &self.node_map
    }

    pub fn allocator(&self) -> &BitmapAllocator {
        &self.allocator
    }
}
