use libc::EINVAL;
use zerocopy::{FromBytes, IntoBytes, TryFromBytes};

use crate::{
    block::{Block, BlockAddr},
    fs::{
        alloc_map::{AllocFlag, AllocMap},
        dir::Dir,
        node::{FileType, NodePtr},
        superblock::Superblock,
        transaction::{IntoTransactionResult, Transaction},
    },
    storage::{self, Storage},
};

pub mod alloc_map;
pub mod dir;
pub mod node;
pub mod path;
pub mod superblock;
pub mod transaction;

/// An in-memory view of the filesystem.
pub struct Filesystem<S: Storage> {
    storage: S,
    superblock: Superblock,
    block_map: AllocMap,
    node_map: AllocMap,
}

impl<S: Storage> Filesystem<S> {
    /// Formats the persistent storage with a filesystem.
    ///
    /// # Panics
    /// ...
    pub fn format(mut storage: S, node_count: u64) -> transaction::Result<Self> {
        let block_count = storage.block_count().into_transaction_res()?;

        // Superblock
        let superblock = Superblock::new(block_count, node_count);

        // Allocation maps
        let mut block_map = AllocMap::new(block_count);
        let mut node_map = AllocMap::new(node_count);

        // Allocate metadata regions
        block_map
            .allocate_span((0, superblock.data_start))
            .expect("'0..superblock.data_start' blocks must not be allocated");

        // Allocate the null node
        node_map
            .allocate_at(0)
            .expect("Null node must not be allocated");

        // Create filesystem
        let mut fs = Filesystem {
            storage,
            superblock,
            block_map,
            node_map,
        };

        {
            // Write superblock
            let superblock = Block::from(&fs.superblock);
            let mut tx = Transaction::new(&mut fs);
            tx.write_block_at(&superblock, superblock::SUPER_ADDR);

            // Initialize the root directory
            let (_, root_id) = tx
                .create_node(FileType::Dir, 0o755u16, 0, 0)
                .expect("Must be able to create the root node");
            assert!(root_id == NodePtr::root());
            let root = Dir::new(root_id, root_id);
            tx.write_dir(root_id, &root)
                .expect("Must be able to write the root directory");

            tx.commit()?;
        }

        Ok(fs)
    }

    /// Mounts the filesystem from the persistent storage.
    ///
    /// # Panics
    /// ...
    pub fn mount(mut storage: S) -> storage::Result<Self> {
        // Read the superblock
        let mut block = Block::default();
        storage
            .read_block_at(&mut block, 0)
            .expect("Must be able to read the superblock");
        let superblock = Superblock::read_from_bytes(&block.data)
            .expect("'block.data' must be a valid 'Superblock'");

        // Verify signature
        if superblock.signature != *superblock::SIGNATURE {
            return Err(EINVAL);
        }

        // Read the block allocation map
        let block_map = Self::read_map(
            &mut storage,
            superblock.block_map_start,
            superblock.node_map_start,
            superblock.block_count,
        )?;

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
            block_map,
            node_map,
        })
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
            storage.read_block_at(&mut blocks[i], addr)?
        }
        let bytes = blocks.as_bytes();
        let flags = <[AllocFlag]>::try_ref_from_bytes(bytes)
            .expect("'bytes' must be a valid '<[AllocFlag]>'");
        Ok(AllocMap::from_slice(&flags[..count as usize]))
    }
}
