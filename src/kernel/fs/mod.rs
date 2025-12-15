use zerocopy::{FromBytes, IntoBytes, TryFromBytes};

use crate::{
    hardware::storage::{Storage, block::Block},
    kernel::fs::{
        alloc_map::{AllocFlag, AllocMap},
        directory::Dir,
        node::{FileType, NodePtr},
        superblock::Superblock,
        transaction::Transaction,
    },
};

pub mod alloc_map;
pub mod directory;
pub mod node;
pub mod path;
pub mod superblock;
pub mod transaction;

/// An in-memory view of the filesystem.
pub struct Filesystem {
    superblock: Superblock,
    block_map: AllocMap,
    node_map: AllocMap,
}

impl Filesystem {
    /// Formats the persistent storage with a filesystem.
    ///
    /// # Panics
    /// ...
    pub fn format(storage: &mut Storage, block_count: usize, node_count: usize) -> Self {
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
            superblock,
            block_map,
            node_map,
        };

        {
            // Write superblock
            let superblock = Block::from(&fs.superblock);
            let mut tx = Transaction::new(&mut fs, storage);
            tx.write_block(superblock::SUPER_ID, &superblock);

            // Initialize the root directory
            let (_, root_id) = tx
                .create_node(FileType::Dir)
                .expect("Must be able to create the root node");
            assert!(root_id == NodePtr::root());
            let root = Dir::new(root_id, root_id);
            tx.write_directory(root_id, &root)
                .expect("Must be able to write the root directory");

            tx.commit();
        }

        fs
    }

    /// Mounts the filesystem from the persistent storage.
    ///
    /// # Panics
    /// ...
    pub fn mount(storage: &Storage) -> Option<Self> {
        // Read the superblock
        let blocks = storage
            .read_block(0)
            .expect("Must be able to read the superblock");
        let bytes = blocks.as_bytes();
        let superblock = Superblock::read_from_bytes(&bytes[0..size_of::<Superblock>()])
            .expect("'bytes' must be a valid 'Superblock'");

        // Verify magic
        if superblock.magic != superblock::MAGIC {
            return None;
        }

        // Read the block allocation map
        let block_map = Self::read_map(
            storage,
            superblock.block_map_start,
            superblock.node_map_start,
            superblock.block_count,
        );

        // Read the node allocation map
        let node_map = Self::read_map(
            storage,
            superblock.node_map_start,
            superblock.node_table_start,
            superblock.node_count,
        );

        Some(Self {
            superblock,
            block_map,
            node_map,
        })
    }

    fn read_map(storage: &Storage, map_start: usize, map_end: usize, count: usize) -> AllocMap {
        let block_ids: Vec<usize> = (map_start..map_end).collect();
        let blocks = storage
            .read_blocks(&block_ids)
            .expect("Must be able to read the allocation map");
        let bytes = blocks.as_bytes();
        let flags = <[AllocFlag]>::try_ref_from_bytes(bytes)
            .expect("'bytes' must be a valid '<[AllocFlag]>'");
        AllocMap::from_slice(&flags[..count])
    }
}
