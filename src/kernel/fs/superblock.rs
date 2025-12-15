use super::{alloc_map::AllocFlag, node::Node};
use crate::hardware::storage::block::{BLOCK_SIZE, Block};
use zerocopy::{FromBytes, Immutable, IntoBytes};

/// A magic number to identify the filesystem.
pub const MAGIC: usize = 0xF5F5_F5F5;

/// Superblock id.
pub const SUPER_ID: usize = 0;

/// Represents metadata about the file system.
#[repr(C)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Superblock {
    pub magic: usize,
    pub block_count: usize,
    pub node_count: usize,
    pub block_map_start: usize,
    pub node_map_start: usize,
    pub node_table_start: usize,
    pub data_start: usize,
}

impl Superblock {
    /// Constructs a superblock with given block and node count.
    pub fn new(block_count: usize, node_count: usize) -> Self {
        let block_map_bytes = block_count * (size_of::<AllocFlag>());
        let block_map_blocks = block_map_bytes.div_ceil(BLOCK_SIZE);

        let node_map_bytes = node_count * (size_of::<AllocFlag>());
        let node_map_blocks = node_map_bytes.div_ceil(BLOCK_SIZE);

        let node_table_bytes = node_count * (size_of::<Node>());
        let node_table_blocks = node_table_bytes.div_ceil(BLOCK_SIZE);

        // Superblock lives in the 0th block
        let block_map_start = 1;
        let node_map_start = block_map_start + block_map_blocks;
        let node_table_start = node_map_start + node_map_blocks;
        let data_start = node_table_start + node_table_blocks;

        Self {
            magic: MAGIC,
            block_count,
            node_count,
            block_map_start,
            node_map_start,
            node_table_start,
            data_start,
        }
    }
}

impl From<&Superblock> for Block {
    fn from(value: &Superblock) -> Self {
        let bytes = value.as_bytes();
        Block::new(bytes)
    }
}
