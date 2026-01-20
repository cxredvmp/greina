use super::{alloc_map::AllocFlag, node::Node};
use crate::block::{BLOCK_SIZE, Block, BlockAddr};
use zerocopy::{FromBytes, Immutable, IntoBytes};

/// Filesystem's signature.
pub const SIGNATURE: &[u8; 8] = b"greinafs";

/// Superblock's address.
pub const SUPER_ADDR: BlockAddr = 0;

/// Filesystem's metadata.
#[repr(C)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Superblock {
    pub signature: [u8; 8],
    pub block_count: u64,
    pub node_count: u64,
    pub allocator_start: BlockAddr,
    pub node_map_start: BlockAddr,
    pub node_table_start: BlockAddr,
    pub data_start: BlockAddr,
}

impl Superblock {
    /// Constructs a superblock with given block and node count.
    pub fn new(block_count: u64, node_count: u64) -> Self {
        let allocator_bytes = block_count.div_ceil(8);
        let allocator_blocks = allocator_bytes.div_ceil(BLOCK_SIZE);

        let node_map_bytes = node_count * (size_of::<AllocFlag>()) as u64;
        let node_map_blocks = node_map_bytes.div_ceil(BLOCK_SIZE);

        let node_table_bytes = node_count * (size_of::<Node>()) as u64;
        let node_table_blocks = node_table_bytes.div_ceil(BLOCK_SIZE);

        // Superblock lives at address 0
        let allocator_start = 1;
        let node_map_start = allocator_start + allocator_blocks;
        let node_table_start = node_map_start + node_map_blocks;
        let data_start = node_table_start + node_table_blocks;

        Self {
            signature: *SIGNATURE,
            block_count,
            node_count,
            allocator_start,
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
