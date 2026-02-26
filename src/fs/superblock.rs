use crate::{
    block::{BLOCK_SIZE, Block, BlockAddr},
    fs::node::NodeId,
};

use zerocopy::{FromBytes, Immutable, IntoBytes};

/// Filesystem's signature.
pub const SIGNATURE: &[u8; 8] = b"greinafs";

/// Superblock's address.
pub const SUPER_ADDR: BlockAddr = 0;

/// Filesystem's metadata.
#[repr(C)]
#[derive(Clone)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Superblock {
    pub signature: [u8; 8],
    pub block_count: u64,
    pub next_node_id: u64,
    pub block_alloc_start: BlockAddr,
    pub root_addr: BlockAddr,
}

impl Superblock {
    /// Constructs a superblock with given block and node count.
    pub fn new(block_count: u64) -> Self {
        let block_alloc_bytes = block_count.div_ceil(8);
        let block_alloc_blocks = block_alloc_bytes.div_ceil(BLOCK_SIZE);

        // Superblock lives at address 0
        let block_alloc_start = 1;
        let root_addr = block_alloc_start + block_alloc_blocks;

        Self {
            signature: *SIGNATURE,
            block_count,
            next_node_id: 1,
            block_alloc_start,
            root_addr,
        }
    }

    pub fn allocate_node(&mut self) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        NodeId::new(id)
    }
}

impl From<&Superblock> for Block {
    fn from(value: &Superblock) -> Self {
        let bytes = value.as_bytes();
        Block::new(bytes)
    }
}
