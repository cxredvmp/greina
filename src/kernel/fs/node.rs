use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::hardware::storage::block::BLOCK_SIZE;

/// [Node] size.
pub const NODE_SIZE: usize = size_of::<Node>();

/// How many nodes fit in a [Block].
pub const NODES_PER_BLOCK: usize = BLOCK_SIZE / NODE_SIZE;

/// How many extents a [Node] can have.
const EXTENTS_PER_NODE: usize = 15;

/// Represents a file system object.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Node {
    pub size: u64,
    pub link_count: u64,
    pub extents: [Extent; EXTENTS_PER_NODE],
}

impl Node {
    /// Resolves the logical block index into a physical block index.
    pub fn get_physical_block(&self, logical_index: usize) -> Option<usize> {
        let mut offset = logical_index;
        for extent in self.extents.iter().take_while(|e| !e.is_null()) {
            let blocks_in_extent = extent.block_count();
            if blocks_in_extent > offset {
                return Some(extent.start + offset);
            }
            offset -= blocks_in_extent;
        }
        None
    }

    /// Resolves the byte offset into a physical block index.
    pub fn get_physical_block_from_offset(&self, byte_offset: usize) -> Option<usize> {
        let logical_index = byte_offset / BLOCK_SIZE;
        self.get_physical_block(logical_index)
    }

    /// Returns the number of logical blocks that belong to the node.
    pub fn block_count(&self) -> usize {
        self.extents
            .iter()
            .filter(|e| !e.is_null())
            .map(|e| e.end - e.start)
            .sum()
    }
}

/// Represents a contiguous span of physical blocks.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Extent {
    start: usize,
    end: usize,
}

impl Extent {
    /// Checks whether the extent does not point to any physical blocks.
    pub fn is_null(&self) -> bool {
        self.start == 0
    }

    /// Returns the number of blocks in this extent.
    pub fn block_count(&self) -> usize {
        self.end - self.start
    }
}
