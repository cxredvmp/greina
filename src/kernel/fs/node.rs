use zerocopy::{FromBytes, Immutable, IntoBytes, TryFromBytes};

use crate::hardware::storage::block::BLOCK_SIZE;

/// [Node] size.
pub const NODE_SIZE: usize = size_of::<Node>();

/// How many nodes fit in a block.
pub const NODES_PER_BLOCK: usize = BLOCK_SIZE / NODE_SIZE;

/// How many extents a [Node] can have.
const EXTENTS_PER_NODE: usize = 15;

/// A pointer to a node.
#[repr(C)]
#[derive(Default, Clone, Copy, PartialEq, Eq)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct NodePtr {
    id: usize,
}

impl NodePtr {
    /// Constructs a pointer to the rood node.
    pub fn root() -> Self {
        Self { id: 1 }
    }

    /// Constructs a pointer from a node id.
    pub fn new(id: usize) -> Self {
        Self { id }
    }

    /// Returns the id of the node.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Checks if the pointer doesn't point to any node.
    pub fn is_null(&self) -> bool {
        self.id == 0
    }
}

/// Represents a file system object.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable)]
pub struct Node {
    pub size: usize,
    pub link_count: u32,
    filetype: FileType,
    _pad: [u8; 3],
    extents: [Extent; EXTENTS_PER_NODE],
}

impl Node {
    /// Constructs a [Node] of the given filetype.
    pub fn new(filetype: FileType) -> Self {
        Self {
            filetype,
            ..Default::default()
        }
    }

    /// Returns the filetype of the node.
    pub fn filetype(&self) -> FileType {
        self.filetype
    }

    /// Returns a reference to node's extents.
    pub fn get_extents(&self) -> &[Extent] {
        &self.extents
    }

    /// Returns a mutable reference to node's extents.
    pub fn get_mut_extents(&mut self) -> &mut [Extent] {
        &mut self.extents
    }

    /// Resolves `block offset` within the file into a block id.
    pub fn get_block_id(&self, mut block_offset: usize) -> Option<usize> {
        for extent in self.extents.iter().take_while(|e| !e.is_null()) {
            let extent_len = extent.len();
            if extent_len > block_offset {
                return if extent.is_hole() {
                    None
                } else {
                    Some(extent.start + block_offset)
                };
            }
            block_offset -= extent_len;
        }
        None
    }

    /// Resolves byte `offset` into a block id.
    pub fn get_block_id_from_offset(&self, offset: usize) -> Option<usize> {
        let block_offset = Self::get_block_offset_from_offset(offset);
        self.get_block_id(block_offset)
    }

    /// Converts byte `offset` into a block offset.
    pub const fn get_block_offset_from_offset(offset: usize) -> usize {
        offset / BLOCK_SIZE
    }

    /// Returns the number of blocks that belong to the node.
    pub fn block_count(&self) -> usize {
        self.extents
            .iter()
            .filter(|e| !e.is_null() && !e.is_hole())
            .map(|e| e.len())
            .sum()
    }

    /// Maps the block at `block offset` within the file to `block id`.
    pub fn map_block(&mut self, mut block_offset: usize, block_id: usize) -> Result<()> {
        assert!(block_id != 0);
        for curr in 0..self.extents.len() {
            if self.extents[curr].is_null() {
                // All allocated extents were passed or there was none
                if curr > 0 {
                    // There is a previous extent
                    let prev = curr - 1;
                    let is_hole = self.extents[prev].is_hole();
                    let contiguous = block_offset == 0 && self.extents[prev].end == block_id;
                    if !is_hole && contiguous {
                        // Can merge with the previous extent
                        self.extents[prev].end += 1;
                        return Ok(());
                    }
                }
                if block_offset == 0 {
                    self.extents[curr].start = block_id;
                    self.extents[curr].end = block_id + 1;
                } else {
                    let next = curr + 1;
                    if next >= self.extents.len() {
                        return Err(Error::OutOfExtents);
                    }
                    // Make the current extent a hole and map the next one
                    self.extents[curr].end = block_offset;
                    self.extents[next].start = block_id;
                    self.extents[next].end = block_id + 1;
                }
                return Ok(());
            }

            let blocks_in_curr = self.extents[curr].len();
            if block_offset < blocks_in_curr {
                // Block resides inside this extent
                let is_hole = self.extents[curr].is_hole();
                if !is_hole {
                    return Err(Error::AlreadyMapped);
                }

                // Split the hole into three extents:
                let mut exts = [Extent::default(); 3];
                exts[0].end = block_offset; // Left hole
                exts[1].start = block_id;
                exts[1].end = block_id + 1;
                exts[2].end = blocks_in_curr - block_offset - 1; // Right hole
                // Remove empty hole, if there is one
                // (i.e. the first/last block of the hole is mapped)
                let exts: Vec<Extent> = exts.into_iter().filter(|e| !e.is_null()).collect();
                let extra = exts.len() - 1; // How many new extents need to be inserted
                let last = self.extents.iter().rposition(|e| !e.is_null()).unwrap();
                if last + extra > (self.extents.len() - 1) {
                    // No room for extent insertion
                    return Err(Error::OutOfExtents);
                }
                let next = curr + 1;
                self.extents.copy_within(next..=last, next + extra);
                self.extents[curr..=(curr + extra)].copy_from_slice(&exts);

                return Ok(());
            }
            block_offset -= blocks_in_curr;
        }
        Err(Error::OutOfExtents)
    }

    /// Appends a sparse region of 'count' blocks to the end of node's extents.
    pub fn append_hole(&mut self, count: usize) -> Result<()> {
        assert!(count != 0);
        for i in 0..self.extents.len() {
            if self.extents[i].is_null() {
                // Check if can be merged with the previous extent
                if i > 0 {
                    let prev_idx = i - 1;
                    let prev = self.extents[prev_idx];
                    if prev.is_hole() {
                        self.extents[prev_idx].end += count;
                        return Ok(());
                    }
                }
                self.extents[i].end = count;
                return Ok(());
            }
        }
        Err(Error::OutOfExtents)
    }
}

/// Represents file types.
#[repr(u8)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[derive(TryFromBytes, IntoBytes, Immutable)]
pub enum FileType {
    #[default]
    File,
    Dir,
    Symlink,
}

/// Represents a contiguous span of blocks.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Extent {
    start: usize,
    end: usize,
}

impl Extent {
    /// Returns the block that marks the start of the extent.
    pub fn start(&self) -> usize {
        self.start
    }

    /// Returns the block that marks the end (exclusive) of the extent.
    pub fn end(&self) -> usize {
        self.end
    }

    /// Checks whether the extent does not point to any blocks.
    pub fn is_null(&self) -> bool {
        self.start == 0 && self.end == 0
    }

    /// Checks whether the extent represents a sparse region.
    pub fn is_hole(&self) -> bool {
        self.start == 0 && self.end > 0
    }

    /// Zeroes out the extent.
    pub fn nullify(&mut self) {
        self.start = 0;
        self.end = 0;
    }

    /// Shrinks the extent to `len`.
    pub fn shrink(&mut self, len: usize) {
        self.end = len;
    }

    /// Returns the number of blocks in this extent.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Represesnts itself as a (start, end) span.
    pub fn span(&self) -> (usize, usize) {
        (self.start, self.end)
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    OutOfExtents,
    AlreadyMapped,
}
