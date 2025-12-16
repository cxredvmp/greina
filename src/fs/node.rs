use zerocopy::{FromBytes, Immutable, IntoBytes, TryFromBytes};

use crate::block::{BLOCK_SIZE, BlockAddr};

/// A pointer to a node.
#[repr(C)]
#[derive(Default, Clone, Copy, PartialEq, Eq)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct NodePtr {
    id: u64,
}

impl NodePtr {
    /// Constructs a pointer to the rood node.
    pub fn root() -> Self {
        Self { id: 1 }
    }

    /// Constructs a pointer from a node id.
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    /// Returns the id of the node.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Checks if the pointer doesn't point to any node.
    pub fn is_null(&self) -> bool {
        self.id == 0
    }
}

/// Node's size.
pub const NODE_SIZE: usize = size_of::<Node>();

/// How many extents a node can have.
const EXTENTS_PER_NODE: usize = 15;

/// Represents a file system object.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable)]
pub struct Node {
    pub size: u64,
    pub link_count: u32,
    filetype: FileType,
    _pad: [u8; 3],
    extents: [Extent; EXTENTS_PER_NODE],
}

impl Node {
    /// Constructs a `Node` of a given filetype.
    pub fn new(filetype: FileType) -> Self {
        Self {
            filetype,
            ..Default::default()
        }
    }

    /// Returns node's file type.
    pub fn filetype(&self) -> FileType {
        self.filetype
    }

    /// Converts byte `offset` into a block offset.
    pub const fn get_block_offset_from_offset(offset: u64) -> u64 {
        offset / BLOCK_SIZE
    }

    /// Resolves byte `offset` into a block address.
    pub fn get_block_addr_from_offset(&self, offset: u64) -> Option<BlockAddr> {
        let block_offset = Self::get_block_offset_from_offset(offset);
        self.get_block_addr(block_offset)
    }

    /// Resolves `block offset` within the file into a block address.
    pub fn get_block_addr(&self, mut block_offset: u64) -> Option<BlockAddr> {
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

    // TODO: This needs some work. Current implementation leads to fragmentation.
    /// Maps the block at `block offset` within the file to `addr`.
    pub fn map_block(&mut self, mut block_offset: u64, addr: BlockAddr) -> Result<()> {
        assert!(addr != 0);
        for curr in 0..self.extents.len() {
            if self.extents[curr].is_null() {
                // All allocated extents were passed or there was none
                if curr > 0 {
                    // There is a previous extent
                    let prev = curr - 1;
                    let is_hole = self.extents[prev].is_hole();
                    let contiguous = block_offset == 0 && self.extents[prev].end == addr;
                    if !is_hole && contiguous {
                        // Can merge with the previous extent
                        self.extents[prev].end += 1;
                        return Ok(());
                    }
                }
                if block_offset == 0 {
                    self.extents[curr].start = addr;
                    self.extents[curr].end = addr + 1;
                } else {
                    let next = curr + 1;
                    if next >= self.extents.len() {
                        return Err(Error::OutOfExtents);
                    }
                    // Make the current extent a hole and map the next one
                    self.extents[curr].end = block_offset;
                    self.extents[next].start = addr;
                    self.extents[next].end = addr + 1;
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
                exts[1].start = addr;
                exts[1].end = addr + 1;
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
    pub fn append_hole(&mut self, count: u64) -> Result<()> {
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

    /// Truncates the node's size to `size`.
    /// Optionally, returns addresses of discarded blocks as a list of spans.
    pub fn truncate(&mut self, size: u64) -> Vec<(u64, u64)> {
        let mut discarded = Vec::new();

        if size >= self.size {
            // Growing, no need to handle blocks
            self.size = size;
            return discarded;
        }

        // Shrinking, some blocks need to be discarded
        self.size = size;
        let keep = size.div_ceil(BLOCK_SIZE);
        let mut passed = 0;

        for extent in &mut self.extents {
            if extent.is_null() {
                break;
            }

            let len = extent.len();

            if passed >= keep {
                // Extent is entirely not needed
                if !extent.is_hole() {
                    discarded.push(extent.span());
                }
                extent.nullify();
            } else if passed + len > keep {
                // Extent is partially needed
                let new_len = keep - passed;
                let new_end = extent.start + new_len;
                if !extent.is_hole() {
                    discarded.push((new_end, extent.end));
                }
                extent.end = new_end;
            }

            passed += len;
        }

        discarded
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
    start: BlockAddr,
    end: BlockAddr,
}

impl Extent {
    /// Checks whether the extent does not point to any blocks.
    fn is_null(&self) -> bool {
        self.start == 0 && self.end == 0
    }

    /// Checks whether the extent represents a hole (sparse region).
    fn is_hole(&self) -> bool {
        self.start == 0 && self.end > 0
    }

    /// Zeroes out the extent.
    fn nullify(&mut self) {
        self.start = 0;
        self.end = 0;
    }

    /// Returns the number of blocks this extent covers.
    fn len(&self) -> u64 {
        self.end - self.start
    }

    /// Represesnts itself as a (start, end) span.
    fn span(&self) -> (u64, u64) {
        (self.start, self.end)
    }
}

type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    OutOfExtents,
    AlreadyMapped,
}
