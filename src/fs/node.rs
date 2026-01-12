use std::time::{Duration, SystemTime, UNIX_EPOCH};

use zerocopy::{FromBytes, Immutable, IntoBytes, TryFromBytes};

use crate::{
    block::{BLOCK_SIZE, BlockAddr},
    fs::extent::Extent,
};

/// A pointer to a node.
#[repr(C)]
#[derive(Default, Clone, Copy, PartialEq, Eq)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct NodePtr(pub u64);

impl NodePtr {
    /// A null node pointer.
    pub const NULL: Self = Self(0);

    /// A pointer to the rood node.
    pub const ROOT: Self = Self(1);

    /// Checks if the pointer doesn't point to any node.
    pub fn is_null(&self) -> bool {
        *self == Self::NULL
    }
}

pub const NODE_SIZE: usize = size_of::<Node>();

const MAX_EXTENTS: usize = 26;

/// A file system object.
#[repr(C)]
#[derive(Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable)]
pub struct Node {
    // File size in bytes
    pub size: u64,

    // File type
    pub file_type: FileType,
    _pad: [u8; 1],

    /// File permissions
    pub perms: u16,

    // Number of hard links
    pub links: u32,

    /// User owner of the file
    pub uid: u32,
    /// Group owner of the file
    pub gid: u32,

    // Node creation time
    pub create_time: NodeTime,
    // Node modification time
    pub change_time: NodeTime,
    // File access time
    pub access_time: NodeTime,
    // File modification time
    pub mod_time: NodeTime,

    // Header is 88 bytes, reserve 8 bytes to get to 96 bytes,
    // leaving 512 - 96 = 416 bytes for 416 / 16 = 26 extents
    _reserved: [u8; 8],

    extents: [Extent; MAX_EXTENTS],
}

impl Node {
    /// Constructs a `Node` of a given filetype.
    pub fn new(filetype: FileType, perms: u16, uid: u32, gid: u32) -> Self {
        let now = NodeTime::now();
        Self {
            file_type: filetype,
            perms,
            uid,
            gid,
            create_time: now,
            change_time: now,
            access_time: now,
            mod_time: now,
            ..Default::default()
        }
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
        for extent in self.extents.iter().take_while(|e| !e.is_empty()) {
            let extent_len = extent.len();
            if extent_len > block_offset {
                return if extent.is_sparse() {
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
            if self.extents[curr].is_empty() {
                // All allocated extents were passed or there was none
                if curr > 0 {
                    // There is a previous extent
                    let prev = curr - 1;
                    let is_hole = self.extents[prev].is_sparse();
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
                let is_hole = self.extents[curr].is_sparse();
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
                let exts: Vec<Extent> = exts.into_iter().filter(|e| !e.is_empty()).collect();
                let extra = exts.len() - 1; // How many new extents need to be inserted
                let last = self.extents.iter().rposition(|e| !e.is_empty()).unwrap();
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
            if self.extents[i].is_empty() {
                // Check if can be merged with the previous extent
                if i > 0 {
                    let prev_idx = i - 1;
                    let prev = self.extents[prev_idx];
                    if prev.is_sparse() {
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

    /// Truncates the file's size to `size`.
    /// Optionally, returns addresses of discarded blocks as a list of spans.
    pub(super) fn truncate(&mut self, size: u64) -> Vec<(u64, u64)> {
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
            if extent.is_empty() {
                break;
            }

            let len = extent.len();

            if passed >= keep {
                // Extent is entirely not needed
                if !extent.is_sparse() {
                    discarded.push(extent.span());
                }
                extent.clear();
            } else if passed + len > keep {
                // Extent is partially needed
                let new_len = keep - passed;
                let new_end = extent.start + new_len;
                if !extent.is_sparse() {
                    discarded.push((new_end, extent.end));
                }
                extent.end = new_end;
            }

            passed += len;
        }

        discarded
    }

    /// Returns the number of blocks belonging to the node.
    pub fn blocks(&self) -> u64 {
        self.extents
            .iter()
            .filter(|e| !e.is_empty() && !e.is_sparse())
            .map(|e| e.len())
            .sum()
    }
}

impl Default for Node {
    fn default() -> Self {
        // I think there's actually no better way to do this
        Self {
            size: Default::default(),
            file_type: Default::default(),
            _pad: Default::default(),
            perms: Default::default(),
            links: Default::default(),
            uid: Default::default(),
            gid: Default::default(),
            create_time: Default::default(),
            change_time: Default::default(),
            access_time: Default::default(),
            mod_time: Default::default(),
            _reserved: Default::default(),
            extents: [Extent::default(); MAX_EXTENTS],
        }
    }
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct NodeTime {
    secs: u64,
    nanos: u32,
    _pad: [u8; 4],
}

impl NodeTime {
    pub fn now() -> Self {
        Self::from(SystemTime::now())
    }
}

impl From<SystemTime> for NodeTime {
    fn from(time: SystemTime) -> Self {
        let duration = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
        Self {
            secs: duration.as_secs(),
            nanos: duration.subsec_nanos(),
            _pad: [0u8; 4],
        }
    }
}

impl From<NodeTime> for SystemTime {
    fn from(time: NodeTime) -> Self {
        Self::UNIX_EPOCH + Duration::new(time.secs, time.nanos)
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

type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    OutOfExtents,

    // Filesystem corruption or logic error
    AlreadyMapped,
}

impl From<Error> for libc::c_int {
    fn from(err: Error) -> Self {
        match err {
            Error::OutOfExtents => libc::EFBIG,

            Error::AlreadyMapped => libc::EIO,
        }
    }
}
