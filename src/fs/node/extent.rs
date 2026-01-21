use zerocopy::{FromBytes, Immutable, IntoBytes, Unaligned, little_endian::U64};

use crate::block::{BLOCK_SIZE, BlockAddr};

use super::*;

/// A contiguous span of blocks.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable, Unaligned)]
pub struct Extent {
    pub start: U64,
    pub len: U64,
}

impl Extent {
    pub fn new(start: u64, len: u64) -> Self {
        Self {
            start: start.into(),
            len: len.into(),
        }
    }
    pub fn start(&self) -> BlockAddr {
        self.start.get()
    }

    pub fn len(&self) -> u64 {
        self.len.get()
    }

    /// Checks whether the extent doesn't cover any blocks.
    pub fn is_empty(&self) -> bool {
        self.start() == 0 && self.len() == 0
    }

    /// Clears the extent.
    pub fn clear(&mut self) {
        self.start = U64::new(0);
        self.len = U64::new(0);
    }
}

pub struct MappedExtent {
    pub start: u64,
    pub len: u64,
    pub inner: Extent,
}

impl MappedExtent {
    pub fn end(&self) -> u64 {
        self.start + self.len
    }

    pub fn read(
        storage: &impl Storage,
        superblock: &Superblock,
        id: NodeId,
        offset: u64,
    ) -> Result<Option<Self>> {
        let key = Key::extent(id, offset);
        if let Some((key, ext)) = Tree::get_le(storage, superblock.root_addr, key)? {
            if key.id != id || key.datatype != DataType::Extent {
                return Ok(None);
            }
            let inner = Extent::read_from_bytes(&ext).map_err(|_| Error::Uninterpretable)?;
            let start = key.offset();
            let len = inner.len() * BLOCK_SIZE;
            let ext = Self { start, len, inner };
            if offset < ext.end() {
                return Ok(Some(ext));
            }
        };
        Ok(None)
    }

    pub fn ensure(
        storage: &mut impl Storage,
        allocator: &mut impl Allocator,
        superblock: &mut Superblock,
        id: NodeId,
        offset: u64,
        len: u64,
    ) -> Result<Self> {
        if let Some(map) = Self::read(storage, superblock, id, offset)? {
            return Ok(map);
        }

        let start = (offset / BLOCK_SIZE) * BLOCK_SIZE;
        let end = offset + len;
        let len = end - start;

        let ext_len = len.div_ceil(BLOCK_SIZE);
        let ext_start = allocator.allocate(ext_len)?;
        let ext = Extent::new(ext_start, ext_len);

        let len = ext_len * BLOCK_SIZE;

        let key = Key::extent(id, start);
        Tree::try_insert(
            storage,
            allocator,
            &mut superblock.root_addr,
            key,
            ext.as_bytes(),
        )?;

        Ok(MappedExtent {
            start,
            len,
            inner: ext,
        })
    }
}
