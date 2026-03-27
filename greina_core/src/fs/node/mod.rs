pub mod dir;
pub mod extent;
use extent::*;
pub mod file;
pub mod hash;
pub mod symlink;

use super::error::*;

use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned,
    little_endian::{U32, U64},
};

use crate::{
    block::{self, BLOCK_SIZE, Block, BlockAddr, storage::Storage},
    fs::superblock::Superblock,
    tree::{DataType, Key, Tree},
};

/// A node identifier.
#[repr(C)]
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[derive(FromBytes, IntoBytes, Immutable, Unaligned)]
pub struct NodeId(U64);

impl NodeId {
    pub const NULL: Self = Self(U64::new(0));
    pub const ROOT: Self = Self(U64::new(1));

    pub fn new(id: u64) -> Self {
        Self(U64::new(id))
    }

    pub fn get(&self) -> u64 {
        self.0.get()
    }

    pub fn is_null(&self) -> bool {
        *self == Self::NULL
    }
}

/// A filesystem object.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable, Unaligned, KnownLayout)]
pub struct Node {
    pub size: U64,
    pub filetype: FileType,
    pub links: U32,
}

impl Node {
    /// Constructs a node of given filetype.
    pub fn new(filetype: FileType, links: u32) -> Self {
        Self {
            size: 0.into(),
            filetype,
            links: links.into(),
        }
    }
}

/// Filetypes.
#[repr(u8)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[derive(TryFromBytes, IntoBytes, Immutable, Unaligned)]
pub enum FileType {
    #[default]
    File,
    Dir,
    Symlink,
}

impl Node {
    pub fn create(
        storage: &mut impl Storage,
        block_alloc: &mut impl block::Allocator,
        superblock: &mut Superblock,
        filetype: FileType,
        links: u32,
    ) -> Result<NodeId> {
        let id = superblock.allocate_node();
        let node = Self::new(filetype, links);
        let key = Key::node(id);
        Tree::try_insert(
            storage,
            block_alloc,
            &mut superblock.root_addr,
            key,
            node.as_bytes(),
        )?;
        Ok(id)
    }

    pub fn read(storage: &impl Storage, superblock: &Superblock, id: NodeId) -> Result<Self> {
        let key = Key::node(id);
        let bytes = Tree::get(storage, superblock.root_addr, key)?.ok_or(Error::NodeNotFound)?;
        let node = Self::try_read_from_bytes(&bytes).map_err(|_| Error::Uninterpretable)?;
        Ok(node)
    }

    pub fn write(
        self,
        storage: &mut impl Storage,
        block_alloc: &mut impl block::Allocator,
        superblock: &mut Superblock,
        id: NodeId,
    ) -> Result<()> {
        let key = Key::node(id);
        Tree::insert(
            storage,
            block_alloc,
            &mut superblock.root_addr,
            key,
            self.as_bytes(),
        )?;
        Ok(())
    }

    pub fn remove(
        storage: &mut impl Storage,
        block_alloc: &mut impl block::Allocator,
        superblock: &mut Superblock,
        id: NodeId,
    ) -> Result<()> {
        let node = Self::read(storage, superblock, id)?;

        if node.filetype == FileType::File || node.filetype == FileType::Symlink {
            Self::truncate_extents(storage, block_alloc, superblock, id, 0)?;
        }

        let key = Key::node(id);
        Tree::remove(storage, block_alloc, &mut superblock.root_addr, key)?;

        Ok(())
    }

    /// Deallocates extents past 'size'.
    fn truncate_extents(
        storage: &mut impl Storage,
        block_alloc: &mut impl block::Allocator,
        superblock: &mut Superblock,
        id: NodeId,
        size: u64,
    ) -> Result<()> {
        let key = Key::extent(id, u64::MAX);
        while let Some((key, bytes)) = Tree::get_le(storage, superblock.root_addr, key)? {
            if key.id != id || key.datatype != DataType::Extent {
                break;
            }

            if key.offset() >= size {
                Tree::remove(storage, block_alloc, &mut superblock.root_addr, key)?
                    .expect("extent exists because 'key' exists");
                let ext = Extent::read_from_bytes(&bytes).map_err(|_| Error::Uninterpretable)?;
                block_alloc.deallocate(ext.start(), ext.len())?;
            } else {
                let mut ext =
                    Extent::read_from_bytes(&bytes).map_err(|_| Error::Uninterpretable)?;

                let keep_bytes = size - key.offset();
                let keep_blocks = keep_bytes.div_ceil(BLOCK_SIZE);

                if keep_blocks < ext.len() {
                    let free_blocks = ext.len() - keep_blocks;
                    let free_start = ext.start() + keep_blocks;
                    block_alloc.deallocate(free_start, free_blocks)?;

                    ext.len.set(keep_blocks);
                    Tree::insert(
                        storage,
                        block_alloc,
                        &mut superblock.root_addr,
                        key,
                        ext.as_bytes(),
                    )?;
                }

                break;
            }
        }
        Ok(())
    }
}
