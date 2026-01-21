use zerocopy::{IntoBytes, TryFromBytes};

use crate::{
    block::{allocator::Allocator, storage::Storage},
    tree::{DataType, Key, Tree},
};

use super::*;

/// Represents a directory entry.
#[repr(C)]
pub struct DirEntry {
    pub filetype: FileType,
    pub id: NodeId,
    pub name: DirEntryName,
}

impl DirEntry {
    pub fn as_bytes(&self) -> Box<[u8]> {
        let mut bytes =
            Vec::with_capacity(size_of::<FileType>() + size_of::<NodeId>() + self.name.0.len());
        bytes.extend_from_slice(self.filetype.as_bytes());
        bytes.extend_from_slice(self.id.as_bytes());
        bytes.extend_from_slice(self.name.0.as_bytes());
        bytes.into()
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self> {
        let (filetype, remain) =
            FileType::try_read_from_prefix(bytes).map_err(|_| Error::Uninterpretable)?;
        let (id, remain) =
            NodeId::try_read_from_prefix(remain).map_err(|_| Error::Uninterpretable)?;
        let name = DirEntryName::try_from_bytes(remain)?;
        Ok(Self { filetype, id, name })
    }
}

/// How long a directory entry name can be.
pub const NAME_MAX_LEN: usize = 256;

/// Represents the name of a directory entry.
/// Guaranteed to be valid UTF-8.
#[repr(C)]
#[derive(Clone, PartialEq, Eq)]
pub struct DirEntryName(Box<[u8]>);

impl DirEntryName {
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self> {
        str::from_utf8(bytes).map_err(|_| Error::Uninterpretable)?;
        Ok(Self(bytes.into()))
    }

    /// Returns the directory entry name as `&str`.
    pub fn as_str(&self) -> &str {
        <&str>::from(self)
    }

    /// Returns the `.` directory entry name.
    pub fn itself() -> Self {
        Self(b".".as_ref().into())
    }

    /// Returns the `..` directory entry name.
    pub fn parent() -> Self {
        Self(b"..".as_ref().into())
    }

    pub fn hash(&self) -> u64 {
        hash::fnv_hash(self.0.as_bytes())
    }

    const fn itself_hash() -> u64 {
        hash::fnv_hash(b".")
    }

    const fn parent_hash() -> u64 {
        hash::fnv_hash(b"..")
    }
}

impl<'a> TryFrom<&'a str> for DirEntryName {
    type Error = Error;

    fn try_from(name: &'a str) -> Result<Self> {
        if name.len() > NAME_MAX_LEN {
            return Err(Error::InvalidName);
        }

        if name.contains('\0') {
            return Err(Error::InvalidName);
        }

        if name.contains('/') {
            return Err(Error::InvalidName);
        }

        if name == "." || name == ".." {
            return Err(Error::InvalidName);
        }

        Ok(Self(name.as_bytes().into()))
    }
}

impl<'a> From<&'a DirEntryName> for &'a str {
    fn from(name: &'a DirEntryName) -> Self {
        str::from_utf8(&name.0).expect("'name' is valid UTF-8")
    }
}

impl From<&DirEntryName> for String {
    fn from(name: &DirEntryName) -> Self {
        name.as_str().to_string()
    }
}

impl DirEntry {
    pub fn create(
        storage: &mut impl Storage,
        allocator: &mut impl Allocator,
        superblock: &mut Superblock,
        parent: NodeId,
        filetype: FileType,
        id: NodeId,
        name: DirEntryName,
    ) -> Result<()> {
        let entry = DirEntry { filetype, id, name };
        let key = Key::direntry(parent, entry.name.hash());
        Tree::try_insert(
            storage,
            allocator,
            &mut superblock.root_addr,
            key,
            &entry.as_bytes(),
        )?;
        Ok(())
    }

    pub fn read(
        storage: &impl Storage,
        superblock: &Superblock,
        parent: NodeId,
        name_hash: u64,
    ) -> Result<DirEntry> {
        let key = Key::direntry(parent, name_hash);
        let bytes =
            Tree::get(storage, superblock.root_addr, key)?.ok_or(Error::DirEntryNotFound)?;
        let entry = Self::try_from_bytes(&bytes)?;
        Ok(entry)
    }

    pub fn write(
        &self,
        storage: &mut impl Storage,
        allocator: &mut impl Allocator,
        superblock: &mut Superblock,
        parent: NodeId,
    ) -> Result<()> {
        let key = Key::direntry(parent, self.name.hash());
        Tree::insert(
            storage,
            allocator,
            &mut superblock.root_addr,
            key,
            &self.as_bytes(),
        )?;
        Ok(())
    }

    pub fn link(
        storage: &mut impl Storage,
        allocator: &mut impl Allocator,
        superblock: &mut Superblock,
        parent: NodeId,
        id: NodeId,
        name: DirEntryName,
    ) -> Result<()> {
        let mut node = Node::read(storage, superblock, id)?;
        if node.filetype == FileType::Dir {
            return Err(Error::IsDir);
        }

        DirEntry::create(
            storage,
            allocator,
            superblock,
            parent,
            node.filetype,
            id,
            name,
        )?;

        node.links += 1;
        node.write(storage, allocator, superblock, id)?;

        Ok(())
    }

    pub fn unlink(
        storage: &mut impl Storage,
        allocator: &mut impl Allocator,
        superblock: &mut Superblock,
        parent: NodeId,
        name: &DirEntryName,
    ) -> Result<()> {
        let key = Key::direntry(parent, name.hash());
        let bytes =
            Tree::get(storage, superblock.root_addr, key)?.ok_or(Error::DirEntryNotFound)?;
        let entry = DirEntry::try_from_bytes(&bytes)?;
        if entry.filetype == FileType::Dir {
            return Err(Error::IsDir);
        }

        let mut node = Node::read(storage, superblock, entry.id)?;

        Tree::remove(storage, allocator, &mut superblock.root_addr, key)?
            .expect("entry exists because 'bytes' is 'Some'");

        node.links -= 1;
        if node.links == 0 {
            Node::remove(storage, allocator, superblock, entry.id)?;
        } else {
            node.write(storage, allocator, superblock, entry.id)?;
        }

        Ok(())
    }

    pub fn rename(
        storage: &mut impl Storage,
        allocator: &mut impl Allocator,
        superblock: &mut Superblock,
        old_parent: NodeId,
        old_name: &DirEntryName,
        new_parent: NodeId,
        new_name: &DirEntryName,
    ) -> Result<()> {
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }

        let new_name_hash = new_name.hash();
        if Self::read(storage, superblock, new_parent, new_name_hash).is_ok() {
            return Err(Error::DirEntryExists);
        }

        let old_name_hash = new_name.hash();
        let mut entry = Self::read(storage, superblock, old_parent, old_name_hash)?;

        if entry.filetype == FileType::Dir {
            if DirEntry::is_ancestor(storage, superblock, entry.id, new_parent)? {
                return Err(Error::InvalidMove);
            }

            let parent_entry = DirEntry {
                filetype: FileType::Dir,
                id: new_parent,
                name: DirEntryName::parent(),
            };
            parent_entry.write(storage, allocator, superblock, entry.id)?;
        }

        let key = Key::direntry(old_parent, old_name_hash);
        Tree::remove(storage, allocator, &mut superblock.root_addr, key)?;

        entry.name = new_name.clone();

        let key = Key::direntry(new_parent, new_name_hash);
        Tree::try_insert(
            storage,
            allocator,
            &mut superblock.root_addr,
            key,
            &entry.as_bytes(),
        )?;

        Ok(())
    }

    /// Checks if `ancestor` is an ancestor directory of `dir` directory.
    /// A directory is its own ancestor.
    fn is_ancestor(
        storage: &impl Storage,
        superblock: &Superblock,
        ancestor: NodeId,
        dir: NodeId,
    ) -> Result<bool> {
        let mut curr_parent = dir;
        loop {
            if curr_parent == ancestor {
                return Ok(true);
            } else if curr_parent == NodeId::ROOT {
                return Ok(false);
            }
            curr_parent = Self::read(
                storage,
                superblock,
                curr_parent,
                DirEntryName::parent_hash(),
            )?
            .id;
        }
    }
}

pub struct Dir;

impl Dir {
    pub fn create(
        storage: &mut impl Storage,
        allocator: &mut impl Allocator,
        superblock: &mut Superblock,
        parent: NodeId,
        name: DirEntryName,
    ) -> Result<NodeId> {
        if DirEntry::read(storage, superblock, parent, name.hash()).is_ok() {
            return Err(Error::DirEntryExists);
        }

        let id = Node::create(storage, allocator, superblock, FileType::Dir, 1)?;

        DirEntry::create(
            storage,
            allocator,
            superblock,
            parent,
            FileType::Dir,
            id,
            name,
        )?;

        DirEntry::create(
            storage,
            allocator,
            superblock,
            id,
            FileType::Dir,
            id,
            DirEntryName::itself(),
        )?;

        DirEntry::create(
            storage,
            allocator,
            superblock,
            id,
            FileType::Dir,
            parent,
            DirEntryName::parent(),
        )?;

        Ok(id)
    }

    pub fn remove(
        storage: &mut impl Storage,
        allocator: &mut impl Allocator,
        superblock: &mut Superblock,
        parent: NodeId,
        name: &str,
    ) -> Result<NodeId> {
        let name = DirEntryName::try_from(name)?;

        let entry = DirEntry::read(storage, superblock, parent, name.hash())?;
        if entry.filetype != FileType::Dir {
            return Err(Error::NotDir);
        }

        if !Dir::is_empty(storage, superblock, entry.id)? {
            return Err(Error::DirNotEmpty);
        }

        let key = Key::direntry(entry.id, DirEntryName::itself_hash());
        Tree::remove(storage, allocator, &mut superblock.root_addr, key)?;
        let key = Key::direntry(entry.id, DirEntryName::parent_hash());
        Tree::remove(storage, allocator, &mut superblock.root_addr, key)?;

        let key = Key::direntry(parent, name.hash());
        Tree::remove(storage, allocator, &mut superblock.root_addr, key)?;

        Node::remove(storage, allocator, superblock, entry.id)?;

        Ok(entry.id)
    }

    pub fn is_empty(
        storage: &mut impl Storage,
        superblock: &Superblock,
        id: NodeId,
    ) -> Result<bool> {
        let itself_hash = DirEntryName::itself_hash();
        let parent_hash = DirEntryName::parent_hash();

        let mut curr_key = Key::direntry(id, u64::MAX);
        while let Some((key, _)) = Tree::get_le(storage, superblock.root_addr, curr_key)? {
            if key.id != id || key.datatype != DataType::DirEntry {
                break;
            }

            let hash = key.offset();
            if hash != itself_hash && hash != parent_hash {
                return Ok(false);
            }

            if hash == 0 {
                break;
            }
            curr_key = Key::direntry(id, hash - 1);
        }

        Ok(true)
    }

    pub fn list(
        storage: &impl Storage,
        superblock: &Superblock,
        id: NodeId,
    ) -> Result<Vec<DirEntry>> {
        let mut entries = Vec::new();

        let mut curr_key = Key::direntry(id, u64::MAX);
        while let Some((key, val)) = Tree::get_le(storage, superblock.root_addr, curr_key)? {
            if key.id != id || key.datatype != DataType::DirEntry {
                break;
            }

            let entry = DirEntry::try_from_bytes(&val)?;
            entries.push(entry);

            let hash = key.offset();
            if hash == 0 {
                break;
            }
            curr_key = Key::direntry(id, hash - 1);
        }

        Ok(entries)
    }
}
