use std::collections::BTreeMap;

use zerocopy::{FromBytes, IntoBytes, TryFromBytes};

use crate::{
    block::{BLOCK_SIZE, Block, BlockAddr},
    fs::{
        Filesystem,
        alloc_map::{self, AllocMap},
        dir::{self, Dir, DirEntry, DirEntryName},
        node::{self, FileType, NODE_SIZE, Node, NodePtr, NodeTime},
        path::{self, Path},
    },
    storage::{self, Storage},
};

/// Cache to buffer changes.
type Changes = BTreeMap<BlockAddr, Block>;

/// Filesystem operation that buffers changes in memory before commiting them to persistent storage.
pub struct Transaction<'a, S: Storage> {
    fs: &'a mut Filesystem<S>,
    // TODO: Replace full clones with caching changes only
    block_map: AllocMap,
    node_map: AllocMap,
    changes: Changes,
}

impl<'a, S: Storage> Transaction<'a, S> {
    /// Constructs a `Transaction` for a given filesystem.
    pub(super) fn new(fs: &'a mut Filesystem<S>) -> Self {
        let block_map = fs.block_map.clone();
        let node_map = fs.node_map.clone();
        Self {
            fs,
            block_map,
            node_map,
            changes: Changes::new(),
        }
    }

    /// Commits the transaction to storage, consuming itself.
    pub(super) fn commit(mut self) -> Result<()> {
        self.sync_maps()?;
        for (&addr, block) in self.changes.iter() {
            self.fs
                .storage
                .write_block_at(block, addr)
                .into_transaction_res()?
        }
        Ok(())
    }

    /// Queues a synchronization of allocation maps.
    fn sync_maps(&mut self) -> Result<()> {
        self.fs.block_map = self.block_map.clone();
        self.fs.node_map = self.node_map.clone();
        for (map, map_start) in [
            (&self.fs.block_map, self.fs.superblock.block_map_start),
            (&self.fs.node_map, self.fs.superblock.node_map_start),
        ] {
            let bytes = map.as_slice().as_bytes();
            for (i, chunk) in bytes.chunks(BLOCK_SIZE as usize).enumerate() {
                let block = Block::read_from_bytes(chunk).unwrap_or_else(|_| Block::new(chunk));
                let addr = map_start + i as u64;
                self.fs
                    .storage
                    .write_block_at(&block, addr)
                    .into_transaction_res()?;
            }
        }
        Ok(())
    }

    /// Reads the block at `addr` into `block`, checking cached changes.
    pub fn read_block_at(&self, block: &mut Block, addr: BlockAddr) -> Result<()> {
        match self.changes.get(&addr) {
            Some(cached) => block.data = cached.data,
            None => self
                .fs
                .storage
                .read_block_at(block, addr)
                .into_transaction_res()?,
        }
        Ok(())
    }

    /// Queues a write of `block` into the block at `addr`.
    pub fn write_block_at(&mut self, block: &Block, addr: BlockAddr) {
        self.changes.insert(addr, *block);
    }

    /// Reads the node from the node table.
    pub fn read_node(&self, node_ptr: NodePtr) -> Result<Node> {
        let addr = self
            .get_node_block_addr(node_ptr)
            .ok_or(Error::NodePtrOutOfBounds)?;

        let mut block = Block::default();
        self.read_block_at(&mut block, addr)?;
        let offset = self
            .get_node_offset(node_ptr)
            .ok_or(Error::NodePtrOutOfBounds)?;

        let node =
            Node::try_read_from_bytes(&block.data[offset as usize..(offset as usize + NODE_SIZE)])
                .expect("'block.data' must hold a valid 'Node'");

        Ok(node)
    }

    /// Queues a write of the node to the node table.
    pub fn write_node(&mut self, node: &mut Node, node_ptr: NodePtr) -> Result<()> {
        let addr = self
            .get_node_block_addr(node_ptr)
            .ok_or(Error::NodePtrOutOfBounds)?;

        let mut block = Block::default();
        self.read_block_at(&mut block, addr)?;

        let offset = self
            .get_node_offset(node_ptr)
            .ok_or(Error::NodePtrOutOfBounds)?;
        block.data[offset as usize..(offset as usize + NODE_SIZE)].copy_from_slice(node.as_bytes());

        self.write_block_at(&block, addr);

        Ok(())
    }

    /// Queses a change of the node in the node table.
    /// Same effect as `write_node`, but also updates `node.change_time` on success.
    pub fn change_node(&mut self, node: &mut Node, node_ptr: NodePtr) -> Result<()> {
        node.change_time = NodeTime::now();
        self.write_node(node, node_ptr)
    }

    /// Allocates a node, returning it and its pointer.
    pub fn create_node(
        &mut self,
        file_type: FileType,
        perms: u16,
        uid: u32,
        gid: u32,
    ) -> Result<(Node, NodePtr)> {
        let mut node = Node::new(file_type, perms, uid, gid);
        let (id, _) = self.node_map.allocate(1)?;
        let node_ptr = NodePtr::new(id);
        self.write_node(&mut node, node_ptr)?;
        Ok((node, node_ptr))
    }

    /// Removes the node, freeing its blocks.
    pub fn remove_node(&mut self, node_ptr: NodePtr) -> Result<()> {
        let mut node = self.read_node(node_ptr)?;

        let spans = node.truncate(0);
        for span in spans {
            // Free the blocks
            self.block_map.free_span(span)?;
        }

        // Free the node
        let id = node_ptr.id();
        self.node_map.free_at(id)?;

        let mut node = Node::default();
        self.write_node(&mut node, node_ptr)?;

        Ok(())
    }

    /// Reads a number of bytes from the file into `buf` starting from `offset`.
    /// Returns the number of bytes read.
    pub fn read_file_at(&mut self, node_ptr: NodePtr, offset: u64, buf: &mut [u8]) -> Result<u64> {
        let mut node = self.read_node(node_ptr)?;

        if offset >= node.size {
            return Ok(0);
        };

        let bytes_available = node.size - offset;
        let bytes_to_read = bytes_available.min(buf.len() as u64);
        let mut bytes_read = 0;

        while bytes_read != bytes_to_read {
            let curr_pos = offset + bytes_read;
            let offset_in_block = curr_pos % BLOCK_SIZE; // First read might be unaligned
            let chunk_size = (BLOCK_SIZE - offset_in_block).min(bytes_to_read - bytes_read);
            match node.get_block_addr_from_offset(curr_pos) {
                Some(addr) => {
                    let mut block = Block::default();
                    self.read_block_at(&mut block, addr)?;
                    buf[bytes_read as usize..(bytes_read + chunk_size) as usize].copy_from_slice(
                        &block.data
                            [offset_in_block as usize..(offset_in_block + chunk_size) as usize],
                    );
                }
                // Handle a sparse file
                None => {
                    buf[bytes_read as usize..(bytes_read + chunk_size) as usize].fill(0u8);
                }
            };
            bytes_read += chunk_size;
        }

        node.access_time = NodeTime::now();
        self.write_node(&mut node, node_ptr)?;

        Ok(bytes_read)
    }

    /// Writes a number of bytes from `buf` to the file starting from `offset`.
    /// Returns the number of byttes written.
    pub fn write_file_at(&mut self, node_ptr: NodePtr, offset: u64, buf: &[u8]) -> Result<u64> {
        let mut node = self.read_node(node_ptr)?;

        let bytes_to_write = buf.len() as u64;
        let mut bytes_written = 0;

        while bytes_written != bytes_to_write {
            let curr_pos = offset + bytes_written;
            let offset_in_block = curr_pos % BLOCK_SIZE; // First write might be unaligned
            let block_offset = Node::get_block_offset_from_offset(curr_pos);

            let (addr, has_alloc) = match node.get_block_addr(block_offset) {
                Some(addr) => (addr, false),
                None => {
                    // Allocate a block
                    let (addr, _) = self.block_map.allocate(1)?;
                    node.map_block(block_offset, addr)?;
                    (addr, true)
                }
            };

            let mut block = Block::default();
            // Don't need to read if it's a freshly allocated block
            if !has_alloc {
                self.read_block_at(&mut block, addr)?
            };

            let chunk_size = (BLOCK_SIZE - offset_in_block).min(bytes_to_write - bytes_written);
            block.data[offset_in_block as usize..(offset_in_block + chunk_size) as usize]
                .copy_from_slice(
                    &buf[bytes_written as usize..(bytes_written + chunk_size) as usize],
                );

            self.write_block_at(&block, addr);
            bytes_written += chunk_size;
        }

        let end_pos = offset + bytes_written;
        if end_pos > node.size {
            node.size = end_pos;
        }

        node.mod_time = NodeTime::now();
        self.change_node(&mut node, node_ptr)?;

        Ok(bytes_written)
    }

    /// Creates a file with a given name and type inside `parent_ptr`.
    /// Returns the file's node pointer.
    pub fn create_file(
        &mut self,
        parent_ptr: NodePtr,
        name: &str,
        file_type: FileType,
        perms: u16,
        uid: u32,
        gid: u32,
    ) -> Result<NodePtr> {
        let name = DirEntryName::try_from(name)?;
        let mut parent = self.read_dir(parent_ptr)?;

        let (mut node, node_ptr) = self.create_node(file_type, perms, uid, gid)?;
        node.links += 1;

        let entry = DirEntry::new(node_ptr, file_type, name);
        parent.add_entry(entry)?;

        self.write_node(&mut node, node_ptr)?;
        self.write_dir(parent_ptr, &parent)?;

        Ok(node_ptr)
    }

    /// Truncates the file's size to `size`.
    pub fn truncate_file(&mut self, node_ptr: NodePtr, size: u64) -> Result<()> {
        let mut node = self.read_node(node_ptr)?;

        match node.file_type {
            FileType::File => (),
            FileType::Dir => return Err(Error::IsDir),
            _ => return Err(Error::NotFile),
        }

        if size == node.size {
            return Ok(());
        }

        let spans = node.truncate(size);
        for span in spans {
            // Free the blocks, if shrinked
            self.block_map.free_span(span)?;
        }

        node.mod_time = NodeTime::now();
        self.change_node(&mut node, node_ptr)?;

        Ok(())
    }

    /// Reads the directory.
    pub fn read_dir(&mut self, node_ptr: NodePtr) -> Result<Dir> {
        let node = self.read_node(node_ptr)?;
        if node.file_type != FileType::Dir {
            return Err(Error::NotDir);
        }
        let mut buf = vec![0u8; node.size as usize];
        self.read_file_at(node_ptr, 0, &mut buf)?;
        let entries = <[DirEntry]>::try_ref_from_bytes(&buf).map_err(|_| Error::CorruptedDir)?;
        Ok(Dir::from_slice(entries))
    }

    /// Writes the directory.
    pub fn write_dir(&mut self, node_ptr: NodePtr, dir: &Dir) -> Result<()> {
        let bytes = dir.as_slice().as_bytes();
        self.write_file_at(node_ptr, 0, bytes)?;
        Ok(())
    }

    /// Creates a directory with a given name inside `parent_ptr`.
    /// Returns the directory's node pointer.
    pub fn create_dir(
        &mut self,
        parent_ptr: NodePtr,
        name: &str,
        perms: u16,
        uid: u32,
        gid: u32,
    ) -> Result<NodePtr> {
        let node_ptr = self.create_file(parent_ptr, name, FileType::Dir, perms, uid, gid)?;
        let parent = Dir::new(node_ptr, parent_ptr);
        self.write_dir(node_ptr, &parent)?;
        Ok(node_ptr)
    }

    /// Removes the empty directory `name` inside `parent_ptr`.
    pub fn remove_dir(&mut self, parent_ptr: NodePtr, name: &str) -> Result<()> {
        let name = DirEntryName::try_from(name)?;
        let mut parent = self.read_dir(parent_ptr)?;

        let entry = parent.remove_entry(name).ok_or(Error::EntryNotFound)?;
        if entry.file_type != FileType::Dir {
            return Err(Error::NotDir);
        }

        let node_ptr = entry.node_ptr;
        let dir = self.read_dir(node_ptr)?;
        if !dir.is_empty() {
            return Err(Error::DirNotEmpty);
        }

        self.write_dir(parent_ptr, &parent)?;
        self.remove_node(node_ptr)?;

        Ok(())
    }

    /// Finds the entry named `name` inside `parent_ptr`.
    pub fn find_entry(&mut self, parent_ptr: NodePtr, name: &str) -> Result<DirEntry> {
        let name = DirEntryName::try_from(name)?;
        let parent = self.read_dir(parent_ptr)?;
        parent.get_entry(name).ok_or(Error::EntryNotFound).copied()
    }

    /// Renames the entry named `old_name` to `new_name` and moves it from `old_parent_ptr` to
    /// `new_parent_ptr`.
    pub fn rename_entry(
        &mut self,
        old_parent_ptr: NodePtr,
        old_name: &str,
        new_parent_ptr: NodePtr,
        new_name: &str,
    ) -> Result<()> {
        let old_name = DirEntryName::try_from(old_name)?;
        if old_name == DirEntryName::itself() || old_name == DirEntryName::parent() {
            return Err(Error::InvalidMove);
        }
        let new_name = DirEntryName::try_from(new_name)?;
        let mut old_parent = self.read_dir(old_parent_ptr)?;

        let mut entry = old_parent
            .remove_entry(old_name)
            .ok_or(Error::EntryNotFound)?;
        entry.name = new_name;

        if old_parent_ptr == new_parent_ptr {
            old_parent.add_entry(entry)?;
            return self.write_dir(old_parent_ptr, &old_parent);
        }

        let mut new_parent = self.read_dir(new_parent_ptr)?;

        if entry.file_type == FileType::Dir {
            if self.is_ancestor_dir(entry.node_ptr, new_parent_ptr)? {
                return Err(Error::InvalidMove);
            }

            let mut dir = self.read_dir(entry.node_ptr)?;
            let parent_entry = dir.get_mut_parent();
            parent_entry.node_ptr = new_parent_ptr;
            self.write_dir(entry.node_ptr, &dir)?;
        }

        new_parent.add_entry(entry)?;

        self.write_dir(old_parent_ptr, &old_parent)?;
        self.write_dir(new_parent_ptr, &new_parent)?;

        Ok(())
    }

    /// Checks if `ancestor_ptr` is an ancestor directory of `dir_ptr` directory.
    /// Ancestry is reflexive, i.e. a directory is its own ancestor.
    fn is_ancestor_dir(&mut self, ancestor_ptr: NodePtr, dir_ptr: NodePtr) -> Result<bool> {
        let root = NodePtr::root();
        let mut curr_parent_ptr = dir_ptr;
        loop {
            if curr_parent_ptr == ancestor_ptr {
                return Ok(true);
            } else if curr_parent_ptr == root {
                return Ok(false);
            }
            let parent = self.read_dir(curr_parent_ptr)?;
            curr_parent_ptr = parent.parent_ptr();
        }
    }

    /// Creates a hard link to the file with a given name.
    pub fn link_file(&mut self, parent_ptr: NodePtr, node_ptr: NodePtr, name: &str) -> Result<()> {
        let name = DirEntryName::try_from(name)?;
        let mut parent = self.read_dir(parent_ptr)?;

        let mut node = self.read_node(node_ptr)?;
        if node.file_type == FileType::Dir {
            return Err(Error::IsDir);
        }

        let entry = DirEntry::new(node_ptr, node.file_type, name);
        parent.add_entry(entry)?;
        node.links += 1;

        self.change_node(&mut node, node_ptr)?;
        self.write_dir(parent_ptr, &parent)?;

        Ok(())
    }

    /// Removes a hard link to the file with a given name.
    /// If `free` is true, deletes the node if `node.link_count` drops to 0, else it must be deallocated manually.
    pub fn unlink_file(&mut self, parent_ptr: NodePtr, name: &str, free: bool) -> Result<()> {
        let name = DirEntryName::try_from(name)?;
        let mut parent = self.read_dir(parent_ptr)?;

        let entry = parent.remove_entry(name).ok_or(Error::EntryNotFound)?;
        if entry.file_type == FileType::Dir {
            return Err(Error::IsDir);
        }

        let node_ptr = entry.node_ptr;
        let mut node = self.read_node(node_ptr)?;
        node.links -= 1;

        self.write_dir(parent_ptr, &parent)?;
        if node.links == 0 && free {
            self.remove_node(node_ptr)?;
        } else {
            self.change_node(&mut node, node_ptr)?;
        }

        Ok(())
    }

    /// Returns the path contained inside `symlink_ptr`.
    pub fn read_symlink(&mut self, symlink_ptr: NodePtr) -> Result<Path<'static>> {
        let node = self.read_node(symlink_ptr)?;
        if node.file_type != FileType::Symlink {
            return Err(Error::NotSymlink);
        }
        let mut buf = vec![0u8; node.size as usize];
        self.read_file_at(symlink_ptr, 0, &mut buf)?;
        Ok(Path::try_from_bytes_owned(&buf)?)
    }

    /// Creates a symlink inside `parent_ptr`, containing `target`.
    /// Returns the node pointer of the symlink.
    pub fn create_symlink(
        &mut self,
        parent_ptr: NodePtr,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<NodePtr> {
        let node_ptr = self.create_file(parent_ptr, name, FileType::Symlink, 0o777u16, uid, gid)?;
        self.write_file_at(node_ptr, 0, target.as_bytes())?;
        Ok(node_ptr)
    }

    /// Finds the node at `path`, using `start_node_ptr` as the start if `path` is relative.
    pub fn path_node(&mut self, path: &Path, start_node_ptr: NodePtr) -> Result<NodePtr> {
        self._path_node(path, start_node_ptr, 0)
    }

    /// Internal implementation of the `path_node` function.
    /// `depth` describes how deep into the recursive call chain the function is.
    fn _path_node(
        &mut self,
        path: &Path,
        start_node_ptr: NodePtr,
        depth: usize,
    ) -> Result<NodePtr> {
        const MAX_DEPTH: usize = 16;
        if depth >= MAX_DEPTH {
            return Err(Error::TooManySymlinks);
        }

        let mut curr_node_ptr = start_node_ptr;
        for part in path.as_parts() {
            match part.as_ref() {
                "/" => {
                    curr_node_ptr = NodePtr::root();
                    continue;
                }
                "." => {
                    continue;
                }
                _ => (),
            }
            let entry = self.find_entry(curr_node_ptr, part.as_ref())?;
            curr_node_ptr = if entry.file_type == FileType::Symlink {
                let target = self.read_symlink(entry.node_ptr)?;
                self._path_node(&target, curr_node_ptr, depth + 1)?
            } else {
                entry.node_ptr
            }
        }
        Ok(curr_node_ptr)
    }

    /// Returns the address of the block in which the node resides.
    fn get_node_block_addr(&self, node_ptr: NodePtr) -> Option<BlockAddr> {
        let id = node_ptr.id();
        if id < self.fs.superblock.node_count {
            Some(self.fs.superblock.node_table_start + (id * NODE_SIZE as u64 / BLOCK_SIZE))
        } else {
            None
        }
    }

    /// Returns the byte offset of the node within the block.
    fn get_node_offset(&self, node_ptr: NodePtr) -> Option<u64> {
        let id = node_ptr.id();
        if id < self.fs.superblock.node_count {
            const NODES_PER_BLOCK: u64 = BLOCK_SIZE / NODE_SIZE as u64;
            Some(id % NODES_PER_BLOCK * NODE_SIZE as u64)
        } else {
            None
        }
    }
}

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Storage(libc::c_int),

    Alloc(alloc_map::Error),
    Dir(dir::Error),
    Node(node::Error),
    Path(path::Error),

    EntryNotFound,
    NotFile,
    IsDir,
    NotDir,
    DirNotEmpty,
    InvalidMove,
    NotSymlink,
    TooManySymlinks,

    // Filesystem corruption or logic error
    NodePtrOutOfBounds,
    CorruptedDir,
}

pub trait IntoTransactionResult {
    type T;

    fn into_transaction_res(self) -> Result<Self::T>;
}

impl<T> IntoTransactionResult for storage::Result<T> {
    type T = T;

    fn into_transaction_res(self) -> Result<Self::T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::Storage(e)),
        }
    }
}

impl From<alloc_map::Error> for Error {
    fn from(err: alloc_map::Error) -> Self {
        Self::Alloc(err)
    }
}

impl From<dir::Error> for Error {
    fn from(err: dir::Error) -> Self {
        Self::Dir(err)
    }
}

impl From<node::Error> for Error {
    fn from(err: node::Error) -> Self {
        Self::Node(err)
    }
}

impl From<path::Error> for Error {
    fn from(err: path::Error) -> Self {
        Self::Path(err)
    }
}

impl From<Error> for libc::c_int {
    fn from(err: Error) -> Self {
        match err {
            Error::Storage(errno) => errno,

            Error::Alloc(e) => e.into(),
            Error::Dir(e) => e.into(),
            Error::Node(e) => e.into(),
            Error::Path(e) => e.into(),

            Error::EntryNotFound => libc::ENOENT,
            Error::NotFile => libc::EINVAL,
            Error::IsDir => libc::EISDIR,
            Error::NotDir => libc::ENOTDIR,
            Error::DirNotEmpty => libc::ENOTEMPTY,
            Error::InvalidMove => libc::EINVAL,
            Error::NotSymlink => libc::EINVAL,
            Error::TooManySymlinks => libc::ELOOP,

            Error::NodePtrOutOfBounds => libc::EIO,
            Error::CorruptedDir => libc::EIO,
        }
    }
}
