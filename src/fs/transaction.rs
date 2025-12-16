use std::collections::BTreeMap;

use libc::c_int;
use zerocopy::{FromBytes, IntoBytes, TryFromBytes};

use crate::{
    block::{BLOCK_SIZE, Block, BlockAddr},
    fs::{
        Filesystem,
        alloc_map::{self},
        directory::{self, Dir, DirEntry, DirEntryName},
        node::{self, FileType, NODE_SIZE, Node, NodePtr},
        path::{self, Path},
    },
    storage::{self, Storage},
};

/// Cache to buffer changes.
type Changes = BTreeMap<BlockAddr, Block>;

/// Filesystem operation that buffers changes in memory before commiting them to persistent storage.
pub struct Transaction<'a, S: Storage> {
    fs: &'a mut Filesystem<S>,
    changes: Changes,
}

impl<'a, S: Storage> Transaction<'a, S> {
    /// Constructs a `Transaction` for a given filesystem.
    pub fn new(fs: &'a mut Filesystem<S>) -> Self {
        Self {
            fs,
            changes: Changes::new(),
        }
    }

    /// Commits the transaction to storage, consuming itself.
    pub fn commit(mut self) -> Result<()> {
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
        Ok(Node::try_read_from_bytes(
            &block.data[offset as usize..(offset as usize + NODE_SIZE as usize)],
        )
        .expect("'bytes' must be a valid 'Node'"))
    }

    // Queues a write of the node to the node table.
    pub fn write_node(&mut self, node: &Node, node_ptr: NodePtr) -> Result<()> {
        let addr = self
            .get_node_block_addr(node_ptr)
            .ok_or(Error::NodePtrOutOfBounds)?;
        let mut block = Block::default();
        self.read_block_at(&mut block, addr)?;
        let offset = self
            .get_node_offset(node_ptr)
            .ok_or(Error::NodePtrOutOfBounds)?;
        block.data[offset as usize..(offset as usize + NODE_SIZE as usize)]
            .copy_from_slice(node.as_bytes());
        self.write_block_at(&block, addr);
        Ok(())
    }

    /// Allocates a node, returning it and its pointer.
    pub fn create_node(&mut self, filetype: FileType) -> Result<(Node, NodePtr)> {
        let node = Node::new(filetype);
        let (id, _) = self.fs.node_map.allocate(1).map_err(Error::Alloc)?;
        let node_ptr = NodePtr::new(id);
        self.write_node(&node, node_ptr)?;
        Ok((node, node_ptr))
    }

    /// Removes the node, freeing its blocks.
    pub fn remove_node(&mut self, node_ptr: NodePtr) -> Result<()> {
        let mut node = self.read_node(node_ptr)?;

        let spans = node.truncate(0);
        for span in spans {
            // Free the blocks
            self.fs.block_map.free(span).map_err(Error::Alloc)?;
        }

        // Free the node
        let id = node_ptr.id();
        self.fs.node_map.free((id, id + 1)).map_err(Error::Alloc)?;

        let node = Node::default();
        self.write_node(&node, node_ptr)?;

        Ok(())
    }

    /// Reads a number of bytes from the file into `buf` starting from `offset`.
    /// Returns the number of bytes read.
    pub fn read_file_at(&self, node_ptr: NodePtr, offset: u64, buf: &mut [u8]) -> Result<u64> {
        let node = self.read_node(node_ptr)?;

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

        Ok(bytes_read)
    }

    // BUG: Doesn't allow to write past the end of the file.
    /// Writes a number of bytes from `buf` to the file starting from `offset`.
    /// Returns the number of byttes written.
    pub fn write_file_at(&mut self, node_ptr: NodePtr, offset: u64, buf: &[u8]) -> Result<u64> {
        let mut node = self.read_node(node_ptr)?;

        if offset > node.size {
            return Ok(0);
        };

        let bytes_to_write = buf.len() as u64;
        let mut bytes_written = 0;
        let mut node_updated = false;

        while bytes_written != bytes_to_write {
            let curr_pos = offset + bytes_written;
            let offset_in_block = curr_pos % BLOCK_SIZE; // First write might be unaligned
            let block_offset = Node::get_block_offset_from_offset(curr_pos);
            let (addr, has_alloc) = match node.get_block_addr(block_offset) {
                Some(addr) => (addr, false),
                None => {
                    // Allocate a block
                    let (addr, _) = self.fs.block_map.allocate(1).map_err(Error::Alloc)?;
                    node.map_block(block_offset, addr).map_err(Error::Node)?;
                    node_updated = true;
                    (addr, true)
                }
            };
            let chunk_size = (BLOCK_SIZE - offset_in_block).min(bytes_to_write - bytes_written);
            // Don't need to read if it's a freshly allocated block
            let mut block = Block::default();
            if !has_alloc {
                self.read_block_at(&mut block, addr)?
            };
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
            node_updated = true;
        }

        if node_updated {
            self.write_node(&node, node_ptr)?;
        }

        Ok(bytes_written)
    }

    /// Creates a file with a given name and type inside `parent_ptr`.
    /// Returns the file's node pointer.
    pub fn create_file(
        &mut self,
        parent_ptr: NodePtr,
        name: &str,
        filetype: FileType,
    ) -> Result<NodePtr> {
        let name = DirEntryName::try_from(name).map_err(Error::Dir)?;

        let mut parent = self.read_directory(parent_ptr)?;
        if parent.get_entry(name).is_some() {
            return Err(Error::FileExists);
        }

        let (mut node, node_ptr) = self.create_node(filetype)?;
        node.link_count += 1;

        let entry = DirEntry::new(node_ptr, filetype, name);
        parent.add_entry(entry);

        self.write_directory(parent_ptr, &parent)?;
        self.write_node(&node, node_ptr)?;

        Ok(node_ptr)
    }

    /// Truncates the file's size to `size`.
    pub fn truncate_file(&mut self, node_ptr: NodePtr, size: u64) -> Result<()> {
        let mut node = self.read_node(node_ptr)?;
        if node.filetype() != FileType::File {
            return Err(Error::NotFile);
        }

        let spans = node.truncate(size);
        for span in spans {
            // Free the blocks, if shrinked
            self.fs.block_map.free(span).map_err(Error::Alloc)?;
        }

        self.write_node(&node, node_ptr)?;
        Ok(())
    }

    /// Reads the directory.
    pub fn read_directory(&self, node_ptr: NodePtr) -> Result<Dir> {
        let node = self.read_node(node_ptr)?;
        if node.filetype() != FileType::Dir {
            return Err(Error::NotDir);
        }
        let mut buf = vec![0u8; node.size as usize];
        self.read_file_at(node_ptr, 0, &mut buf)?;
        let entries = <[DirEntry]>::try_ref_from_bytes(&buf).map_err(|_| Error::CorruptedDir)?;
        Ok(Dir::from_slice(entries))
    }

    /// Writes the directory.
    pub fn write_directory(&mut self, node_ptr: NodePtr, dir: &Dir) -> Result<()> {
        let bytes = dir.as_slice().as_bytes();
        self.write_file_at(node_ptr, 0, bytes)?;
        Ok(())
    }

    /// Creates a directory with a given name inside `parent_ptr`.
    /// Returns the directory's node pointer.
    pub fn create_directory(&mut self, parent_ptr: NodePtr, name: &str) -> Result<NodePtr> {
        let node_ptr = self.create_file(parent_ptr, name, FileType::Dir)?;
        let dir = Dir::new(node_ptr, parent_ptr);
        self.write_directory(node_ptr, &dir)?;
        Ok(node_ptr)
    }

    /// Removes the empty directory `name` inside `parent_ptr`.
    pub fn remove_directory(&mut self, parent_ptr: NodePtr, name: &str) -> Result<()> {
        let mut parent_dir = self.read_directory(parent_ptr)?;

        let name = DirEntryName::try_from(name)?;
        let entry = parent_dir.get_entry(name).ok_or(Error::NodeNotFound)?;

        if entry.filetype() != FileType::Dir {
            return Err(Error::NotDir);
        }

        let node_ptr = entry.node_ptr();
        let dir = self.read_directory(node_ptr)?;
        if !dir.is_empty() {
            return Err(Error::DirNotEmpty);
        }

        parent_dir.remove_entry(name)?;
        self.write_directory(parent_ptr, &parent_dir)?;

        self.remove_node(node_ptr)
    }

    /// Creates a hard link to the file with a given name.
    pub fn link_file(&mut self, parent_ptr: NodePtr, node_ptr: NodePtr, name: &str) -> Result<()> {
        let name = DirEntryName::try_from(name).map_err(Error::Dir)?;

        let mut dir = self.read_directory(parent_ptr)?;
        if dir.get_entry(name).is_some() {
            return Err(Error::FileExists);
        }

        let mut node = self.read_node(node_ptr)?;
        if node.filetype() == FileType::Dir {
            return Err(Error::IsDir);
        }

        let entry = DirEntry::new(node_ptr, node.filetype(), name);
        dir.add_entry(entry);
        node.link_count += 1;

        self.write_node(&node, node_ptr)?;
        self.write_directory(parent_ptr, &dir)?;
        Ok(())
    }

    /// Removes a hard link to the file with a given name.
    /// If `free` is true, deletes the node if `node.link_count` drops to 0, else it must be deallocated manually.
    pub fn unlink_file(&mut self, parent_ptr: NodePtr, name: &str, free: bool) -> Result<()> {
        let name = DirEntryName::try_from(name).map_err(Error::Dir)?;

        let mut dir = self.read_directory(parent_ptr)?;
        let entry = dir.get_entry(name).ok_or(Error::NodeNotFound)?;
        if entry.filetype() == FileType::Dir {
            return Err(Error::IsDir);
        }

        let node_ptr = dir.remove_entry(name).map_err(Error::Dir)?;
        self.write_directory(parent_ptr, &dir)?;

        let mut node = self.read_node(node_ptr)?;
        node.link_count -= 1;

        if node.link_count == 0 && free {
            self.remove_node(node_ptr)?;
        } else {
            self.write_node(&node, node_ptr)?;
        }

        Ok(())
    }

    /// Returns the path contained inside `symlink_ptr`.
    pub fn read_symlink(&self, symlink_ptr: NodePtr) -> Result<Path<'_>> {
        let node = self.read_node(symlink_ptr)?;
        if node.filetype() != FileType::Symlink {
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
        target: &Path,
    ) -> Result<NodePtr> {
        let node_ptr = self.create_file(parent_ptr, name, FileType::Symlink)?;
        self.write_file_at(node_ptr, 0, target.as_bytes())?;
        Ok(node_ptr)
    }

    /// Finds the entry named `name` inside `parent_ptr`.
    pub fn find_entry(&self, parent_ptr: NodePtr, name: &str) -> Result<DirEntry> {
        let name = DirEntryName::try_from(name)?;
        let dir = self.read_directory(parent_ptr)?;
        dir.get_entry(name).ok_or(Error::NodeNotFound).copied()
    }

    /// Finds the node at `path`, using `start_node_ptr` as the start if `path` is relative.
    pub fn path_node(&self, path: &Path, start_node_ptr: NodePtr) -> Result<NodePtr> {
        self._path_node(path, start_node_ptr, 0)
    }

    /// Internal implementation of the `path_node` function.
    /// `depth` describes how deep into the recursive call chain the function is.
    fn _path_node(&self, path: &Path, start_node_ptr: NodePtr, depth: usize) -> Result<NodePtr> {
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
            curr_node_ptr = if entry.filetype() == FileType::Symlink {
                let target = self.read_symlink(entry.node_ptr())?;
                self._path_node(&target, curr_node_ptr, depth + 1)?
            } else {
                entry.node_ptr()
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
    Storage(c_int),
    NodePtrOutOfBounds,
    Alloc(alloc_map::Error),
    Dir(directory::Error),
    Node(node::Error),
    Path(path::Error),
    NodeNotFound,
    NotFile,
    NotDir,
    IsDir,
    CorruptedDir,
    DirNotEmpty,
    FileExists,
    NotSymlink,
    TooManySymlinks,
}

impl From<directory::Error> for Error {
    fn from(value: directory::Error) -> Self {
        Self::Dir(value)
    }
}

impl From<path::Error> for Error {
    fn from(value: path::Error) -> Self {
        Self::Path(value)
    }
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
