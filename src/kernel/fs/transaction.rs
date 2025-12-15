use std::collections::BTreeMap;

use zerocopy::{FromBytes, IntoBytes, TryFromBytes};

use crate::{
    hardware::storage::{
        Storage,
        block::{BLOCK_SIZE, Block},
    },
    kernel::fs::{
        Filesystem,
        alloc_map::{self, AllocMap},
        directory::{self, Dir, DirEntry, DirEntryName},
        node::{self, FileType, NODE_SIZE, NODES_PER_BLOCK, Node, NodePtr},
        path::{self, Path},
    },
};

/// A cache to buffer changes.
type Changes = BTreeMap<usize, Block>;

/// A filesystem operation that buffers changes in memory before commiting them to persistent storage.
pub struct Transaction<'a> {
    fs: &'a mut Filesystem,
    storage: &'a mut Storage,
    changes: Changes,
}

impl<'a> Transaction<'a> {
    /// Constructs a [Transaction] for the given filesystem and storage.
    pub fn new(fs: &'a mut Filesystem, storage: &'a mut Storage) -> Self {
        Self {
            fs,
            storage,
            changes: Changes::new(),
        }
    }

    /// Commits the transaction to persistent storage, consuming the transaction.
    pub fn commit(mut self) {
        self.sync_maps();
        for (&block_id, block) in self.changes.iter() {
            self.storage
                .write_block(block_id, block)
                .expect("'block_id' must be a valid block id")
        }
    }

    /// Queues a synchronization of allocation maps.
    fn sync_maps(&mut self) {
        let fs = &self.fs;
        let storage = &self.storage;
        let changes = &mut self.changes;
        Self::_sync_map(
            storage,
            changes,
            &fs.block_map,
            fs.superblock.block_map_start,
        );
        Self::_sync_map(storage, changes, &fs.node_map, fs.superblock.node_map_start);
    }

    // Internal implementation of 'sync_maps' for a single map.
    // Separated to split borrows.
    fn _sync_map(storage: &Storage, changes: &mut Changes, map: &AllocMap, map_start: usize) {
        let bytes = map.as_slice().as_bytes();
        for (i, chunk) in bytes.chunks(BLOCK_SIZE).enumerate() {
            let block_mem = Block::read_from_bytes(chunk).unwrap_or_else(|_| Block::new(chunk));
            // Check if in-memory and stored blocks differ
            let block_id = map_start + i;
            let block_stored = Self::_read_block(storage, changes, block_id)
                .expect("Must be able to read the allocation map");
            if block_mem.data != block_stored.data {
                Self::_write_block(changes, map_start + i, &block_mem);
            }
        }
    }

    /// Reads the node from the node table.
    pub fn read_node(&self, node_ptr: NodePtr) -> Result<Node> {
        let block_id = self
            .get_node_block_id(node_ptr)
            .ok_or(Error::NodePtrOutOfBounds)?;
        let block = self.read_block(block_id)?;
        let offset = self
            .get_node_offset(node_ptr)
            .ok_or(Error::NodePtrOutOfBounds)?;
        Ok(
            Node::try_read_from_bytes(&block.data[offset..(offset + NODE_SIZE)])
                .expect("'bytes' must be a valid 'Node'"),
        )
    }

    // Queues a write of the node to the node table.
    pub fn write_node(&mut self, node_ptr: NodePtr, node: Node) -> Result<()> {
        let block_id = self
            .get_node_block_id(node_ptr)
            .ok_or(Error::NodePtrOutOfBounds)?;
        let mut block = self.read_block(block_id)?;
        let offset = self
            .get_node_offset(node_ptr)
            .ok_or(Error::NodePtrOutOfBounds)?;
        block.data[offset..(offset + NODE_SIZE)].copy_from_slice(node.as_bytes());
        self.write_block(block_id, &block);
        Ok(())
    }

    /// Allocates a [Node], returning it and its pointer.
    pub fn create_node(&mut self, filetype: FileType) -> Result<(Node, NodePtr)> {
        let node = Node::new(filetype);
        let (id, _) = self.fs.node_map.allocate(1).map_err(Error::Alloc)?;
        let node_ptr = NodePtr::new(id);
        self.write_node(node_ptr, node)?;
        Ok((node, node_ptr))
    }

    /// Reads a number of bytes from the file starting from a given offset into the buffer.
    /// Returns the number of bytes read.
    pub fn read_file_at(&self, node_ptr: NodePtr, offset: usize, buf: &mut [u8]) -> Result<usize> {
        let node = self.read_node(node_ptr)?;

        if offset >= node.size {
            return Ok(0);
        };

        let bytes_available = node.size - offset;
        let bytes_to_read = bytes_available.min(buf.len());
        let mut bytes_read = 0;

        while bytes_read != bytes_to_read {
            let curr_pos = offset + bytes_read;
            let offset_in_block = curr_pos % BLOCK_SIZE; // First read might be unaligned
            let chunk_size = (BLOCK_SIZE - offset_in_block).min(bytes_to_read - bytes_read);
            match node.get_block_id_from_offset(curr_pos) {
                Some(block_id) => {
                    let data = self.read_block(block_id)?.data;
                    buf[bytes_read..(bytes_read + chunk_size)]
                        .copy_from_slice(&data[offset_in_block..(offset_in_block + chunk_size)]);
                }
                // Handle a sparse file
                None => {
                    buf[bytes_read..(bytes_read + chunk_size)].fill(0u8);
                }
            };
            bytes_read += chunk_size;
        }

        Ok(bytes_read)
    }

    // BUG: Doesn't allow to write past the end of the file yet.
    /// Writes a byte slice to the file starting from a given offset.
    /// Returns the number of byttes written.
    pub fn write_file_at(
        &mut self,
        node_ptr: NodePtr,
        offset: usize,
        data: &[u8],
    ) -> Result<usize> {
        let mut node = self.read_node(node_ptr)?;

        if offset > node.size {
            return Ok(0);
        };

        let bytes_to_write = data.len();
        let mut bytes_written = 0;
        let mut node_updated = false;

        while bytes_written != bytes_to_write {
            let curr_pos = offset + bytes_written;
            let offset_in_block = curr_pos % BLOCK_SIZE; // First read might be unaligned
            let block_offset = Node::get_block_offset_from_offset(curr_pos);
            let (block_id, has_alloc) = match node.get_block_id(block_offset) {
                Some(block_id) => (block_id, false),
                None => {
                    // Allocate a block
                    let (block_id, _) = self.fs.block_map.allocate(1).map_err(Error::Alloc)?;
                    node.map_block(block_offset, block_id)
                        .map_err(Error::Node)?;
                    node_updated = true;
                    (block_id, true)
                }
            };
            let chunk_size = (BLOCK_SIZE - offset_in_block).min(bytes_to_write - bytes_written);
            // Don't need to read if it's a freshly allocated block
            let mut block = if has_alloc {
                Block::default()
            } else {
                self.read_block(block_id)?
            };
            block.data[offset_in_block..(offset_in_block + chunk_size)]
                .copy_from_slice(&data[bytes_written..(bytes_written + chunk_size)]);
            self.write_block(block_id, &block);
            bytes_written += chunk_size;
        }

        let end_pos = offset + bytes_written;
        if end_pos > node.size {
            node.size = end_pos;
            node_updated = true;
        }

        if node_updated {
            self.write_node(node_ptr, node)?;
        }

        Ok(bytes_written)
    }

    /// Truncates the size of the file to `size`.
    pub fn truncate_file(&mut self, node_ptr: NodePtr, size: usize) -> Result<()> {
        let mut node = self.read_node(node_ptr)?;

        if node.filetype() != FileType::File {
            return Err(Error::NotFile);
        }

        if size >= node.size {
            node.size = size;
            self.write_node(node_ptr, node)?;
            return Ok(());
        }

        let blocks_needed = size.div_ceil(BLOCK_SIZE);
        let mut blocks_passed = 0;
        for extent in node.get_mut_extents() {
            if extent.is_null() {
                break;
            }
            let extent_len = extent.len();
            if blocks_passed >= blocks_needed {
                // Extent is entirely beyond the size
                self.fs
                    .block_map
                    .free(extent.span())
                    .map_err(Error::Alloc)?;
                extent.nullify();
            } else if blocks_passed + extent_len >= blocks_needed {
                // Extent is partially needed
                let blocks_keep = blocks_needed - blocks_passed;
                let new_end = extent.start() + blocks_keep;
                self.fs
                    .block_map
                    .free((new_end, extent.end()))
                    .map_err(Error::Alloc)?;
                extent.shrink(blocks_keep);
            }
            blocks_passed += extent_len;
        }

        node.size = size;
        self.write_node(node_ptr, node)?;
        Ok(())
    }

    /// Creates a file with given name and type inside `parent_ptr`.
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
        self.write_node(node_ptr, node)?;

        Ok(node_ptr)
    }

    /// Reads the directory.
    pub fn read_directory(&self, node_ptr: NodePtr) -> Result<Dir> {
        let node = self.read_node(node_ptr)?;
        if node.filetype() != FileType::Dir {
            return Err(Error::NotDir);
        }
        let mut buf = vec![0u8; node.size];
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

    /// Creates a directory with given name inside `parent_ptr`.
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

        self.write_node(node_ptr, node)?;
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
            self.write_node(node_ptr, node)?;
        }

        Ok(())
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

    /// Removes the node, deallocating its blocks.
    pub fn remove_node(&mut self, node_ptr: NodePtr) -> Result<()> {
        let node = self.read_node(node_ptr)?;
        let extents = node.get_extents().iter().take_while(|e| !e.is_null());
        for extent in extents {
            self.fs
                .block_map
                .free(extent.span())
                .map_err(Error::Alloc)?;
        }
        let id = node_ptr.id();
        self.fs.node_map.free((id, id + 1)).map_err(Error::Alloc)?;
        let node = Node::default();
        self.write_node(node_ptr, node)?;
        Ok(())
    }

    /// Returns the path contained inside `symlink_ptr`.
    pub fn read_symlink(&self, symlink_ptr: NodePtr) -> Result<Path<'_>> {
        let node = self.read_node(symlink_ptr)?;
        if node.filetype() != FileType::Symlink {
            return Err(Error::NotSymlink);
        }
        let mut buf = vec![0u8; node.size];
        self.read_file_at(symlink_ptr, 0, &mut buf)?;
        Ok(Path::try_from_bytes_owned(&buf)?)
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

    // Internal implementation of 'read_block'.
    // Separated to split borrows in some contexts.
    fn _read_block(storage: &Storage, changes: &Changes, block_id: usize) -> Result<Block> {
        // Check cached changes
        match changes.get(&block_id) {
            Some(block) => Ok(*block),
            None => storage
                .read_block(block_id)
                .map_err(|_| Error::BlockIdOutOfBounds),
        }
    }

    /// Reads the block.
    pub fn read_block(&self, block_id: usize) -> Result<Block> {
        Self::_read_block(self.storage, &self.changes, block_id)
    }

    // Internal implementation of 'write_block'.
    // Separated to split borrows in some contexts.
    fn _write_block(changes: &mut Changes, block_id: usize, block: &Block) {
        changes.insert(block_id, *block);
    }

    /// Queues a write of the block.
    pub fn write_block(&mut self, block_id: usize, block: &Block) {
        Self::_write_block(&mut self.changes, block_id, block);
    }

    /// Returns the id of the block in which the node resides.
    fn get_node_block_id(&self, node_ptr: NodePtr) -> Option<usize> {
        let id = node_ptr.id();
        if id < self.fs.superblock.node_count {
            Some(self.fs.superblock.node_table_start + (id * NODE_SIZE / BLOCK_SIZE))
        } else {
            None
        }
    }

    /// Returns the byte offset of the node within the block.
    fn get_node_offset(&self, node_ptr: NodePtr) -> Option<usize> {
        let id = node_ptr.id();
        if id < self.fs.superblock.node_count {
            Some(id % NODES_PER_BLOCK * NODE_SIZE)
        } else {
            None
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    BlockIdOutOfBounds,
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
