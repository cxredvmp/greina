use std::collections::BTreeMap;

use zerocopy::{FromBytes, IntoBytes, TryFromBytes};

use crate::{
    hardware::storage::{
        Storage,
        block::{BLOCK_SIZE, Block},
    },
    kernel::fs::{
        Filesystem, ROOT_INDEX,
        alloc_map::{self, AllocMap},
        directory::{self, Dir, DirEntry, DirEntryName},
        node::{self, FileType, NODE_SIZE, NODES_PER_BLOCK, Node},
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
        for (&block_index, block) in self.changes.iter() {
            self.storage
                .write_block(block_index, block)
                .expect("'block_index' must be a valid block index")
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
            fs.superblock.block_map_offset,
        );
        Self::_sync_map(
            storage,
            changes,
            &fs.node_map,
            fs.superblock.node_map_offset,
        );
    }

    // Internal implementation of 'sync_maps' for a single map.
    // Separated to split borrows.
    fn _sync_map(storage: &Storage, changes: &mut Changes, map: &AllocMap, map_offset: usize) {
        let bytes = map.as_slice().as_bytes();
        for (i, chunk) in bytes.chunks(BLOCK_SIZE).enumerate() {
            let block_mem = Block::read_from_bytes(chunk).unwrap_or_else(|_| Block::new(chunk));
            // Check if in-memory and stored blocks differ
            let block_index = map_offset + i;
            let block_stored = Self::_read_block(storage, changes, block_index)
                .expect("Must be able to read the allocation map");
            if block_mem.data != block_stored.data {
                Self::_write_block(changes, map_offset + i, &block_mem);
            }
        }
    }

    /// Reads the node from the node table.
    pub fn read_node(&self, node_index: usize) -> Result<Node> {
        let block_index = self
            .get_node_block_index(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        let block = self.read_block(block_index)?;
        let byte_offset = self
            .get_node_byte_offset(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        Ok(
            Node::try_read_from_bytes(&block.data[byte_offset..(byte_offset + NODE_SIZE)])
                .expect("'bytes' must be a valid 'Node'"),
        )
    }

    // Queues a write of the node to the node table.
    pub fn write_node(&mut self, node_index: usize, node: Node) -> Result<()> {
        let block_index = self
            .get_node_block_index(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        let mut block = self.read_block(block_index)?;
        let byte_offset = self
            .get_node_byte_offset(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        block.data[byte_offset..(byte_offset + NODE_SIZE)].copy_from_slice(node.as_bytes());
        self.write_block(block_index, &block);
        Ok(())
    }

    /// Allocates a [Node], returning it and its index.
    pub fn create_node(&mut self, filetype: FileType) -> Result<(Node, usize)> {
        let node = Node::new(filetype);
        let (node_index, _) = self.fs.node_map.allocate(1).map_err(Error::Alloc)?;
        self.write_node(node_index, node)?;
        Ok((node, node_index))
    }

    /// Reads a number of bytes from the file starting from a given offset into the buffer.
    /// Returns the number of bytes read.
    pub fn read_file_at(&self, node_index: usize, offset: usize, buf: &mut [u8]) -> Result<usize> {
        let node = self.read_node(node_index)?;

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
            match node.get_physical_block_from_offset(curr_pos) {
                Some(block_index) => {
                    let data = self.read_block(block_index)?.data;
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
        node_index: usize,
        offset: usize,
        data: &[u8],
    ) -> Result<usize> {
        let mut node = self.read_node(node_index)?;

        if offset > node.size {
            return Ok(0);
        };

        let bytes_to_write = data.len();
        let mut bytes_written = 0;
        let mut node_updated = false;

        while bytes_written != bytes_to_write {
            let curr_pos = offset + bytes_written;
            let offset_in_block = curr_pos % BLOCK_SIZE; // First read might be unaligned
            let logic_block = Node::get_logical_block_from_offset(curr_pos);
            let (phys_block, has_alloc) = match node.get_physical_block(logic_block) {
                Some(index) => (index, false),
                None => {
                    // Allocate a physical block
                    let (phys_block, _) = self.fs.block_map.allocate(1).map_err(Error::Alloc)?;
                    node.map_block(logic_block, phys_block)
                        .map_err(Error::Node)?;
                    node_updated = true;
                    (phys_block, true)
                }
            };
            let chunk_size = (BLOCK_SIZE - offset_in_block).min(bytes_to_write - bytes_written);
            // Don't need to read if it's a freshly allocated block
            let mut block = if has_alloc {
                Block::default()
            } else {
                self.read_block(phys_block)?
            };
            block.data[offset_in_block..(offset_in_block + chunk_size)]
                .copy_from_slice(&data[bytes_written..(bytes_written + chunk_size)]);
            self.write_block(phys_block, &block);
            bytes_written += chunk_size;
        }

        let end_pos = offset + bytes_written;
        if end_pos > node.size {
            node.size = end_pos;
            node_updated = true;
        }

        if node_updated {
            self.write_node(node_index, node)?;
        }

        Ok(bytes_written)
    }

    /// Truncates the file to specified size.
    pub fn truncate_file(&mut self, node_index: usize, size: usize) -> Result<()> {
        let mut node = self.read_node(node_index)?;

        if node.filetype() != FileType::File {
            return Err(Error::NotFile);
        }

        if size >= node.size {
            node.size = size;
            self.write_node(node_index, node)?;
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
        self.write_node(node_index, node)?;
        Ok(())
    }

    /// Creates a file with the given name and type inside the specified parent directory, returning its node index.
    pub fn create_file(
        &mut self,
        parent_index: usize,
        name: &str,
        filetype: FileType,
    ) -> Result<usize> {
        let name = DirEntryName::try_from(name).map_err(Error::Dir)?;

        let (mut node, node_index) = self.create_node(filetype)?;
        node.link_count += 1;

        let entry = DirEntry::new(node_index, filetype, name);
        let mut parent = self.read_directory(parent_index)?;
        parent.add_entry(entry);

        self.write_directory(parent_index, &parent)?;
        self.write_node(node_index, node)?;

        Ok(node_index)
    }

    /// Reads the directory.
    pub fn read_directory(&self, node_index: usize) -> Result<Dir> {
        let node = self.read_node(node_index)?;
        if node.filetype() != FileType::Dir {
            return Err(Error::NotDir);
        }
        let mut buf = vec![0u8; node.size];
        self.read_file_at(node_index, 0, &mut buf)?;
        let entries = <[DirEntry]>::try_ref_from_bytes(&buf).map_err(|_| Error::CorruptedDir)?;
        Ok(Dir::from_slice(entries))
    }

    /// Writes the directory.
    pub fn write_directory(&mut self, node_index: usize, dir: &Dir) -> Result<()> {
        let bytes = dir.as_slice().as_bytes();
        self.write_file_at(node_index, 0, bytes)?;
        Ok(())
    }

    /// Creates a directory with the given name inside the specified parent directory, returning its node index.
    pub fn create_directory(&mut self, parent_index: usize, name: &str) -> Result<usize> {
        let node_index = self.create_file(parent_index, name, FileType::Dir)?;
        let dir = Dir::new(node_index, parent_index);
        self.write_directory(node_index, &dir)?;
        Ok(node_index)
    }

    /// Removes the empty directory `name` inside the parent directory pointed to by `parent`.
    pub fn remove_directory(&mut self, parent: usize, name: &str) -> Result<()> {
        let mut parent_dir = self.read_directory(parent)?;

        let name = DirEntryName::try_from(name)?;
        let entry = parent_dir.get_entry(name).ok_or(Error::NodeNotFound)?;

        if entry.filetype() != FileType::Dir {
            return Err(Error::NotDir);
        }

        let node_index = entry.node_index();
        let dir = self.read_directory(node_index)?;
        if !dir.is_empty() {
            return Err(Error::DirNotEmpty);
        }

        parent_dir.remove_entry(name)?;
        self.write_directory(parent, &parent_dir)?;

        self.delete_node(node_index)
    }

    /// Creates a hard link to the file with a given name.
    pub fn link_file(&mut self, parent_index: usize, node_index: usize, name: &str) -> Result<()> {
        let name = DirEntryName::try_from(name).map_err(Error::Dir)?;

        let mut node = self.read_node(node_index)?;
        if node.filetype() == FileType::Dir {
            return Err(Error::IsDir);
        }

        node.link_count += 1;
        self.write_node(node_index, node)?;

        let mut dir = self.read_directory(parent_index)?;
        let entry = DirEntry::new(node_index, node.filetype(), name);
        dir.add_entry(entry);
        self.write_directory(parent_index, &dir)?;

        Ok(())
    }

    /// Removes a hard link to the file with a given name.
    /// If `free` is true, deletes the node if `node.link_count` drops to 0, else it must be deallocated manually.
    pub fn unlink_file(&mut self, parent_index: usize, name: &str, free: bool) -> Result<()> {
        let name = DirEntryName::try_from(name).map_err(Error::Dir)?;

        let mut dir = self.read_directory(parent_index)?;
        let entry = dir.get_entry(name).ok_or(Error::NodeNotFound)?;
        if entry.filetype() == FileType::Dir {
            return Err(Error::IsDir);
        }

        let node_index = dir.remove_entry(name).map_err(Error::Dir)?;
        self.write_directory(parent_index, &dir)?;

        let mut node = self.read_node(node_index)?;
        node.link_count -= 1;

        if node.link_count == 0 && free {
            self.delete_node(node_index)?;
        } else {
            self.write_node(node_index, node)?;
        }

        Ok(())
    }

    /// Deletes the node, deallocating its physical blocks.
    pub fn delete_node(&mut self, node_index: usize) -> Result<()> {
        let node = self.read_node(node_index)?;
        let extents = node.get_extents().iter().take_while(|e| !e.is_null());
        for extent in extents {
            self.fs
                .block_map
                .free(extent.span())
                .map_err(Error::Alloc)?;
        }
        self.fs
            .node_map
            .free((node_index, node_index + 1))
            .map_err(Error::Alloc)?;
        let node = Node::default();
        self.write_node(node_index, node)?;
        Ok(())
    }

    /// Returns the node index to which `symlink` points to.
    pub fn resolve_symlink(&self, symlink: usize, parent: usize) -> Result<usize> {
        todo!()
    }

    /// Finds the node at `path`, using `curr_dir` as parent if `path` is relative.
    pub fn find_node(&self, path: &str, curr_dir: usize) -> Result<usize> {
        let is_absolute = path.starts_with('/');
        let parent = if is_absolute { ROOT_INDEX } else { curr_dir };
        let mut curr_node = parent;

        let tokens = path.split('/').filter(|t| !t.is_empty());
        for token in tokens {
            let dir = self.read_directory(curr_node)?;
            let name = DirEntryName::try_from(token)?;
            let entry = dir.get_entry(name).ok_or(Error::NodeNotFound)?;

            let filetype = entry.filetype();
            if filetype == FileType::Symlink {
                curr_node = self.resolve_symlink(entry.node_index(), curr_node)?;
            } else {
                curr_node = entry.node_index();
            }
        }
        Ok(curr_node)
    }

    // Internal implementation of 'read_block'.
    // Separated to split borrows in some contexts.
    fn _read_block(storage: &Storage, changes: &Changes, block_index: usize) -> Result<Block> {
        // Check cached changes
        match changes.get(&block_index) {
            Some(block) => Ok(*block),
            None => storage
                .read_block(block_index)
                .map_err(|_| Error::BlockIndexOutOfBounds),
        }
    }

    /// Reads the physical block.
    pub fn read_block(&self, block_index: usize) -> Result<Block> {
        Self::_read_block(self.storage, &self.changes, block_index)
    }

    // Internal implementation of 'write_block'.
    // Separated to split borrows in some contexts.
    fn _write_block(changes: &mut Changes, block_index: usize, block: &Block) {
        changes.insert(block_index, *block);
    }

    /// Queues a write of the physical block.
    pub fn write_block(&mut self, block_index: usize, block: &Block) {
        Self::_write_block(&mut self.changes, block_index, block);
    }

    /// Returns the index of the block in which the node resides.
    fn get_node_block_index(&self, node_index: usize) -> Option<usize> {
        if node_index < self.fs.superblock.node_count {
            Some(self.fs.superblock.node_table_offset + (node_index * NODE_SIZE / BLOCK_SIZE))
        } else {
            None
        }
    }

    /// Returns the byte offset of the node within the block.
    fn get_node_byte_offset(&self, node_index: usize) -> Option<usize> {
        if node_index < self.fs.superblock.node_count {
            Some(node_index % NODES_PER_BLOCK * NODE_SIZE)
        } else {
            None
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    BlockIndexOutOfBounds,
    NodeIndexOutOfBounds,
    LogicalIndexOutOfBounds,
    Alloc(alloc_map::Error),
    Dir(directory::Error),
    Node(node::Error),
    NodeNotFound,
    NotFile,
    NotDir,
    IsDir,
    CorruptedDir,
    DirNotEmpty,
}

impl From<directory::Error> for Error {
    fn from(value: directory::Error) -> Self {
        Self::Dir(value)
    }
}
