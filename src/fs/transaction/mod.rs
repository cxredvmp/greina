mod buf;
use buf::*;

use crate::{
    block::storage::Storage,
    fs::{
        Filesystem,
        error::Result,
        node::{
            FileType, Node, NodeId,
            dir::{Dir, DirEntry, DirEntryName},
            file::File,
            symlink::Symlink,
        },
        superblock::Superblock,
    },
};

/// Filesystem operation that buffers changes in memory before commiting them to persistent storage.
pub struct Transaction<'a, S: Storage> {
    storage: BufStorage<'a, S>,
    fs_superblock: &'a mut Superblock,
    superblock: Superblock,
    block_alloc: BufAllocator<'a>,
}

impl<'a, S: Storage> Transaction<'a, S> {
    /// Constructs a `Transaction` for a given filesystem.
    pub(super) fn new(fs: &'a mut Filesystem<S>) -> Self {
        let superblock = fs.superblock.clone();
        Self {
            storage: BufStorage::new(&mut fs.storage),
            fs_superblock: &mut fs.superblock,
            superblock,
            block_alloc: BufAllocator::new(&mut fs.block_alloc),
        }
    }

    /// Commits the transaction to storage, consuming itself.
    pub(super) fn commit(mut self) -> Result<()> {
        self.sync_superblock()?;
        self.block_alloc
            .sync(&mut self.storage, self.superblock.block_alloc_start)?;
        self.storage.sync()?;
        Ok(())
    }

    /// Queues a synchronization of allocation maps.
    fn sync_superblock(&mut self) -> Result<()> {
        Filesystem::write_superblock(&mut self.storage, &self.superblock)?;
        *self.fs_superblock = self.superblock.clone();
        Ok(())
    }

    pub fn create_node(&mut self, filetype: FileType, links: u32) -> Result<NodeId> {
        Node::create(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            filetype,
            links,
        )
    }

    pub fn read_node(&self, id: NodeId) -> Result<Node> {
        Node::read(&self.storage, &self.superblock, id)
    }

    pub fn write_node(&mut self, node: &Node, id: NodeId) -> Result<()> {
        node.write(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            id,
        )
    }

    pub fn remove_node(&mut self, id: NodeId) -> Result<()> {
        Node::remove(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            id,
        )
    }

    pub fn find_entry(&self, parent: NodeId, name: &str) -> Result<DirEntry> {
        let name = DirEntryName::try_from(name)?;
        DirEntry::read(&self.storage, &self.superblock, parent, name.hash())
    }

    pub fn create_dir(&mut self, parent: NodeId, name: &str) -> Result<NodeId> {
        let name = DirEntryName::try_from(name)?;
        Dir::create(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            parent,
            name,
        )
    }

    pub fn remove_dir(&mut self, parent: NodeId, name: &str) -> Result<NodeId> {
        Dir::remove(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            parent,
            name,
        )
    }

    pub fn read_dir(&self, id: NodeId) -> Result<Vec<DirEntry>> {
        Dir::list(&self.storage, &self.superblock, id)
    }

    pub fn create_root_dir(&mut self) -> Result<NodeId> {
        let id = Node::create(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            FileType::Dir,
            1,
        )?;

        assert_eq!(id, NodeId::ROOT, "root must have id 1, got {:?}", id);

        DirEntry::create(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            id,
            FileType::Dir,
            id,
            DirEntryName::itself(),
        )?;

        DirEntry::create(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            id,
            FileType::Dir,
            id,
            DirEntryName::parent(),
        )?;

        Ok(id)
    }

    pub fn create_file(
        &mut self,
        parent: NodeId,
        name: &str,
        filetype: FileType,
    ) -> Result<NodeId> {
        File::create(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            parent,
            filetype,
            name,
        )
    }

    pub fn read_file_at(&self, id: NodeId, offset: u64, buf: &mut [u8]) -> Result<u64> {
        File::read_at(&self.storage, &self.superblock, id, offset, buf)
    }

    pub fn write_file_at(&mut self, id: NodeId, offset: u64, buf: &[u8]) -> Result<u64> {
        File::write_at(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            id,
            offset,
            buf,
        )
    }

    pub fn truncate_file(&mut self, id: NodeId, size: u64) -> Result<()> {
        // TODO: Check if the node is a file
        // TODO: Deallocate extents
        let mut node = self.read_node(id)?;
        node.size.set(size);
        self.write_node(&node, id)
    }

    pub fn create_symlink(&mut self, parent: NodeId, name: &str, target: &str) -> Result<NodeId> {
        Symlink::create(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            parent,
            name,
            target,
        )
    }

    pub fn read_symlink(&self, id: NodeId) -> Result<Box<[u8]>> {
        Symlink::read(&self.storage, &self.superblock, id)
    }

    pub fn link_file(&mut self, parent: NodeId, id: NodeId, name: &str) -> Result<()> {
        let name = DirEntryName::try_from(name)?;
        DirEntry::link(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            parent,
            id,
            name,
        )
    }

    pub fn unlink_file(&mut self, parent: NodeId, name: &str) -> Result<()> {
        let name = DirEntryName::try_from(name)?;
        DirEntry::unlink(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            parent,
            &name,
        )
    }

    pub fn rename_entry(
        &mut self,
        old_parent: NodeId,
        old_name: &str,
        new_parent: NodeId,
        new_name: &str,
    ) -> Result<()> {
        let old_name = DirEntryName::try_from(old_name)?;
        let new_name = DirEntryName::try_from(new_name)?;
        DirEntry::rename(
            &mut self.storage,
            &mut self.block_alloc,
            &mut self.superblock,
            old_parent,
            &old_name,
            new_parent,
            &new_name,
        )
    }
}
