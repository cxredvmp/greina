use crate::block::{allocator::Allocator, storage::Storage};

use super::*;
use file::*;

pub struct Symlink;

impl Symlink {
    pub fn create(
        storage: &mut impl Storage,
        allocator: &mut impl Allocator,
        superblock: &mut Superblock,
        parent: NodeId,
        name: &str,
        target: &str,
    ) -> Result<NodeId> {
        let id = File::create(
            storage,
            allocator,
            superblock,
            parent,
            FileType::Symlink,
            name,
        )?;
        File::write_at(storage, allocator, superblock, id, 0, target.as_bytes())?;
        Ok(id)
    }

    pub fn read(storage: &impl Storage, superblock: &Superblock, id: NodeId) -> Result<Box<[u8]>> {
        let node = Node::read(storage, superblock, id)?;
        if node.filetype != FileType::Symlink {
            return Err(Error::NotSymlink);
        }
        let mut buf = vec![0u8; node.size.get() as usize];
        File::read_at(storage, superblock, id, 0, &mut buf)?;
        Ok(buf.into())
    }
}
