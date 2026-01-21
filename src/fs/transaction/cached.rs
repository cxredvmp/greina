use std::collections::BTreeMap;

use crate::block::{
    Block, BlockAddr,
    storage::{Result, Storage},
};

pub(super) struct CachedStorage<'a, S> {
    inner: &'a mut S,
    cache: BTreeMap<BlockAddr, Block>,
}

impl<'a, S: Storage> CachedStorage<'a, S> {
    pub(super) fn new(inner: &'a mut S) -> Self {
        Self {
            inner,
            cache: Default::default(),
        }
    }

    pub(super) fn sync(&mut self) -> Result<()> {
        for (addr, block) in &self.cache {
            self.inner.write_at(block, *addr)?;
        }
        Ok(())
    }
}

impl<S: Storage> Storage for CachedStorage<'_, S> {
    fn read_at(&self, block: &mut Block, addr: BlockAddr) -> Result<()> {
        if let Some(cached) = self.cache.get(&addr) {
            *block = *cached;
            Ok(())
        } else {
            self.inner.read_at(block, addr)
        }
    }

    fn write_at(&mut self, block: &Block, addr: BlockAddr) -> Result<()> {
        self.cache.insert(addr, *block);
        Ok(())
    }

    fn capacity(&self) -> Result<u64> {
        self.inner.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::block::storage::map::MapStorage;

    #[test]
    fn reads_from_inner() {
        let mut inner = MapStorage::default();
        let mut write_block = Block::default();
        write_block.data.fill(0xAB);
        inner.write_at(&write_block, 0).unwrap();

        let cached = CachedStorage::new(&mut inner);

        let mut read_block = Block::default();
        cached.read_at(&mut read_block, 0).unwrap();
        assert_eq!(read_block.data, write_block.data);
    }

    #[test]
    fn buffers_writes() {
        let mut inner = MapStorage::default();
        let mut cached = CachedStorage::new(&mut inner);

        let mut write_block = Block::default();
        write_block.data.fill(0xAB);
        cached.write_at(&write_block, 0).unwrap();

        let mut read_block = Block::default();
        cached.read_at(&mut read_block, 0).unwrap();
        assert_eq!(read_block.data, write_block.data);

        let mut inner_read_block = Block::default();
        assert!(cached.inner.read_at(&mut inner_read_block, 0).is_err());
    }

    #[test]
    fn syncs_writes_to_inner() {
        let mut inner = MapStorage::default();
        let mut cached = CachedStorage::new(&mut inner);

        let mut write_block_1 = Block::default();
        let mut write_block_2 = Block::default();

        write_block_1.data.fill(0xAB);
        write_block_2.data.fill(0xCD);

        cached.write_at(&write_block_1, 0).unwrap();
        cached.write_at(&write_block_2, 1).unwrap();

        cached.sync().unwrap();

        let mut inner_read_block_1 = Block::default();
        let mut inner_read_block_2 = Block::default();

        cached.inner.read_at(&mut inner_read_block_1, 0).unwrap();
        cached.inner.read_at(&mut inner_read_block_2, 1).unwrap();

        assert_eq!(inner_read_block_1.data, write_block_1.data);
        assert_eq!(inner_read_block_2.data, write_block_2.data);
    }
}
