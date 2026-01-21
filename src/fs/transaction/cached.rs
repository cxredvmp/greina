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
