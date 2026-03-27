use std::{collections::HashMap, sync::RwLock};

use crate::block::{
    Block, BlockAddr,
    storage::{Result, Storage},
};

#[derive(Default)]
pub struct FakeStorage {
    inner: RwLock<FakeStorageInner>,
}

impl Storage for FakeStorage {
    fn read_at(&self, block: &mut Block, addr: BlockAddr) -> Result<()> {
        let inner = self.inner.read().unwrap();
        inner.read_at(block, addr)
    }

    fn write_at(&self, block: &Block, addr: BlockAddr) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.write_at(block, addr)
    }

    fn capacity(&self) -> Result<u64> {
        let inner = self.inner.read().unwrap();
        inner.capacity()
    }
}

#[derive(Default)]
struct FakeStorageInner {
    blocks: HashMap<BlockAddr, Block>,
}

impl FakeStorageInner {
    fn read_at(&self, block: &mut Block, addr: BlockAddr) -> Result<()> {
        *block = *self.blocks.get(&addr).ok_or(libc::EIO)?;
        Ok(())
    }

    fn write_at(&mut self, block: &Block, addr: BlockAddr) -> Result<()> {
        self.blocks.insert(addr, *block);
        Ok(())
    }

    fn capacity(&self) -> Result<u64> {
        Ok(0)
    }
}
