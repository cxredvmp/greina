use std::collections::HashMap;

use crate::block::{
    Block, BlockAddr,
    storage::{Result, Storage},
};

#[derive(Default)]
pub struct MapStorage {
    capacity: u64,
    blocks: HashMap<BlockAddr, Block>,
}

impl MapStorage {
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            ..Default::default()
        }
    }
}

impl Storage for MapStorage {
    fn read_at(&self, block: &mut Block, addr: BlockAddr) -> Result<()> {
        *block = *self.blocks.get(&addr).ok_or(libc::EIO)?;
        Ok(())
    }

    fn write_at(&mut self, block: &Block, addr: BlockAddr) -> Result<()> {
        self.blocks.insert(addr, *block);
        Ok(())
    }

    fn capacity(&self) -> Result<u64> {
        Ok(self.capacity)
    }
}
