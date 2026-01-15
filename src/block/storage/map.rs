use std::collections::HashMap;

use crate::block::{
    Block, BlockAddr,
    storage::{Result, Storage},
};

#[derive(Default)]
pub struct MapStorage {
    blocks: HashMap<BlockAddr, Block>,
}

impl Storage for MapStorage {
    fn read_at(&self, block: &mut Block, addr: BlockAddr) -> Result<()> {
        *block = *self.blocks.get(&addr).expect(&format!(
            "attempted to read uninitialized block at address {}",
            addr
        ));
        Ok(())
    }

    fn write_at(&mut self, block: &Block, addr: BlockAddr) -> Result<()> {
        self.blocks.insert(addr, *block);
        Ok(())
    }

    fn capacity(&mut self) -> Result<u64> {
        Ok(u64::MAX)
    }
}

