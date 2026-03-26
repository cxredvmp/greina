use std::collections::HashSet;

use crate::block::{
    BlockAddr,
    allocator::{Allocator, Error, Result},
};

#[derive(Default)]
pub struct FakeAllocator {
    allocations: HashSet<BlockAddr>,
    next: BlockAddr,
}

impl Allocator for FakeAllocator {
    fn allocate(&mut self, count: u64) -> Result<BlockAddr> {
        let start = self.next;
        self.next += count;
        for i in 0..count {
            self.allocations.insert(start + i);
        }
        Ok(start)
    }

    fn deallocate(&mut self, start: BlockAddr, count: u64) -> Result<()> {
        for i in 0..count {
            if !self.allocations.contains(&(start + i)) {
                return Err(Error::NotAllocated);
            }
        }
        for i in 0..count {
            self.allocations.remove(&(start + i));
        }
        Ok(())
    }

    fn available(&self) -> u64 {
        u64::MAX
    }
}
