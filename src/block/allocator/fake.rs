use std::{collections::HashSet, sync::Mutex};

use crate::block::{
    BlockAddr,
    allocator::{Allocator, Error, Result},
};

#[derive(Default)]
pub struct FakeAllocator {
    inner: Mutex<FakeAllocatorInner>,
}

impl Allocator for FakeAllocator {
    fn allocate(&self, count: u64) -> Result<BlockAddr> {
        let mut inner = self.inner.lock().unwrap();
        inner.allocate(count)
    }

    fn deallocate(&self, start: BlockAddr, count: u64) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.deallocate(start, count)
    }

    fn available(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.available()
    }
}

#[derive(Default)]
struct FakeAllocatorInner {
    allocs: HashSet<BlockAddr>,
    next: BlockAddr,
}

impl FakeAllocatorInner {
    fn allocate(&mut self, count: u64) -> Result<BlockAddr> {
        let start = self.next;
        self.next += count;
        for i in 0..count {
            self.allocs.insert(start + i);
        }
        Ok(start)
    }

    fn deallocate(&mut self, start: BlockAddr, count: u64) -> Result<()> {
        for i in 0..count {
            if !self.allocs.contains(&(start + i)) {
                return Err(Error::NotAllocated);
            }
        }
        for i in 0..count {
            self.allocs.remove(&(start + i));
        }
        Ok(())
    }

    fn available(&self) -> u64 {
        u64::MAX
    }
}
