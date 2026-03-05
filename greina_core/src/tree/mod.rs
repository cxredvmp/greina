mod node;

#[cfg(test)]
mod tests;

pub use self::node::{DataType, Key};

use core::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use self::node::*;

use crate::block::{Block, BlockAddr, allocator, storage::Storage};

pub const DATA_MAX_LEN: usize = 512;

#[derive(Default)]
struct Path(Vec<BlockAddr>);

struct Range<'a> {
    block: &'a mut Block,
    path: Path,
    start_bound: Bound<Key>,
    end_bound: Bound<Key>,
}

impl<'a> Iterator for Range<'a> {
    type Item = (Key, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<'a> DoubleEndedIterator for Range<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

// Range
impl<S: Storage> Tree<S> {
    /// Finds the leaf that may contain `key`, returning the leaf and traversal.
    /// On success, `block` will contain the found leaf.
    ///
    /// # Errors
    /// Returns an error on storage read failure or corrupted node data.
    fn traverse<'a>(
        storage: &S,
        mut root_addr: BlockAddr,
        block: &'a mut Block,
        key: Key,
    ) -> Result<Option<(&'a Node<LeafItem>, Path)>> {
        let mut traversal = Path::default();

        loop {
            traversal.0.push(root_addr);

            storage.read_at(block, root_addr)?;

            match NodeKind::try_from(&*block)? {
                NodeKind::Branch(branch) => {
                    let Some((_, addr)) = branch.get_child(key) else {
                        return Ok(None);
                    };
                    root_addr = addr;
                }

                NodeKind::Leaf(leaf) => {
                    // SAFETY: Workaround NLL problem case #3
                    let leaf = unsafe { &*(leaf as *const Node<LeafItem>) };
                    return Ok(Some((leaf, traversal)));
                }
            }
        }
    }

    ///
    pub fn range<'a>(
        storage: &S,
        block: &'a mut Block,
        root_addr: &BlockAddr,
        range: impl RangeBounds<Key>,
    ) -> Range<'a> {
        let mut range = Range {
            block,
            path: Default::default(),
            start_bound: range.start_bound().copied(),
            end_bound: range.end_bound().copied(),
        };
        range
    }
}

pub struct Tree<S> {
    _marker: PhantomData<S>,
}

impl<S: Storage> Tree<S> {
    pub fn format(storage: &mut S, block: &mut Block, root_addr: BlockAddr) -> Result<()> {
        Header::format(block, 0);
        storage.write_at(&block, root_addr)?;
        Ok(())
    }

    /// Retrieves the value associated with the key.
    /// On success, `block` will contain the leaf, containing the found value.
    ///
    /// # Errors
    /// Returns an error if a storage read fails, or if node data is corrupted.
    pub fn get<'a>(
        storage: &S,
        mut root_addr: BlockAddr,
        block: &'a mut Block,
        key: Key,
    ) -> Result<Option<&'a [u8]>> {
        loop {
            storage.read_at(block, root_addr)?;

            match NodeKind::try_from(&*block)? {
                NodeKind::Branch(branch) => {
                    let Some((_, addr)) = branch.get_child(key) else {
                        return Ok(None);
                    };
                    root_addr = addr;
                }

                NodeKind::Leaf(leaf) => {
                    // SAFETY: Workaround NLL problem case #3
                    let leaf = unsafe { &*(leaf as *const Node<LeafItem>) };
                    return Ok(leaf.get_data(key));
                }
            }
        }
    }

    /// Finds the leaf that may contain `key`, returning the mutable leaf and traversal.
    /// On success, `block` will contain the found leaf.
    ///
    /// # Errors
    /// Returns an error on storage read failure or corrupted node data.
    fn traverse_mut<'a>(
        storage: &S,
        mut root_addr: BlockAddr,
        block: &'a mut Block,
        key: Key,
    ) -> Result<Option<(&'a mut Node<LeafItem>, Path)>> {
        let mut traversal = Path::default();

        loop {
            traversal.0.push(root_addr);

            storage.read_at(block, root_addr)?;

            match NodeMutKind::try_from(&mut *block)? {
                NodeMutKind::Branch(branch) => {
                    let Some((_, addr)) = branch.get_child(key) else {
                        return Ok(None);
                    };
                    root_addr = addr;
                }

                NodeMutKind::Leaf(leaf) => {
                    // SAFETY: Workaround NLL problem case #3
                    let leaf = unsafe { &mut *(leaf as *mut Node<LeafItem>) };
                    return Ok(Some((leaf, traversal)));
                }
            }
        }
    }

    // pub fn insert(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     root_addr: &mut BlockAddr,
    //     key: Key,
    //     data: &[u8],
    // ) -> Result<Option<Box<[u8]>>> {
    //     // TODO: This is an inefficient and unsafe temporary solution
    //     let target_data = Self::remove(storage, block_alloc, root_addr, key)?;
    //     match Self::try_insert(storage, block_alloc, root_addr, key, data) {
    //         Ok(()) => Ok(target_data),
    //         Err(Error::Occupied) => unreachable!(),
    //         Err(e) => Err(e),
    //     }
    // }
    //
    // pub fn try_insert(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     root_addr: &mut BlockAddr,
    //     key: Key,
    //     data: &[u8],
    // ) -> Result<()> {
    //     if data.len() > DATA_MAX_LEN {
    //         return Err(Error::DataTooLong);
    //     }
    //
    //     match Self::insert_recursive(storage, block_alloc, *root_addr, key, data)? {
    //         InsertOutcome::Done => Ok(()),
    //         InsertOutcome::LowerBoundChanged(_) => Ok(()),
    //         InsertOutcome::Split(result) => {
    //             Self::handle_split_root(storage, block_alloc, root_addr, result)
    //         }
    //         InsertOutcome::SplitAndLowerBoundChanged {
    //             result,
    //             lower_bound: _,
    //         } => Self::handle_split_root(storage, block_alloc, root_addr, result),
    //     }
    // }
    //
    // fn handle_split_root(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     root_addr: &mut BlockAddr,
    //     result: SplitOutcome,
    // ) -> Result<()> {
    //     let mut old_root_block = Block::default();
    //     storage.read_at(&mut old_root_block, *root_addr)?;
    //     let (old_root_lower_bound, old_root_height) =
    //         match NodeVariant::try_new(&mut old_root_block)? {
    //             NodeVariant::Branch(old_root) => (old_root.lower_bound(), old_root.height()),
    //             NodeVariant::Leaf(old_root) => (old_root.lower_bound(), old_root.height()),
    //         };
    //
    //     let new_root_addr = block_alloc.allocate(1)?;
    //     let mut new_root_block = Block::default();
    //     let mut new_root = Branch::format(&mut new_root_block, old_root_height + 1);
    //
    //     new_root
    //         .insert(old_root_lower_bound, *root_addr)
    //         .expect("must be empty");
    //     new_root
    //         .insert(result.right_lower_bound, result.right_addr)
    //         .expect("must have one item");
    //
    //     storage.write_at(&new_root_block, new_root_addr)?;
    //     *root_addr = new_root_addr;
    //
    //     Ok(())
    // }
    //
    // fn insert_recursive(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     addr: BlockAddr,
    //     key: Key,
    //     data: &[u8],
    // ) -> Result<InsertOutcome> {
    //     let mut block = Block::default();
    //     storage.read_at(&mut block, addr)?;
    //
    //     match NodeVariant::try_new(&mut block)? {
    //         NodeVariant::Branch(mut branch) => {
    //             let child_idx = branch.child_idx_for(key);
    //             let child_addr = branch.child_at(child_idx).expect("child must exist");
    //
    //             match Self::insert_recursive(storage, block_alloc, child_addr, key, data)? {
    //                 InsertOutcome::Done => Ok(InsertOutcome::Done),
    //
    //                 InsertOutcome::Split(result) => {
    //                     Self::handle_split_child(storage, block_alloc, &mut branch, addr, result)
    //                 }
    //
    //                 InsertOutcome::LowerBoundChanged(child_lower_bound) => {
    //                     Self::handle_lower_bound_changed(
    //                         storage,
    //                         &mut branch,
    //                         addr,
    //                         child_idx,
    //                         child_lower_bound,
    //                     )
    //                 }
    //
    //                 InsertOutcome::SplitAndLowerBoundChanged {
    //                     result: child_result,
    //                     lower_bound: child_lower_bound,
    //                 } => {
    //                     let lower_bound_result = Self::handle_lower_bound_changed(
    //                         storage,
    //                         &mut branch,
    //                         addr,
    //                         child_idx,
    //                         child_lower_bound,
    //                     )?;
    //                     let split_result = Self::handle_split_child(
    //                         storage,
    //                         block_alloc,
    //                         &mut branch,
    //                         addr,
    //                         child_result,
    //                     )?;
    //                     use InsertOutcome::*;
    //                     let result = match (lower_bound_result, split_result) {
    //                         (Done, Done) => Done,
    //                         (Done, Split(result)) => Split(result),
    //                         (LowerBoundChanged(key), Done) => LowerBoundChanged(key),
    //                         (LowerBoundChanged(key), Split(result)) => SplitAndLowerBoundChanged {
    //                             result,
    //                             lower_bound: key,
    //                         },
    //                         _ => unreachable!(),
    //                     };
    //                     Ok(result)
    //                 }
    //             }
    //         }
    //
    //         NodeVariant::Leaf(mut leaf) => match leaf.insert(key, data) {
    //             Ok(()) => {
    //                 storage.write_at(leaf.block(), addr)?;
    //                 if key == leaf.lower_bound() {
    //                     Ok(InsertOutcome::LowerBoundChanged(key))
    //                 } else {
    //                     Ok(InsertOutcome::Done)
    //                 }
    //             }
    //
    //             Err(InsertError::Overflow) => {
    //                 let result = Self::handle_overflow(storage, block_alloc, &mut leaf, addr)?;
    //                 Self::handle_split_leaf(storage, &mut leaf, addr, key, data, result)
    //             }
    //
    //             Err(InsertError::Occupied) => Err(Error::Occupied),
    //         },
    //     }
    // }
    //
    // fn handle_lower_bound_changed(
    //     storage: &mut S,
    //     branch: &mut Branch<&mut Block>,
    //     branch_addr: BlockAddr,
    //     child_idx: usize,
    //     child_lower_bound: Key,
    // ) -> Result<InsertOutcome> {
    //     branch.set_key_at(child_idx, child_lower_bound);
    //     storage.write_at(branch.block(), branch_addr)?;
    //     let lower_bound = branch.lower_bound();
    //     if lower_bound == child_lower_bound {
    //         Ok(InsertOutcome::LowerBoundChanged(lower_bound))
    //     } else {
    //         Ok(InsertOutcome::Done)
    //     }
    // }
    //
    // fn handle_overflow<I: Item>(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     node: &mut Node<&mut Block, I>,
    //     node_addr: BlockAddr,
    // ) -> Result<SplitOutcome>
    // where
    //     for<'a> Node<&'a mut Block, I>: Split<Item = I>,
    // {
    //     let right_addr = block_alloc.allocate(1)?;
    //     let mut right_block = Block::default();
    //     let mut right = Node::<&mut Block, I>::format(&mut right_block, node.height());
    //
    //     node.split(&mut right);
    //     let right_lower_bound = right.lower_bound();
    //
    //     storage.write_at(node.block(), node_addr)?;
    //     storage.write_at(&right_block, right_addr)?;
    //
    //     Ok(SplitOutcome {
    //         right_lower_bound,
    //         right_addr,
    //     })
    // }
    //
    // fn handle_split_child(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     branch: &mut Branch<&mut Block>,
    //     branch_addr: BlockAddr,
    //     child_result: SplitOutcome,
    // ) -> Result<InsertOutcome> {
    //     match branch.insert(child_result.right_lower_bound, child_result.right_addr) {
    //         Ok(()) => {
    //             storage.write_at(branch.block(), branch_addr)?;
    //             // 'child_result.right_lower_bound' can't become a lower bound,
    //             // because it's the right sibling of some node
    //             Ok(InsertOutcome::Done)
    //         }
    //
    //         Err(InsertError::Overflow) => {
    //             let mut result = Self::handle_overflow(storage, block_alloc, branch, branch_addr)?;
    //             if child_result.right_lower_bound < result.right_lower_bound {
    //                 branch
    //                     .insert(child_result.right_lower_bound, child_result.right_addr)
    //                     .expect("must be able to insert after split");
    //                 storage.write_at(branch.block(), branch_addr)?;
    //                 // 'child_result.right_lower_bound' can't become a lower bound,
    //                 // because it's the right sibling of some node
    //                 Ok(InsertOutcome::Split(result))
    //             } else {
    //                 let mut block = Block::default();
    //                 storage.read_at(&mut block, result.right_addr)?;
    //                 let mut right = Branch::try_new(&mut block)?;
    //                 right
    //                     .insert(child_result.right_lower_bound, child_result.right_addr)
    //                     .expect("must be able to insert after split");
    //                 result.right_lower_bound = right.lower_bound();
    //                 storage.write_at(right.block(), result.right_addr)?;
    //                 Ok(InsertOutcome::Split(result))
    //             }
    //         }
    //
    //         Err(InsertError::Occupied) => unreachable!(),
    //     }
    // }
    //
    // fn handle_split_leaf(
    //     storage: &mut S,
    //     leaf: &mut Leaf<&mut Block>,
    //     leaf_addr: BlockAddr,
    //     key: Key,
    //     data: &[u8],
    //     mut result: SplitOutcome,
    // ) -> Result<InsertOutcome> {
    //     if key < result.right_lower_bound {
    //         match leaf.insert(key, data) {
    //             Ok(()) => (),
    //             Err(InsertError::Occupied) => return Err(Error::Occupied),
    //             Err(InsertError::Overflow) => unreachable!(),
    //         }
    //         storage.write_at(leaf.block(), leaf_addr)?;
    //         let lower_bound = leaf.lower_bound();
    //         if lower_bound == key {
    //             Ok(InsertOutcome::SplitAndLowerBoundChanged {
    //                 result,
    //                 lower_bound,
    //             })
    //         } else {
    //             Ok(InsertOutcome::Split(result))
    //         }
    //     } else {
    //         let mut block = Block::default();
    //         storage.read_at(&mut block, result.right_addr)?;
    //         let mut right = Leaf::try_new(&mut block)?;
    //         match right.insert(key, data) {
    //             Ok(()) => (),
    //             Err(InsertError::Occupied) => return Err(Error::Occupied),
    //             Err(InsertError::Overflow) => unreachable!(),
    //         }
    //         storage.write_at(right.block(), result.right_addr)?;
    //         result.right_lower_bound = right.lower_bound();
    //         Ok(InsertOutcome::Split(result))
    //     }
    // }
    //
    // pub fn remove(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     root_addr: &mut BlockAddr,
    //     key: Key,
    // ) -> Result<Option<Box<[u8]>>> {
    //     match Self::remove_recursive(storage, block_alloc, *root_addr, key)? {
    //         RemoveOutcome::BecameDeficient(data) => {
    //             Self::handle_deficient_root(storage, block_alloc, root_addr)?;
    //             Ok(data)
    //         }
    //
    //         RemoveOutcome::Done(data) => Ok(data),
    //     }
    // }
    //
    // fn handle_deficient_root(
    //     storage: &S,
    //     block_alloc: &mut impl block::Allocator,
    //     root_addr: &mut BlockAddr,
    // ) -> Result<()> {
    //     let mut block = Block::default();
    //     storage.read_at(&mut block, *root_addr)?;
    //     match NodeVariant::try_new(&block)? {
    //         NodeVariant::Branch(root) => {
    //             if root.item_count() == 1 {
    //                 let child_addr = root.child_at(0).expect("must have a child");
    //                 block_alloc.deallocate(*root_addr, 1)?;
    //                 *root_addr = child_addr;
    //             }
    //             Ok(())
    //         }
    //
    //         NodeVariant::Leaf(_) => Ok(()),
    //     }
    // }
    //
    // fn remove_recursive(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     root_addr: BlockAddr,
    //     key: Key,
    // ) -> Result<RemoveOutcome> {
    //     let mut block = Block::default();
    //     storage.read_at(&mut block, root_addr)?;
    //
    //     match NodeVariant::try_new(&mut block)? {
    //         NodeVariant::Branch(mut branch) => {
    //             let child_idx = branch.child_idx_for(key);
    //             let child_addr = branch.child_at(child_idx).expect("must have a child");
    //
    //             let data = match Self::remove_recursive(storage, block_alloc, child_addr, key)? {
    //                 RemoveOutcome::BecameDeficient(data) => {
    //                     let mut child_block = Block::default();
    //                     storage.read_at(&mut child_block, child_addr)?;
    //
    //                     match NodeVariant::try_new(&mut child_block)? {
    //                         NodeVariant::Branch(mut child) => Self::handle_deficient(
    //                             storage,
    //                             block_alloc,
    //                             &mut branch,
    //                             root_addr,
    //                             &mut child,
    //                             child_addr,
    //                             child_idx,
    //                         ),
    //
    //                         NodeVariant::Leaf(mut child) => Self::handle_deficient(
    //                             storage,
    //                             block_alloc,
    //                             &mut branch,
    //                             root_addr,
    //                             &mut child,
    //                             child_addr,
    //                             child_idx,
    //                         ),
    //                     }?;
    //
    //                     data
    //                 }
    //
    //                 RemoveOutcome::Done(data) => data,
    //             };
    //
    //             if branch.is_deficient() {
    //                 Ok(RemoveOutcome::BecameDeficient(data))
    //             } else {
    //                 Ok(RemoveOutcome::Done(data))
    //             }
    //         }
    //
    //         NodeVariant::Leaf(mut leaf) => {
    //             let data = leaf.remove(key);
    //             storage.write_at(leaf.block(), root_addr)?;
    //
    //             if leaf.is_deficient() {
    //                 Ok(RemoveOutcome::BecameDeficient(data))
    //             } else {
    //                 Ok(RemoveOutcome::Done(data))
    //             }
    //         }
    //     }
    // }
    //
    // fn handle_deficient<I: Item>(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     parent: &mut Branch<&mut Block>,
    //     parent_addr: BlockAddr,
    //     child: &mut Node<&mut Block, I>,
    //     child_addr: BlockAddr,
    //     child_idx: usize,
    // ) -> Result<()>
    // where
    //     for<'a> Node<&'a mut Block, I>: Rotate<Item = I>,
    // {
    //     let mut sibling_block = Block::default();
    //
    //     let right_idx = child_idx + 1;
    //     let right_addr = parent.child_at(right_idx);
    //     if let Some(right_addr) = right_addr {
    //         storage.read_at(&mut sibling_block, right_addr)?;
    //         let mut right = Node::<&mut Block, I>::try_new(&mut sibling_block)?;
    //
    //         if Self::rotate(
    //             storage,
    //             block_alloc,
    //             parent,
    //             parent_addr,
    //             child,
    //             child_addr,
    //             &mut right,
    //             right_addr,
    //             right_idx,
    //             DeficientSide::Left,
    //         )? {
    //             return Ok(());
    //         }
    //     };
    //
    //     let left_idx = child_idx.checked_sub(1);
    //     let left_addr = left_idx.and_then(|idx| parent.child_at(idx));
    //     if let Some(left_addr) = left_addr {
    //         storage.read_at(&mut sibling_block, left_addr)?;
    //         let mut left = Node::<&mut Block, I>::try_new(&mut sibling_block)?;
    //
    //         Self::rotate(
    //             storage,
    //             block_alloc,
    //             parent,
    //             parent_addr,
    //             &mut left,
    //             left_addr,
    //             child,
    //             child_addr,
    //             child_idx,
    //             DeficientSide::Right,
    //         )?;
    //     };
    //
    //     Ok(())
    // }
    //
    // fn rotate<I: Item>(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     parent: &mut Branch<&mut Block>,
    //     parent_addr: BlockAddr,
    //     left: &mut Node<&mut Block, I>,
    //     left_addr: BlockAddr,
    //     right: &mut Node<&mut Block, I>,
    //     right_addr: BlockAddr,
    //     right_idx: usize,
    //     side: DeficientSide,
    // ) -> Result<bool>
    // where
    //     for<'a> Node<&'a mut Block, I>: Rotate<Item = I>,
    // {
    //     let result = match side {
    //         DeficientSide::Left => left.rotate_left(right),
    //         DeficientSide::Right => right.rotate_right(left),
    //     };
    //
    //     if let Err(RotateError::SiblingBecomesDeficient) = result {
    //         return Self::merge(
    //             storage,
    //             block_alloc,
    //             parent,
    //             parent_addr,
    //             left,
    //             left_addr,
    //             right,
    //             right_addr,
    //             right_idx,
    //         );
    //     }
    //
    //     parent.set_key_at(right_idx, right.lower_bound());
    //
    //     storage.write_at(left.block(), left_addr)?;
    //     storage.write_at(right.block(), right_addr)?;
    //     storage.write_at(parent.block(), parent_addr)?;
    //
    //     Ok(true)
    // }
    //
    // fn merge<I: Item>(
    //     storage: &mut S,
    //     block_alloc: &mut impl block::Allocator,
    //     parent: &mut Branch<&mut Block>,
    //     parent_addr: BlockAddr,
    //     left: &mut Node<&mut Block, I>,
    //     left_addr: BlockAddr,
    //     right: &mut Node<&mut Block, I>,
    //     right_addr: BlockAddr,
    //     right_idx: usize,
    // ) -> Result<bool>
    // where
    //     for<'a> Node<&'a mut Block, I>: Rotate<Item = I>,
    // {
    //     if let Err(MergeError::Overflows) = left.merge(right) {
    //         return Ok(false);
    //     }
    //
    //     parent.remove_at(right_idx);
    //
    //     storage.write_at(left.block(), left_addr)?;
    //     storage.write_at(parent.block(), parent_addr)?;
    //
    //     block_alloc.deallocate(right_addr, 1)?;
    //
    //     Ok(true)
    // }
}

enum InsertOutcome {
    Done,
    Split(SplitOutcome),
    LowerFenceUpdated(Key),
    SplitAndLowerFenceUpdated {
        split_outcome: SplitOutcome,
        lower_fence: Key,
    },
}

struct SplitOutcome {
    right_lower_fence: Key,
    right_addr: BlockAddr,
}

enum RemoveOutcome {
    Done(Option<Box<[u8]>>),
    BecameUnderfull(Option<Box<[u8]>>),
}

enum DeficientSide {
    Left,
    Right,
}

#[derive(Debug)]
enum InsertError {
    Occupied,
    Overfull,
}

#[derive(Debug)]
struct SiblingBecomesUnderfull;

#[derive(Debug)]
struct Overflows;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Uninterpretable,
    Occupied,
    DataTooLong,

    Storage(libc::c_int),
    Allocator(allocator::Error),
}

impl From<libc::c_int> for Error {
    fn from(err: libc::c_int) -> Self {
        Error::Storage(err)
    }
}

impl From<allocator::Error> for Error {
    fn from(err: allocator::Error) -> Self {
        Error::Allocator(err)
    }
}
