#[cfg(test)]
mod tests;

mod node;

use std::marker::PhantomData;

use crate::{
    block::{
        Block, BlockAddr,
        allocator::{self, Allocator},
        storage::Storage,
    },
    tree::node::*,
};

const DATA_MAX_LEN: usize = 512;

pub struct Tree<S> {
    _storage: PhantomData<S>,
}

impl<S> Tree<S>
where
    S: Storage,
{
    pub fn get(storage: &S, mut root_addr: BlockAddr, key: Key) -> Result<Option<Box<[u8]>>> {
        let mut block = Block::default();

        let leaf = loop {
            storage.read_at(&mut block, root_addr)?;

            match NodeVariant::try_new(&block)? {
                NodeVariant::Branch(branch) => {
                    root_addr = branch.child_for(key);
                }

                NodeVariant::Leaf(leaf) => break leaf,
            }
        };

        Ok(leaf.get(key).map(|data| data.to_vec().into_boxed_slice()))
    }

    pub fn insert<A: Allocator>(
        storage: &mut S,
        allocator: &mut A,
        root_addr: &mut BlockAddr,
        key: Key,
        data: &[u8],
    ) -> Result<()> {
        if data.len() > DATA_MAX_LEN {
            return Err(Error::DataTooLong);
        }

        match Self::insert_recursive(storage, allocator, *root_addr, key, data)? {
            InsertOutcome::Done => Ok(()),
            InsertOutcome::LowerBoundChanged(_) => Ok(()),
            InsertOutcome::Split(result) => {
                Self::handle_split_root(storage, allocator, root_addr, result)
            }
            InsertOutcome::SplitAndLowerBoundChanged {
                result,
                lower_bound: _,
            } => Self::handle_split_root(storage, allocator, root_addr, result),
        }
    }

    fn handle_split_root<A: Allocator>(
        storage: &mut S,
        allocator: &mut A,
        root_addr: &mut BlockAddr,
        result: SplitOutcome,
    ) -> Result<()> {
        let mut old_root_block = Block::default();
        storage.read_at(&mut old_root_block, *root_addr)?;
        let (old_root_lower_bound, old_root_height) =
            match NodeVariant::try_new(&mut old_root_block)? {
                NodeVariant::Branch(old_root) => (old_root.lower_bound(), old_root.height()),
                NodeVariant::Leaf(old_root) => (old_root.lower_bound(), old_root.height()),
            };

        let new_root_addr = allocator.allocate(1)?;
        let mut new_root_block = Block::default();
        let mut new_root = Branch::format(&mut new_root_block, old_root_height + 1);

        new_root
            .insert(old_root_lower_bound, *root_addr)
            .expect("must be empty");
        new_root
            .insert(result.right_lower_bound, result.right_addr)
            .expect("must have one item");

        storage.write_at(&new_root_block, new_root_addr)?;
        *root_addr = new_root_addr;

        Ok(())
    }

    fn insert_recursive<A: Allocator>(
        storage: &mut S,
        allocator: &mut A,
        addr: BlockAddr,
        key: Key,
        data: &[u8],
    ) -> Result<InsertOutcome> {
        let mut block = Block::default();
        storage.read_at(&mut block, addr)?;

        match NodeVariant::try_new(&mut block)? {
            NodeVariant::Branch(mut branch) => {
                let child_idx = branch.child_idx_for(key);
                let child_addr = branch.child_at(child_idx).expect("child must exist");

                match Self::insert_recursive(storage, allocator, child_addr, key, data)? {
                    InsertOutcome::Done => Ok(InsertOutcome::Done),

                    InsertOutcome::Split(result) => {
                        Self::handle_split_child(storage, allocator, &mut branch, addr, result)
                    }

                    InsertOutcome::LowerBoundChanged(child_lower_bound) => {
                        Self::handle_lower_bound_changed(
                            storage,
                            &mut branch,
                            addr,
                            child_idx,
                            child_lower_bound,
                        )
                    }

                    InsertOutcome::SplitAndLowerBoundChanged {
                        result: child_result,
                        lower_bound: child_lower_bound,
                    } => {
                        let lower_bound_result = Self::handle_lower_bound_changed(
                            storage,
                            &mut branch,
                            addr,
                            child_idx,
                            child_lower_bound,
                        )?;
                        let split_result = Self::handle_split_child(
                            storage,
                            allocator,
                            &mut branch,
                            addr,
                            child_result,
                        )?;
                        use InsertOutcome::*;
                        let result = match (lower_bound_result, split_result) {
                            (Done, Done) => Done,
                            (Done, Split(result)) => Split(result),
                            (LowerBoundChanged(key), Done) => LowerBoundChanged(key),
                            (LowerBoundChanged(key), Split(result)) => SplitAndLowerBoundChanged {
                                result,
                                lower_bound: key,
                            },
                            _ => unreachable!(),
                        };
                        Ok(result)
                    }
                }
            }

            NodeVariant::Leaf(mut leaf) => match leaf.insert(key, data) {
                Ok(()) => {
                    storage.write_at(leaf.block(), addr)?;
                    if key == leaf.lower_bound() {
                        Ok(InsertOutcome::LowerBoundChanged(key))
                    } else {
                        Ok(InsertOutcome::Done)
                    }
                }

                Err(InsertError::Overflow) => {
                    let result = Self::handle_overflow(storage, allocator, &mut leaf, addr)?;
                    Self::handle_split_leaf(storage, &mut leaf, addr, key, data, result)
                }

                Err(InsertError::Occupied) => Err(Error::Occupied),
            },
        }
    }

    fn handle_lower_bound_changed(
        storage: &mut S,
        branch: &mut Branch<&mut Block>,
        branch_addr: BlockAddr,
        child_idx: usize,
        child_lower_bound: Key,
    ) -> Result<InsertOutcome> {
        branch.set_key_at(child_idx, child_lower_bound);
        storage.write_at(branch.block(), branch_addr)?;
        let lower_bound = branch.lower_bound();
        if lower_bound == child_lower_bound {
            Ok(InsertOutcome::LowerBoundChanged(lower_bound))
        } else {
            Ok(InsertOutcome::Done)
        }
    }

    fn handle_overflow<A: Allocator, I: Item>(
        storage: &mut S,
        allocator: &mut A,
        node: &mut Node<&mut Block, I>,
        node_addr: BlockAddr,
    ) -> Result<SplitOutcome>
    where
        for<'a> Node<&'a mut Block, I>: Split<Item = I>,
    {
        let right_addr = allocator.allocate(1)?;
        let mut right_block = Block::default();
        let mut right = Node::<&mut Block, I>::format(&mut right_block, node.height());

        node.split(&mut right);
        let right_lower_bound = right.lower_bound();

        storage.write_at(node.block(), node_addr)?;
        storage.write_at(&right_block, right_addr)?;

        Ok(SplitOutcome {
            right_lower_bound,
            right_addr,
        })
    }

    fn handle_split_child<A: Allocator>(
        storage: &mut S,
        allocator: &mut A,
        branch: &mut Branch<&mut Block>,
        branch_addr: BlockAddr,
        child_result: SplitOutcome,
    ) -> Result<InsertOutcome> {
        match branch.insert(child_result.right_lower_bound, child_result.right_addr) {
            Ok(()) => {
                storage.write_at(branch.block(), branch_addr)?;
                // 'child_result.right_lower_bound' can't become a lower bound,
                // because it's the right sibling of some node
                Ok(InsertOutcome::Done)
            }

            Err(InsertError::Overflow) => {
                let mut result = Self::handle_overflow(storage, allocator, branch, branch_addr)?;
                if child_result.right_lower_bound < result.right_lower_bound {
                    branch
                        .insert(child_result.right_lower_bound, child_result.right_addr)
                        .expect("must be able to insert after split");
                    storage.write_at(branch.block(), branch_addr)?;
                    // 'child_result.right_lower_bound' can't become a lower bound,
                    // because it's the right sibling of some node
                    Ok(InsertOutcome::Split(result))
                } else {
                    let mut block = Block::default();
                    storage.read_at(&mut block, result.right_addr)?;
                    let mut right = Branch::try_new(&mut block)?;
                    right
                        .insert(child_result.right_lower_bound, child_result.right_addr)
                        .expect("must be able to insert after split");
                    result.right_lower_bound = right.lower_bound();
                    storage.write_at(right.block(), result.right_addr)?;
                    Ok(InsertOutcome::Split(result))
                }
            }

            Err(InsertError::Occupied) => unreachable!(),
        }
    }

    fn handle_split_leaf(
        storage: &mut S,
        leaf: &mut Leaf<&mut Block>,
        leaf_addr: BlockAddr,
        key: Key,
        data: &[u8],
        mut result: SplitOutcome,
    ) -> Result<InsertOutcome> {
        if key < result.right_lower_bound {
            match leaf.insert(key, data) {
                Ok(()) => (),
                Err(InsertError::Occupied) => return Err(Error::Occupied),
                Err(InsertError::Overflow) => unreachable!(),
            }
            storage.write_at(leaf.block(), leaf_addr)?;
            let lower_bound = leaf.lower_bound();
            if lower_bound == key {
                Ok(InsertOutcome::SplitAndLowerBoundChanged {
                    result,
                    lower_bound,
                })
            } else {
                Ok(InsertOutcome::Split(result))
            }
        } else {
            let mut block = Block::default();
            storage.read_at(&mut block, result.right_addr)?;
            let mut right = Leaf::try_new(&mut block)?;
            match right.insert(key, data) {
                Ok(()) => (),
                Err(InsertError::Occupied) => return Err(Error::Occupied),
                Err(InsertError::Overflow) => unreachable!(),
            }
            storage.write_at(right.block(), result.right_addr)?;
            result.right_lower_bound = right.lower_bound();
            Ok(InsertOutcome::Split(result))
        }
    }

    pub fn remove<A: Allocator>(
        storage: &mut S,
        allocator: &mut A,
        root_addr: &mut BlockAddr,
        key: Key,
    ) -> Result<Option<Box<[u8]>>> {
        match Self::remove_recursive(storage, allocator, *root_addr, key)? {
            RemoveOutcome::BecameDeficient(data) => {
                Self::handle_deficient_root(storage, allocator, root_addr)?;
                Ok(data)
            }

            RemoveOutcome::Done(data) => Ok(data),
        }
    }

    fn handle_deficient_root<A: Allocator>(
        storage: &S,
        allocator: &mut A,
        root_addr: &mut BlockAddr,
    ) -> Result<()> {
        let mut block = Block::default();
        storage.read_at(&mut block, *root_addr)?;
        match NodeVariant::try_new(&block)? {
            NodeVariant::Branch(root) => {
                if root.item_count() == 1 {
                    let child_addr = root.child_at(0).expect("must have a child");
                    allocator.deallocate(*root_addr, 1)?;
                    *root_addr = child_addr;
                }
                Ok(())
            }

            NodeVariant::Leaf(_) => Ok(()),
        }
    }

    fn remove_recursive<A: Allocator>(
        storage: &mut S,
        allocator: &mut A,
        root_addr: BlockAddr,
        key: Key,
    ) -> Result<RemoveOutcome> {
        let mut block = Block::default();
        storage.read_at(&mut block, root_addr)?;

        match NodeVariant::try_new(&mut block)? {
            NodeVariant::Branch(mut branch) => {
                let child_idx = branch.child_idx_for(key);
                let child_addr = branch.child_at(child_idx).expect("must have a child");

                let data = match Self::remove_recursive(storage, allocator, child_addr, key)? {
                    RemoveOutcome::BecameDeficient(data) => {
                        let mut child_block = Block::default();
                        storage.read_at(&mut child_block, child_addr)?;

                        match NodeVariant::try_new(&mut child_block)? {
                            NodeVariant::Branch(mut child) => Self::handle_deficient(
                                storage,
                                allocator,
                                &mut branch,
                                root_addr,
                                &mut child,
                                child_addr,
                                child_idx,
                            ),

                            NodeVariant::Leaf(mut child) => Self::handle_deficient(
                                storage,
                                allocator,
                                &mut branch,
                                root_addr,
                                &mut child,
                                child_addr,
                                child_idx,
                            ),
                        }?;

                        data
                    }

                    RemoveOutcome::Done(data) => data,
                };

                if branch.is_deficient() {
                    Ok(RemoveOutcome::BecameDeficient(data))
                } else {
                    Ok(RemoveOutcome::Done(data))
                }
            }

            NodeVariant::Leaf(mut leaf) => {
                let data = leaf.remove(key);
                storage.write_at(leaf.block(), root_addr)?;

                if leaf.is_deficient() {
                    Ok(RemoveOutcome::BecameDeficient(data))
                } else {
                    Ok(RemoveOutcome::Done(data))
                }
            }
        }
    }

    fn handle_deficient<A: Allocator, I: Item>(
        storage: &mut S,
        allocator: &mut A,
        parent: &mut Branch<&mut Block>,
        parent_addr: BlockAddr,
        child: &mut Node<&mut Block, I>,
        child_addr: BlockAddr,
        child_idx: usize,
    ) -> Result<()>
    where
        for<'a> Node<&'a mut Block, I>: Rotate<Item = I>,
    {
        let mut sibling_block = Block::default();

        let right_idx = child_idx + 1;
        let right_addr = parent.child_at(right_idx);
        if let Some(right_addr) = right_addr {
            storage.read_at(&mut sibling_block, right_addr)?;
            let mut right = Node::<&mut Block, I>::try_new(&mut sibling_block)?;

            if Self::rotate(
                storage,
                allocator,
                parent,
                parent_addr,
                child,
                child_addr,
                &mut right,
                right_addr,
                right_idx,
                DeficientSide::Left,
            )? {
                return Ok(());
            }
        };

        let left_idx = child_idx.checked_sub(1);
        let left_addr = left_idx.and_then(|idx| parent.child_at(idx));
        if let Some(left_addr) = left_addr {
            storage.read_at(&mut sibling_block, left_addr)?;
            let mut left = Node::<&mut Block, I>::try_new(&mut sibling_block)?;

            Self::rotate(
                storage,
                allocator,
                parent,
                parent_addr,
                &mut left,
                left_addr,
                child,
                child_addr,
                child_idx,
                DeficientSide::Right,
            )?;
        };

        Ok(())
    }

    fn rotate<A: Allocator, I: Item>(
        storage: &mut S,
        allocator: &mut A,
        parent: &mut Branch<&mut Block>,
        parent_addr: BlockAddr,
        left: &mut Node<&mut Block, I>,
        left_addr: BlockAddr,
        right: &mut Node<&mut Block, I>,
        right_addr: BlockAddr,
        right_idx: usize,
        side: DeficientSide,
    ) -> Result<bool>
    where
        for<'a> Node<&'a mut Block, I>: Rotate<Item = I>,
    {
        let result = match side {
            DeficientSide::Left => left.rotate_left(right),
            DeficientSide::Right => right.rotate_right(left),
        };

        if let Err(RotateError::SiblingBecomesDeficient) = result {
            return Self::merge(
                storage,
                allocator,
                parent,
                parent_addr,
                left,
                left_addr,
                right,
                right_addr,
                right_idx,
            );
        }

        parent.set_key_at(right_idx, right.lower_bound());

        storage.write_at(left.block(), left_addr)?;
        storage.write_at(right.block(), right_addr)?;
        storage.write_at(parent.block(), parent_addr)?;

        Ok(true)
    }

    fn merge<A: Allocator, I: Item>(
        storage: &mut S,
        allocator: &mut A,
        parent: &mut Branch<&mut Block>,
        parent_addr: BlockAddr,
        left: &mut Node<&mut Block, I>,
        left_addr: BlockAddr,
        right: &mut Node<&mut Block, I>,
        right_addr: BlockAddr,
        right_idx: usize,
    ) -> Result<bool>
    where
        for<'a> Node<&'a mut Block, I>: Rotate<Item = I>,
    {
        if let Err(MergeError::Overflows) = left.merge(right) {
            return Ok(false);
        }

        parent.remove_at(right_idx);

        storage.write_at(left.block(), left_addr)?;
        storage.write_at(parent.block(), parent_addr)?;

        allocator.deallocate(right_addr, 1)?;

        Ok(true)
    }
}

enum InsertOutcome {
    Done,
    Split(SplitOutcome),
    LowerBoundChanged(Key),
    SplitAndLowerBoundChanged {
        result: SplitOutcome,
        lower_bound: Key,
    },
}

struct SplitOutcome {
    right_lower_bound: Key,
    right_addr: BlockAddr,
}

enum RemoveOutcome {
    Done(Option<Box<[u8]>>),
    BecameDeficient(Option<Box<[u8]>>),
}

enum DeficientSide {
    Left,
    Right,
}

#[derive(Debug)]
enum InsertError {
    Occupied,
    Overflow,
}

#[derive(Debug)]
// TODO
enum RotateError {
    SiblingBecomesDeficient,
}

#[derive(Debug)]
// TODO
enum MergeError {
    Overflows,
}

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
