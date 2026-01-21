#[cfg(test)]
mod tests;

use std::{
    borrow::{Borrow, BorrowMut},
    fmt::Debug,
    marker::PhantomData,
};

use zerocopy::{
    Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned,
    little_endian::{U16, U64},
};

use crate::{
    block::{BLOCK_SIZE, Block, BlockAddr, BlockAddrStored},
    fs::node::NodeId,
    tree::{Error, InsertError, MergeError, RotateError},
};

#[derive(Debug)]
pub(super) enum NodeVariant<B: Borrow<Block>> {
    Branch(Branch<B>),
    Leaf(Leaf<B>),
}

impl<B> NodeVariant<B>
where
    B: Borrow<Block>,
{
    pub(super) fn try_new(block: B) -> Result<Self, Error> {
        let (header, _) = Header::try_ref_from_prefix(&block.borrow().data)
            .map_err(|_| Error::Uninterpretable)?;
        let node = if header.height.get() == 0 {
            Self::Leaf(Leaf::try_new(block)?)
        } else {
            Self::Branch(Branch::try_new(block)?)
        };
        Ok(node)
    }
}

pub(super) const NODE_CAPACITY: usize = BLOCK_SIZE as usize - HEADER_SIZE;

const OCCUPANCY_THRESH: usize = NODE_CAPACITY / 2;

const fn is_deficient(used_space: usize) -> bool {
    used_space < OCCUPANCY_THRESH
}

/// A handle to the tree's node.
/// Items in a node are guaranteed to be sorted by key.
///
/// # Type parameters
/// - `B` determines whether the handle is mutable or immutable.
/// - `I` determines whether the node is a branch or a leaf node.
pub(super) struct Node<B, I> {
    block: B,
    _item_type: PhantomData<I>,
}

impl<B, I> Node<B, I>
where
    B: Borrow<Block>,
    I: Item,
{
    pub(super) const ITEM_SIZE: usize = size_of::<I>();

    pub(super) fn try_new(block: B) -> Result<Self, Error> {
        let (header, _) = Header::try_ref_from_prefix(&block.borrow().data)
            .map_err(|_| Error::Uninterpretable)?;
        <[I]>::try_ref_from_prefix_with_elems(
            &block.borrow().data[HEADER_SIZE..],
            header.item_count.into(),
        )
        .map_err(|_| Error::Uninterpretable)?;
        Ok(Self {
            block,
            _item_type: PhantomData,
        })
    }

    pub(super) fn block(&self) -> &Block {
        self.block.borrow()
    }

    fn data(&self) -> &[u8; BLOCK_SIZE as usize] {
        &self.block.borrow().data
    }

    fn header(&self) -> &Header {
        let (header, _) = Header::try_ref_from_prefix(&self.data()[..HEADER_SIZE])
            .expect("'self.data' must hold a valid header");
        header
    }

    pub(super) fn height(&self) -> u16 {
        self.header().height.get()
    }

    pub(super) fn item_count(&self) -> u16 {
        self.header().item_count.get()
    }

    fn data_offset(&self) -> u16 {
        self.header().data_offset.get()
    }

    /// # Panics
    /// Panics if there are no items.
    pub(super) fn lower_bound(&self) -> Key {
        self.items()[0].key()
    }

    pub(super) fn is_deficient(&self) -> bool {
        is_deficient(self.used_space())
    }

    fn items(&self) -> &[I] {
        let count: usize = self.header().item_count.into();
        let (items, _) = <[I]>::try_ref_from_prefix_with_elems(&self.data()[HEADER_SIZE..], count)
            .expect("'self.data' must hold a valid item list");
        items
    }

    fn get_item_idx(&self, key: Key) -> Option<usize> {
        self.items()
            .binary_search_by_key(&key, |item| item.key())
            .ok()
    }

    fn get_item_idx_le(&self, key: Key) -> Option<usize> {
        let idx = self.items().partition_point(|item| item.key() <= key);
        if idx == 0 { None } else { Some(idx - 1) }
    }

    fn get_item(&self, key: Key) -> Option<&I> {
        self.get_item_idx(key).map(|idx| &self.items()[idx])
    }

    fn get_item_le(&self, key: Key) -> Option<&I> {
        self.get_item_idx_le(key).map(|idx| &self.items()[idx])
    }

    fn used_space(&self) -> usize {
        let header = self.header();
        let item_count: usize = header.item_count.get().into();
        let items_size = item_count * Self::ITEM_SIZE;
        let data_offset: usize = header.data_offset.get().into();
        let data_size = BLOCK_SIZE as usize - data_offset;
        items_size + data_size
    }

    fn free_space(&self) -> usize {
        NODE_CAPACITY - self.used_space()
    }

    fn can_insert(&self, item_count: usize, data_size: usize) -> bool {
        let required = item_count * Self::ITEM_SIZE + data_size;
        self.free_space() >= required
    }
}

impl<B, I> Node<B, I>
where
    B: Borrow<Block> + BorrowMut<Block>,
    I: Item,
{
    /// Formats the block as an empty node of given height and returns a handle to it.
    pub(super) fn format(mut block: B, height: u16) -> Self {
        let mut header = Header::default();
        header.height.set(height);
        block.borrow_mut().data[..HEADER_SIZE].copy_from_slice(header.as_bytes());
        Self::try_new(block).expect("'block' must be a valid node")
    }

    fn data_mut(&mut self) -> &mut [u8; BLOCK_SIZE as usize] {
        &mut self.block.borrow_mut().data
    }

    fn header_mut(&mut self) -> &mut Header {
        let (header, _) = Header::try_mut_from_prefix(&mut self.data_mut()[..HEADER_SIZE])
            .expect("'self.data' must hold a valid header");
        header
    }

    fn items_mut(&mut self) -> &mut [I] {
        let count = self.header().item_count.into();
        let (items, _) =
            <[I]>::try_mut_from_prefix_with_elems(&mut self.data_mut()[HEADER_SIZE..], count)
                .expect("'self.data' must hold a valid item list");
        items
    }

    fn insert_item(&mut self, item: I) -> Result<(), InsertError> {
        let items = self.items_mut();
        let idx = match items.binary_search_by_key(&item.key(), |item| item.key()) {
            Ok(_) => return Err(InsertError::Occupied),
            Err(idx) => idx,
        };
        let to_shift = items.len() - idx;

        // Shift items
        let start = HEADER_SIZE + idx * Self::ITEM_SIZE;
        let end = start + to_shift * Self::ITEM_SIZE;
        let dest = start + Self::ITEM_SIZE;
        self.data_mut().copy_within(start..end, dest);

        self.data_mut()[start..dest].copy_from_slice(item.as_bytes());

        self.header_mut().item_count += 1;

        Ok(())
    }

    fn insert_items_front(&mut self, items: &[I]) {
        let old_count: usize = self.header().item_count.into();
        let insert_count = items.len();
        let insert_size = insert_count * Self::ITEM_SIZE;

        // Shift items
        let start = HEADER_SIZE;
        let end = start + old_count * Self::ITEM_SIZE;
        let dest = start + insert_size;
        self.data_mut().copy_within(start..end, dest);

        self.data_mut()[start..dest].copy_from_slice(items.as_bytes());

        let new_count: u16 = (old_count + insert_count).try_into().unwrap();
        self.header_mut().item_count.set(new_count);
    }

    fn insert_items_back(&mut self, items: &[I]) {
        let old_count: usize = self.header().item_count.into();
        let insert_count = items.len();
        let insert_size = insert_count * Self::ITEM_SIZE;

        let start = HEADER_SIZE + old_count * Self::ITEM_SIZE;
        let end = start + insert_size;
        self.data_mut()[start..end].copy_from_slice(items.as_bytes());

        let new_count: u16 = (old_count + insert_count).try_into().unwrap();
        self.header_mut().item_count.set(new_count);
    }

    fn remove_item_at(&mut self, idx: usize) -> Option<I> {
        let items = self.items_mut();
        let target = *items.get(idx)?;

        let next = idx + 1;
        let to_shift = items.len() - next;

        items.copy_within(next..(next + to_shift), idx);
        self.header_mut().item_count -= 1;

        Some(target)
    }

    fn take_items_from_right<U>(&mut self, right: &mut Node<U, I>, count: u16)
    where
        U: Borrow<Block> + BorrowMut<Block>,
    {
        let items_to_take = &right.items()[..count.into()];
        self.insert_items_back(items_to_take);

        let old_count = right.item_count();
        let new_count = old_count - count;

        // Shift items
        let taken_size = usize::from(count) * Self::ITEM_SIZE;
        let dest = HEADER_SIZE;
        let start = dest + taken_size;
        let end = start + usize::from(new_count) * Self::ITEM_SIZE;
        right.data_mut().copy_within(start..end, dest);

        right.header_mut().item_count.set(new_count);
    }

    fn take_items_from_left<U>(&mut self, left: &mut Node<U, I>, count: u16)
    where
        U: Borrow<Block> + BorrowMut<Block>,
    {
        let old_count = left.item_count();
        let new_count = old_count - count;

        let items_to_take = &left.items()[new_count.into()..];
        self.insert_items_front(items_to_take);

        left.header_mut().item_count.set(new_count);
    }
}

pub(super) type Branch<B> = Node<B, BranchItem>;

impl<B> Branch<B>
where
    B: Borrow<Block>,
{
    const ITEM_OCCUPANCY_THRESH: u16 = OCCUPANCY_THRESH.div_ceil(Self::ITEM_SIZE) as u16;

    const ITEM_CAPACITY: u16 = (NODE_CAPACITY / Self::ITEM_SIZE) as u16;

    /// Binary searches for the child that covers the given key.
    /// Returns the index of the item containing the child.
    pub(super) fn child_idx_for(&self, key: Key) -> usize {
        self.items()
            .partition_point(|item| item.key() <= key)
            .saturating_sub(1)
    }

    /// Returns the child of the item at index.
    pub(super) fn child_at(&self, idx: usize) -> Option<BlockAddr> {
        self.items().get(idx).map(|item| item.child.into())
    }

    /// Binary searches for the child that covers the given key.
    /// Returns the child.
    pub(super) fn child_for(&self, key: Key) -> BlockAddr {
        self.items()[self.child_idx_for(key)].child.into()
    }
}

impl<B> Branch<B>
where
    B: Borrow<Block> + BorrowMut<Block>,
{
    /// Constructs an item with a child and inserts it.
    pub(super) fn insert(&mut self, key: Key, child: BlockAddr) -> Result<(), InsertError> {
        if !self.can_insert(1, 0) {
            return Err(InsertError::Overflow);
        }
        self.insert_item(BranchItem::new(key, child))
            .expect("must not insert existing child");
        Ok(())
    }

    /// Removes the item at index, returning its child.
    ///
    /// # Panics
    /// Panics if the index is out of bounds.
    pub(super) fn remove_at(&mut self, idx: usize) -> BlockAddr {
        self.remove_item_at(idx)
            .map(|item| item.child.into())
            .expect("must not remove inexisting child")
    }

    /// Sets the key of the item at index.
    ///
    /// # Panics
    /// Panics if the index is out of bounds.
    pub(super) fn set_key_at(&mut self, idx: usize, key: Key) {
        self.items_mut()[idx].key = key
    }
}

pub(super) type Leaf<B> = Node<B, LeafItem>;

impl<B> Leaf<B>
where
    B: Borrow<Block>,
{
    /// Returns a reference to the data associated with the item.
    fn get_for_item(&self, item: &LeafItem) -> &[u8] {
        let start = usize::from(item.offset);
        let end = start + usize::from(item.size);
        &self.data()[start..end]
    }

    /// Returns a reference to the data associated with the item corresponding to the key.
    pub(super) fn get(&self, key: Key) -> Option<&[u8]> {
        self.get_item(key).map(|item| self.get_for_item(item))
    }

    pub(super) fn get_le(&self, key: Key) -> Option<(Key, &[u8])> {
        self.get_item_le(key)
            .map(|item| (item.key, self.get_for_item(item)))
    }
}

impl<B> Leaf<B>
where
    B: Borrow<Block> + BorrowMut<Block>,
{
    /// Constructs an item (an item and data) and inserts it.
    pub(super) fn insert(&mut self, key: Key, data: &[u8]) -> Result<(), InsertError> {
        if !self.can_insert(1, data.len()) {
            return Err(InsertError::Overflow);
        }

        // Construct item
        let size = data.len().try_into().unwrap();
        let offset = self.data_offset() - size;
        let item = LeafItem::new(key, offset, size);

        self.insert_item(item)?;

        // Insert data
        let start = usize::from(offset);
        let end = start + usize::from(size);
        self.data_mut()[start..end].copy_from_slice(data);
        self.header_mut().data_offset.set(offset);

        Ok(())
    }

    /// Constructs an item and inserts it using the given item insertion strategy.
    fn insert_with_strategy<F>(&mut self, key: Key, data: &[u8], strategy: F)
    where
        F: FnOnce(&mut Self, &[LeafItem]),
    {
        // Construct item
        let size = data.len().try_into().unwrap();
        let offset = self.data_offset() - size;
        let item = LeafItem::new(key, offset, size);

        strategy(self, &[item]);

        // Insert data
        let start = usize::from(offset);
        let end = start + usize::from(size);
        self.data_mut()[start..end].copy_from_slice(data);
        self.header_mut().data_offset.set(offset);
    }

    /// Constructs an item and inserts it at the back.
    fn insert_back(&mut self, key: Key, data: &[u8]) {
        self.insert_with_strategy(key, data, |node, items| node.insert_items_back(items));
    }

    /// Constructs an item and inserts it at the front.
    fn insert_front(&mut self, key: Key, data: &[u8]) {
        self.insert_with_strategy(key, data, |node, items| node.insert_items_front(items));
    }

    /// Removes the item at index, returning the data.
    fn remove_at(&mut self, idx: usize) -> Option<Box<[u8]>> {
        let target = self.remove_item_at(idx)?;
        let data = self.get_for_item(&target).to_vec().into_boxed_slice();

        // Calculate span of data that needs to be shifted
        let start = usize::from(self.header().data_offset);
        let end = usize::from(target.offset);
        self.header_mut().data_offset += target.size;

        if start != end {
            // Compact the data area
            let dest = start + usize::from(target.size);
            self.data_mut().copy_within(start..end, dest);
        }

        // Update the items' data offsets
        let items = self.items_mut();
        for item in items {
            if item.offset <= target.offset {
                item.offset += target.size;
            }
        }

        Some(data)
    }

    /// Removes the item corresponding to the key.
    pub(super) fn remove(&mut self, key: Key) -> Option<Box<[u8]>> {
        self.remove_at(self.get_item_idx(key)?)
    }

    /// Returns the number of items that needs to be taken from a sibling to replenish `self`.
    /// If can't replenish `self` without making `sibling` deficient, returns `0`.
    fn rotate_count<'a, U>(
        &self,
        sibling: &Leaf<U>,
        items: impl Iterator<Item = &'a LeafItem>,
    ) -> u16
    where
        U: Borrow<Block>,
    {
        let mut self_used = self.used_space();
        let mut sibling_used = sibling.used_space();

        for (i, item) in items.enumerate() {
            let diff = Self::ITEM_SIZE + usize::from(item.size);

            sibling_used -= diff;
            if is_deficient(sibling_used) {
                break;
            }

            self_used += diff;
            if !is_deficient(self_used) {
                return (i + 1) as u16;
            }
        }

        0
    }

    /// Returns the number of items that needs to be taken from `self` to split it.
    /// If `self` is unsplittable (`self.item_count() < 2`), returns 0.
    fn split_count(&self) -> u16 {
        let mut self_used = self.used_space();
        let mut right_used = 0;

        let mut best_imbalance = self_used;

        for (i, item) in self.items().iter().rev().enumerate() {
            let diff = Self::ITEM_SIZE + usize::from(item.size);

            self_used -= diff;
            right_used += diff;

            let imbalance = self_used.abs_diff(right_used);

            if imbalance >= best_imbalance {
                return i as u16;
            }

            best_imbalance = imbalance;
        }

        0
    }

    fn copy_with_strategy<'a, U, F>(
        &mut self,
        other: &Leaf<U>,
        items: impl Iterator<Item = &'a LeafItem>,
        strategy: F,
    ) where
        U: Borrow<Block>,
        F: Fn(&mut Self, Key, &[u8]),
    {
        items.for_each(|item| {
            let data = other.get_for_item(item);
            strategy(self, item.key, data)
        });
    }

    /// Moves the last `count` items of `left` into `self`.
    fn take_from_left<U>(&mut self, left: &mut Leaf<U>, count: u16)
    where
        U: Borrow<Block> + BorrowMut<Block>,
    {
        let left_new_count = left.item_count() - count;
        let move_items = &left.items()[left_new_count.into()..];

        self.copy_with_strategy(left, move_items.iter().rev(), |node, key, data| {
            node.insert_front(key, data)
        });

        for _ in 0..count {
            left.remove_at(left_new_count.into())
                .expect("item must exist");
        }
    }

    /// Moves the first `count` items of `right` into `self`.
    fn take_from_right<U>(&mut self, right: &mut Leaf<U>, count: u16)
    where
        U: Borrow<Block> + BorrowMut<Block>,
    {
        let move_items = &right.items()[..count.into()];

        self.copy_with_strategy(right, move_items.iter(), |node, key, data| {
            node.insert_back(key, data)
        });

        for _ in 0..count {
            right.remove_at(0).expect("item must exist");
        }
    }
}

const HEADER_SIZE: usize = size_of::<Header>();

/// A header stored at the beginning of a node.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
struct Header {
    // The distance from this node to a leaf node
    height: U16,
    item_count: U16,
    // The absolute offset of the data area in a leaf node
    data_offset: U16,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            height: Default::default(),
            item_count: Default::default(),
            data_offset: U16::new(BLOCK_SIZE as u16),
        }
    }
}

/// A unique identifier of an item in the tree.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable, Unaligned)]
pub struct Key {
    // The node this item is associated with
    pub id: NodeId,
    pub datatype: DataType,
    // Additional information that depends on the data type
    pub offset: U64,
}

impl Key {
    pub fn new(id: NodeId, datatype: DataType, offset: u64) -> Self {
        Self {
            id,
            datatype,
            offset: offset.into(),
        }
    }

    pub fn node(id: NodeId) -> Self {
        Self {
            id,
            datatype: DataType::Node,
            offset: 0.into(),
        }
    }

    pub fn direntry(id: NodeId, hash: u64) -> Self {
        Self {
            id,
            datatype: DataType::DirEntry,
            offset: hash.into(),
        }
    }

    pub fn extent(id: NodeId, offset: u64) -> Self {
        Self {
            id,
            datatype: DataType::Extent,
            offset: offset.into(),
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset.get()
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id
            .cmp(&other.id)
            .then(self.datatype.cmp(&other.datatype))
            .then(self.offset.cmp(&other.offset))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.datatype == other.datatype && self.offset == other.offset
    }
}

impl Eq for Key {}

/// The type of data associated with an item stored in a leaf node.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[derive(TryFromBytes, IntoBytes, Immutable, Unaligned)]
pub enum DataType {
    // A filesystem object
    Node,
    // A contiguous range of blocks that belongs to a node
    Extent,
    // A mapping of a name to a node
    DirEntry,
}

pub(super) trait Item:
    Debug + Clone + Copy + TryFromBytes + IntoBytes + Immutable + Unaligned
{
    fn key(&self) -> Key;
}

/// An item stored in a branch node.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable, Unaligned)]
pub(super) struct BranchItem {
    key: Key,
    // The address of the child node
    child: BlockAddrStored,
}

impl BranchItem {
    fn new(key: Key, child: BlockAddr) -> Self {
        Self {
            key,
            child: child.into(),
        }
    }
}

impl Item for BranchItem {
    fn key(&self) -> Key {
        self.key
    }
}

/// An item stored in a leaf node.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable, Unaligned)]
pub(super) struct LeafItem {
    key: Key,
    // The absolute offset of the item's data
    offset: U16,
    // The size of the item's data
    size: U16,
}

impl LeafItem {
    fn new(key: Key, offset: u16, size: u16) -> Self {
        Self {
            key,
            offset: U16::new(offset),
            size: U16::new(size),
        }
    }
}

impl Item for LeafItem {
    fn key(&self) -> Key {
        self.key
    }
}

pub(super) trait Rotate {
    type Item;

    /// Replenishes `self` by taking some items from `right`.
    fn rotate_left<U>(&mut self, right: &mut Node<U, Self::Item>) -> Result<(), RotateError>
    where
        U: Borrow<Block> + BorrowMut<Block>;

    /// Replenishes `self` by taking some items from `left`.
    fn rotate_right<U>(&mut self, left: &mut Node<U, Self::Item>) -> Result<(), RotateError>
    where
        U: Borrow<Block> + BorrowMut<Block>;

    /// Copies `right`'s items into `self`.
    fn merge<U>(&mut self, right: &Node<U, Self::Item>) -> Result<(), MergeError>
    where
        U: Borrow<Block>;
}

impl<B> Rotate for Branch<B>
where
    B: Borrow<Block> + BorrowMut<Block>,
{
    type Item = BranchItem;

    fn rotate_left<U>(&mut self, right: &mut Node<U, Self::Item>) -> Result<(), RotateError>
    where
        U: Borrow<Block> + BorrowMut<Block>,
    {
        debug_assert!(self.is_deficient(), "'self' must be deficient");

        let req_count = Self::ITEM_OCCUPANCY_THRESH - self.item_count();
        let right_count = right.item_count();

        if (right_count.saturating_sub(req_count)) < Self::ITEM_OCCUPANCY_THRESH {
            return Err(RotateError::SiblingBecomesDeficient);
        }

        self.take_items_from_right(right, req_count);
        Ok(())
    }

    fn rotate_right<U>(&mut self, left: &mut Node<U, Self::Item>) -> Result<(), RotateError>
    where
        U: Borrow<Block> + BorrowMut<Block>,
    {
        debug_assert!(self.is_deficient(), "'self' must be deficient");

        let req_count = Self::ITEM_OCCUPANCY_THRESH - self.item_count();
        let left_count = left.item_count();

        if (left_count.saturating_sub(req_count)) < Self::ITEM_OCCUPANCY_THRESH {
            return Err(RotateError::SiblingBecomesDeficient);
        }

        self.take_items_from_left(left, req_count);
        Ok(())
    }

    fn merge<U>(&mut self, right: &Node<U, Self::Item>) -> Result<(), MergeError>
    where
        U: Borrow<Block>,
    {
        debug_assert!(
            self.is_deficient() || right.is_deficient(),
            "one of the branches must be deficient"
        );

        debug_assert!(
            (self.item_count() + right.item_count()) <= Self::ITEM_CAPACITY,
            "items of both branches must fit in a single branch"
        );

        self.insert_items_back(right.items());
        Ok(())
    }
}

impl<B> Rotate for Leaf<B>
where
    B: Borrow<Block> + BorrowMut<Block>,
{
    type Item = LeafItem;

    fn rotate_left<U>(&mut self, right: &mut Node<U, Self::Item>) -> Result<(), RotateError>
    where
        U: Borrow<Block> + BorrowMut<Block>,
    {
        debug_assert!(self.is_deficient(), "'self' must be deficient");

        let count = self.rotate_count(right, right.items().iter());
        if count == 0 {
            return Err(RotateError::SiblingBecomesDeficient);
        }

        self.take_from_right(right, count);
        Ok(())
    }

    fn rotate_right<U>(&mut self, left: &mut Node<U, Self::Item>) -> Result<(), RotateError>
    where
        U: Borrow<Block> + BorrowMut<Block>,
    {
        debug_assert!(self.is_deficient(), "'self' must be deficient");

        let count = self.rotate_count(left, left.items().iter().rev());
        if count == 0 {
            return Err(RotateError::SiblingBecomesDeficient);
        }

        self.take_from_left(left, count);
        Ok(())
    }

    fn merge<U>(&mut self, right: &Node<U, Self::Item>) -> Result<(), MergeError>
    where
        U: Borrow<Block>,
    {
        debug_assert!(
            self.is_deficient() || right.is_deficient(),
            "one of the leafs must be deficient"
        );

        let right_items = right.items();
        let right_data_size = BLOCK_SIZE as usize - usize::from(right.data_offset());

        if !self.can_insert(right_items.len(), right_data_size) {
            return Err(MergeError::Overflows);
        }

        self.copy_with_strategy(right, right_items.iter(), |node, key, data| {
            node.insert_back(key, data)
        });

        Ok(())
    }
}

pub(super) trait Split {
    type Item;

    /// Moves the second half of items from `self` into `right`.
    fn split<U>(&mut self, right: &mut Node<U, Self::Item>)
    where
        U: Borrow<Block> + BorrowMut<Block>;
}

impl<B> Split for Branch<B>
where
    B: Borrow<Block> + BorrowMut<Block>,
{
    type Item = BranchItem;

    fn split<U>(&mut self, right: &mut Node<U, Self::Item>)
    where
        U: Borrow<Block> + BorrowMut<Block>,
    {
        debug_assert!(
            self.item_count() > 1,
            "branch with less than two items mustn't exist"
        );
        debug_assert_eq!(right.item_count(), 0, "'right' must be empty");

        let item_count = self.item_count();
        let count = item_count / 2;

        right.take_items_from_left(self, count);
    }
}

impl<B> Split for Leaf<B>
where
    B: Borrow<Block> + BorrowMut<Block>,
{
    type Item = LeafItem;

    fn split<U>(&mut self, right: &mut Node<U, Self::Item>)
    where
        U: Borrow<Block> + BorrowMut<Block>,
    {
        debug_assert!(self.item_count() >= 2, "'self' must have at least 2 items");
        debug_assert_eq!(right.item_count(), 0, "'right' must be empty");
        let count = self.split_count();

        right.take_from_left(self, count);
    }
}

impl<B, I> Debug for Node<B, I>
where
    B: Borrow<Block>,
    I: Item,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let height = self.height();
        let name = if height == 0 { "Leaf" } else { "Branch" };
        f.debug_struct(name)
            .field("height", &height)
            .field("items", &self.items())
            .finish()
    }
}
