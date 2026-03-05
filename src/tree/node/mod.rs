#[cfg(test)]
mod tests;

use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use zerocopy::{
    Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned,
    little_endian::{U16, U64},
};

use crate::{
    block::{BLOCK_SIZE, Block, BlockAddr, BlockAddrStored},
    fs::node::NodeId,
    tree::{Error, InsertError, Overflows, SiblingBecomesDeficient},
};

/// A kind of a tree node: either a branch or a leaf.
pub enum NodeKind<B> {
    /// A branch node routes to data.
    Branch(NodeView<B, BranchItem>),
    /// A leaf node contains the data.
    Leaf(NodeView<B, LeafItem>),
}

impl<B: Deref<Target = Block>> NodeKind<B> {
    pub fn try_new(block: B) -> Result<Self, Error> {
        let (header, _) =
            Header::try_ref_from_prefix(&block.data).map_err(|_| Error::Uninterpretable)?;

        Ok(if header.height == 0 {
            Self::Leaf(NodeView {
                block,
                _item_type: PhantomData,
            })
        } else {
            Self::Branch(NodeView {
                block,
                _item_type: PhantomData,
            })
        })
    }
}

/// A view into a block as a tree node.
pub struct NodeView<B, I> {
    block: B,
    _item_type: PhantomData<I>,
}

impl<B: Deref<Target = Block>, I> Deref for NodeView<B, I> {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}
impl<B: DerefMut<Target = Block>, I> DerefMut for NodeView<B, I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.block
    }
}

impl<B: Deref<Target = Block>> Node<BranchItem> for NodeView<B, BranchItem> {}
impl<B: DerefMut<Target = Block>> NodeMut<BranchItem> for NodeView<B, BranchItem> {}
impl<B: Deref<Target = Block>> Branch for NodeView<B, BranchItem> {}
impl<B: DerefMut<Target = Block>> BranchMut for NodeView<B, BranchItem> {}

impl<B: Deref<Target = Block>> Node<LeafItem> for NodeView<B, LeafItem> {}
impl<B: DerefMut<Target = Block>> NodeMut<LeafItem> for NodeView<B, LeafItem> {}
impl<B: Deref<Target = Block>> Leaf for NodeView<B, LeafItem> {}
impl<B: DerefMut<Target = Block>> LeafMut for NodeView<B, LeafItem> {}

/// A handle to a tree node.
/// Items in a node are guaranteed to be sorted by key.
pub trait Node<I: Item>: Deref<Target = Block> {
    fn header(&self) -> &Header {
        let (header, _) = Header::try_ref_from_prefix(&self.data[..Header::SIZE])
            .expect("'self.data' must hold a valid header");
        header
    }

    fn height(&self) -> u16 {
        self.header().height.get()
    }

    fn item_count(&self) -> u16 {
        self.header().item_count.get()
    }

    fn data_offset(&self) -> u16 {
        self.header().data_offset.get()
    }

    fn items(&self) -> &[I] {
        let count: usize = self.header().item_count.into();
        let (items, _) = <[I]>::try_ref_from_prefix_with_elems(&self.data[Header::SIZE..], count)
            .expect("'self.data' must hold a valid item list");
        items
    }

    fn lower_bound(&self) -> Key {
        self.items()[0].key()
    }

    fn get_item_index(&self, key: Key) -> Option<usize> {
        self.items()
            .binary_search_by_key(&key, |item| item.key())
            .ok()
    }

    fn get_item_index_le(&self, key: Key) -> Option<usize> {
        let idx = self.items().partition_point(|item| item.key() <= key);
        if idx == 0 { None } else { Some(idx - 1) }
    }

    fn get_item(&self, key: Key) -> Option<&I> {
        self.get_item_index(key).map(|idx| &self.items()[idx])
    }

    fn get_item_le(&self, key: Key) -> Option<&I> {
        self.get_item_index_le(key).map(|idx| &self.items()[idx])
    }

    fn used_space(&self) -> usize {
        let items_size = usize::from(self.item_count()) * I::SIZE;
        let data_size = BLOCK_SIZE as usize - usize::from(self.data_offset());
        items_size + data_size
    }

    const CAPACITY: usize = BLOCK_SIZE as usize - Header::SIZE;

    fn free_space(&self) -> usize {
        Self::CAPACITY - self.used_space()
    }

    const OCCUPANCY_THRESH: usize = Self::CAPACITY / 2;

    fn is_below_occupancy_threshold(used_space: usize) -> bool {
        used_space < Self::OCCUPANCY_THRESH
    }

    fn is_deficient(&self) -> bool {
        Self::is_below_occupancy_threshold(self.used_space())
    }
}

/// A mutable handle to a tree node.
pub trait NodeMut<I: Item>: Node<I> + DerefMut<Target = Block> {
    /// Formats the block as an empty node of given height.
    fn format_as_node(&mut self, height: u16) {
        let mut header = Header::default();
        header.height.set(height);
        self.data[..Header::SIZE].copy_from_slice(header.as_bytes());
    }

    fn header_mut(&mut self) -> &mut Header {
        let (header, _) = Header::try_mut_from_prefix(&mut self.data[..Header::SIZE])
            .expect("'self.data' must hold a valid header");
        header
    }

    fn set_height(&mut self, value: u16) {
        self.header_mut().height.set(value)
    }

    fn set_item_count(&mut self, value: u16) {
        self.header_mut().item_count.set(value)
    }

    fn set_data_offset(&mut self, value: u16) {
        self.header_mut().data_offset.set(value)
    }

    fn items_mut(&mut self) -> &mut [I] {
        let count = self.header().item_count.into();
        let (items, _) =
            <[I]>::try_mut_from_prefix_with_elems(&mut self.data[Header::SIZE..], count)
                .expect("'self.data' must hold a valid item list");
        items
    }

    fn insert_items_at(&mut self, index: usize, items: &[I]) {
        let count = items.len();
        let old_count: usize = self.header().item_count.into();
        let delta = count * I::SIZE;

        // Shift items
        let start = Header::SIZE + index * I::SIZE;
        let end = start + (old_count - index) * I::SIZE;
        let dest = start + delta;
        self.data.copy_within(start..end, dest);

        self.data[start..dest].copy_from_slice(items.as_bytes());

        let new_count: u16 = (old_count + count).try_into().unwrap();
        self.set_item_count(new_count);
    }

    fn insert_items_front(&mut self, items: &[I]) {
        self.insert_items_at(0, items);
    }

    fn insert_items_back(&mut self, items: &[I]) {
        let index = self.header().item_count.into();
        self.insert_items_at(index, items);
    }

    fn insert_item(&mut self, item: I) -> Result<(), InsertError> {
        let index = match self
            .items()
            .binary_search_by_key(&item.key(), |item| item.key())
        {
            Ok(_) => return Err(InsertError::Occupied),
            Err(index) => index,
        };
        self.insert_items_at(index, &[item]);
        Ok(())
    }

    fn remove_items_at(&mut self, index: usize, count: usize) {
        let old_count: usize = self.header().item_count.into();
        let new_count = old_count - count;
        let delta: usize = count * I::SIZE;

        // Shift items
        let dest = Header::SIZE + index * I::SIZE;
        let start = dest + delta;
        let end = start + (new_count - index) * I::SIZE;
        self.data.copy_within(start..end, dest);

        let new_count: u16 = new_count.try_into().unwrap();
        self.set_item_count(new_count);
    }

    fn remove_item_at(&mut self, index: usize) -> Option<I> {
        let target = *self.items().get(index)?;
        self.remove_items_at(index, 1);
        Some(target)
    }

    fn take_items_from_right(&mut self, right: &mut impl NodeMut<I>, count: usize) {
        let items_take = &right.items()[..count.into()];
        self.insert_items_back(items_take);
        right.remove_items_at(0, count);
    }

    fn take_items_from_left(&mut self, left: &mut impl NodeMut<I>, count: usize) {
        let index = usize::from(left.header().item_count) - count;
        let items_take = &left.items()[index..];
        self.insert_items_front(items_take);
        left.remove_items_at(index, count);
    }
}

/// A handle to a branch node.
pub trait Branch: Node<BranchItem> {
    const ITEM_OCCUPANCY_THRESH: u16 = Self::OCCUPANCY_THRESH.div_ceil(BranchItem::SIZE) as u16;

    const ITEM_CAPACITY: u16 = (Self::CAPACITY / BranchItem::SIZE) as u16;

    /// Binary searches for the child that covers the given key.
    /// Returns the index of the item containing the child.
    fn child_idx_for(&self, key: Key) -> usize {
        self.items()
            .partition_point(|item| item.key() <= key)
            .saturating_sub(1)
    }

    /// Returns the child of the item at index.
    fn child_at(&self, idx: usize) -> Option<BlockAddr> {
        self.items().get(idx).map(|item| item.child.into())
    }

    /// Binary searches for the child that covers the given key.
    /// Returns the child.
    fn child_for(&self, key: Key) -> BlockAddr {
        self.items()[self.child_idx_for(key)].child.into()
    }

    fn can_insert(&self, item_count: usize) -> bool {
        let required = item_count * BranchItem::SIZE;
        self.free_space() >= required
    }
}

/// A mutable handle to a branch node.
pub trait BranchMut: Branch + NodeMut<BranchItem> {
    /// Constructs an item and inserts it.
    fn insert(&mut self, key: Key, child: BlockAddr) -> Result<(), InsertError> {
        if !self.can_insert(1) {
            return Err(InsertError::Overflow);
        }
        self.insert_item(BranchItem::new(key, child))
            .expect("must not insert existing child");
        Ok(())
    }

    /// Removes the item at index, returning the child.
    ///
    /// # Panics
    /// Panics if the index is out of bounds.
    fn remove_at(&mut self, idx: usize) -> BlockAddr {
        self.remove_item_at(idx)
            .map(|item| item.child.into())
            .expect("must not remove inexisting child")
    }

    /// Sets the key of the item at index.
    ///
    /// # Panics
    /// Panics if the index is out of bounds.
    fn set_key_at(&mut self, index: usize, key: Key) {
        self.items_mut()[index].key = key
    }
}

/// A handle to a leaf node.
pub trait Leaf: Node<LeafItem> {
    /// Returns a reference to the data associated with the item.
    fn get_for_item(&self, item: &LeafItem) -> &[u8] {
        let start = usize::from(item.offset);
        let end = start + usize::from(item.size);
        &self.data[start..end]
    }

    /// Returns a reference to the data associated with the item corresponding to the key.
    fn get(&self, key: Key) -> Option<&[u8]> {
        self.get_item(key).map(|item| self.get_for_item(item))
    }

    fn get_le(&self, key: Key) -> Option<(Key, &[u8])> {
        self.get_item_le(key)
            .map(|item| (item.key, self.get_for_item(item)))
    }

    fn can_insert(&self, item_count: usize, data_size: usize) -> bool {
        let required = item_count * LeafItem::SIZE + data_size;
        self.free_space() >= required
    }
}

/// A mutable handle to a leaf node.
pub trait LeafMut: Leaf + NodeMut<LeafItem> {
    /// Constructs an item and inserts it and its data.
    fn insert(&mut self, key: Key, data: &[u8]) -> Result<(), InsertError> {
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
        self.data[start..end].copy_from_slice(data);
        self.set_data_offset(offset);

        Ok(())
    }

    /// Constructs an item and inserts it and its data using the given item insertion strategy.
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
        self.data[start..end].copy_from_slice(data);
        self.set_data_offset(offset);
    }

    /// Constructs an item and inserts it at the front.
    fn insert_front(&mut self, key: Key, data: &[u8]) {
        self.insert_with_strategy(key, data, |node, items| node.insert_items_front(items));
    }

    /// Constructs an item and inserts it at the back.
    fn insert_back(&mut self, key: Key, data: &[u8]) {
        self.insert_with_strategy(key, data, |node, items| node.insert_items_back(items));
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
            self.data.copy_within(start..end, dest);
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
    fn remove(&mut self, key: Key) -> Option<Box<[u8]>> {
        self.remove_at(self.get_item_index(key)?)
    }

    /// Returns the number of items that needs to be taken from a sibling to replenish `self`.
    /// If can't replenish `self` without making `sibling` deficient, returns `0`.
    fn rotate_count<'a>(
        &self,
        sibling: &impl Leaf,
        items: impl Iterator<Item = &'a LeafItem>,
    ) -> u16 {
        let mut self_used = self.used_space();
        let mut sibling_used = sibling.used_space();

        for (i, item) in items.enumerate() {
            let diff = LeafItem::SIZE + usize::from(item.size);

            sibling_used -= diff;
            if Self::is_below_occupancy_threshold(sibling_used) {
                break;
            }

            self_used += diff;
            if !Self::is_below_occupancy_threshold(self_used) {
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
            let diff = LeafItem::SIZE + usize::from(item.size);

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

    fn copy_with_strategy<'a, F>(
        &mut self,
        other: &impl Leaf,
        items: impl Iterator<Item = &'a LeafItem>,
        strategy: F,
    ) where
        F: Fn(&mut Self, Key, &[u8]),
    {
        items.for_each(|item| {
            let data = other.get_for_item(item);
            strategy(self, item.key, data)
        });
    }

    /// Moves the last `count` items of `left` into `self`.
    fn take_from_left(&mut self, left: &mut impl LeafMut, count: u16) {
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
    fn take_from_right(&mut self, right: &mut impl LeafMut, count: u16) {
        let move_items = &right.items()[..count.into()];

        self.copy_with_strategy(right, move_items.iter(), |node, key, data| {
            node.insert_back(key, data)
        });

        for _ in 0..count {
            right.remove_at(0).expect("item must exist");
        }
    }
}

pub trait Rotate<I: Item>: NodeMut<I> {
    /// Replenishes `self` by taking some items from `right`.
    fn rotate_left(&mut self, right: &mut Self) -> Result<(), SiblingBecomesDeficient>;

    /// Replenishes `self` by taking some items from `left`.
    fn rotate_right(&mut self, left: &mut Self) -> Result<(), SiblingBecomesDeficient>;

    /// Copies `right`'s items into `self`.
    fn merge(&mut self, right: &Self) -> Result<(), Overflows>;
}

impl<T> Rotate<BranchItem> for T
where
    T: BranchMut,
{
    fn rotate_left(&mut self, right: &mut Self) -> Result<(), SiblingBecomesDeficient> {
        let req_count = Self::ITEM_OCCUPANCY_THRESH - self.item_count();
        let right_count = right.item_count();

        if (right_count.saturating_sub(req_count)) < Self::ITEM_OCCUPANCY_THRESH {
            return Err(SiblingBecomesDeficient);
        }

        self.take_items_from_right(right, req_count.into());
        Ok(())
    }

    fn rotate_right(&mut self, left: &mut Self) -> Result<(), SiblingBecomesDeficient> {
        let req_count = Self::ITEM_OCCUPANCY_THRESH - self.item_count();
        let left_count = left.item_count();

        if (left_count.saturating_sub(req_count)) < Self::ITEM_OCCUPANCY_THRESH {
            return Err(SiblingBecomesDeficient);
        }

        self.take_items_from_left(left, req_count.into());
        Ok(())
    }

    fn merge(&mut self, right: &Self) -> Result<(), Overflows> {
        self.insert_items_back(right.items());
        Ok(())
    }
}

impl<T> Rotate<LeafItem> for T
where
    T: LeafMut,
{
    fn rotate_left(&mut self, right: &mut Self) -> Result<(), SiblingBecomesDeficient> {
        let count = self.rotate_count(right, right.items().iter());
        if count == 0 {
            return Err(SiblingBecomesDeficient);
        }

        self.take_from_right(right, count);
        Ok(())
    }

    fn rotate_right(&mut self, left: &mut Self) -> Result<(), SiblingBecomesDeficient> {
        let count = self.rotate_count(left, left.items().iter().rev());
        if count == 0 {
            return Err(SiblingBecomesDeficient);
        }

        self.take_from_left(left, count);
        Ok(())
    }

    fn merge(&mut self, right: &Self) -> Result<(), Overflows> {
        let right_items = right.items();
        let right_data_size = BLOCK_SIZE as usize - usize::from(right.data_offset());

        if !self.can_insert(right_items.len(), right_data_size) {
            return Err(Overflows);
        }

        self.copy_with_strategy(right, right_items.iter(), |node, key, data| {
            node.insert_back(key, data)
        });

        Ok(())
    }
}

pub trait Split<I: Item>: NodeMut<I> {
    /// Moves the second half of items from `self` into `right`.
    fn split(&mut self, right: &mut Self);
}

impl<T> Split<BranchItem> for T
where
    T: BranchMut,
{
    fn split(&mut self, right: &mut Self) {
        let count = usize::from(self.item_count() / 2);
        right.take_items_from_left(self, count);
    }
}

impl<T> Split<LeafItem> for T
where
    T: LeafMut,
{
    fn split(&mut self, right: &mut Self) {
        let count = self.split_count();
        right.take_from_left(self, count);
    }
}

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

impl Header {
    const SIZE: usize = size_of::<Self>();
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
    // The id of the node this item is associated with
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

pub trait Item: Debug + Clone + Copy + TryFromBytes + IntoBytes + Immutable + Unaligned {
    const SIZE: usize = size_of::<Self>();

    fn key(&self) -> Key;
}

/// An item stored in a branch node.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable, Unaligned)]
pub struct BranchItem {
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
pub struct LeafItem {
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
