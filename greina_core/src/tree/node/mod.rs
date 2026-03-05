#[cfg(test)]
mod tests;

use core::{fmt::Debug, marker::PhantomData, ptr};

use zerocopy::{
    Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned,
    little_endian::{U16, U64},
};

use crate::{
    block::{BLOCK_SIZE, Block, BlockAddr, BlockAddrStored},
    fs::node::NodeId,
    tree::{Error, InsertError, Overflows, SiblingBecomesUnderfull},
};

/// A tree node kind: either a branch or a leaf.
pub enum NodeKind<'a> {
    /// A branch node routes to data.
    Branch(&'a Node<BranchItem>),
    /// A leaf node contains the data.
    Leaf(&'a Node<LeafItem>),
}

/// A mutable tree node kind: either a branch or a leaf.
pub enum NodeMutKind<'a> {
    /// A branch node routes to data.
    Branch(&'a mut Node<BranchItem>),
    /// A leaf node contains the data.
    Leaf(&'a mut Node<LeafItem>),
}

impl<'a> TryFrom<&'a Block> for NodeKind<'a> {
    type Error = Error;

    fn try_from(block: &'a Block) -> Result<Self, Self::Error> {
        let (header, _) =
            Header::try_ref_from_prefix(&block[..]).map_err(|_| Error::Uninterpretable)?;

        Ok(if header.height.get() == 0 {
            // SAFETY:
            let node = unsafe {
                ptr::from_ref(block)
                    .cast::<Node<LeafItem>>()
                    .as_ref_unchecked()
            };
            Self::Leaf(node)
        } else {
            // SAFETY:
            let node = unsafe {
                ptr::from_ref(block)
                    .cast::<Node<BranchItem>>()
                    .as_ref_unchecked()
            };
            Self::Branch(node)
        })
    }
}

impl<'a> TryFrom<&'a mut Block> for NodeMutKind<'a> {
    type Error = Error;

    fn try_from(block: &'a mut Block) -> Result<Self, Self::Error> {
        let (header, _) =
            Header::try_ref_from_prefix(&block[..]).map_err(|_| Error::Uninterpretable)?;

        Ok(if header.height.get() == 0 {
            // SAFETY:
            let node = unsafe {
                ptr::from_mut(block)
                    .cast::<Node<LeafItem>>()
                    .as_mut_unchecked()
            };
            Self::Leaf(node)
        } else {
            // SAFETY:
            let node = unsafe {
                ptr::from_mut(block)
                    .cast::<Node<BranchItem>>()
                    .as_mut_unchecked()
            };
            Self::Branch(node)
        })
    }
}

/// A view into a block as a tree node.
#[repr(transparent)]
pub struct Node<I> {
    block: Block,
    _marker: PhantomData<I>,
}

impl<I: Item> Node<I> {
    fn header(&self) -> &Header {
        let (header, _) = Header::try_ref_from_prefix(&self.block[..Header::SIZE])
            .expect("`self.block` must hold a valid header");
        header
    }

    fn header_mut(&mut self) -> &mut Header {
        let (header, _) = Header::try_mut_from_prefix(&mut self.block[..Header::SIZE])
            .expect("`self.block` must hold a valid header");
        header
    }

    fn items(&self) -> &[I] {
        let item_count: usize = self.header().item_count.into();
        let (items, _) =
            <[I]>::try_ref_from_prefix_with_elems(&self.block[Header::SIZE..], item_count)
                .expect("`self.block` must hold a valid item list");
        items
    }

    fn items_mut(&mut self) -> &mut [I] {
        let item_count = self.header().item_count.into();
        let (items, _) =
            <[I]>::try_mut_from_prefix_with_elems(&mut self.block[Header::SIZE..], item_count)
                .expect("`self.block` must hold a valid item list");
        items
    }

    fn get_item(&self, key: Key) -> Option<&I> {
        let index = self.get_item_index(key).ok()?;
        Some(&self.items()[index])
    }

    fn get_item_mut(&mut self, key: Key) -> Option<&mut I> {
        let index = self.get_item_index(key).ok()?;
        Some(&mut self.items_mut()[index])
    }

    /// Binary searches for the index of the item corresponding to the key.
    ///
    /// If the item is found, [Result::Ok] is returned, containing the index of the item.
    /// If the item is not found, [Result::Err] is returned, containing the index where an item
    /// could be inserted.
    fn get_item_index(&self, key: Key) -> Result<usize, usize> {
        self.items().binary_search_by_key(&key, |item| item.key())
    }

    fn insert_items_at(&mut self, index: usize, items: &[I]) {
        let count = items.len();
        let old_count: usize = self.header().item_count.into();
        let delta = count * I::SIZE;

        // Shift items
        let start = Header::SIZE + index * I::SIZE;
        let end = start + (old_count - index) * I::SIZE;
        let dest = start + delta;
        self.block.copy_within(start..end, dest);

        self.block[start..dest].copy_from_slice(items.as_bytes());

        let new_count: u16 = (old_count + count).try_into().unwrap();
        self.header_mut().item_count.set(new_count);
    }

    fn insert_items_front(&mut self, items: &[I]) {
        self.insert_items_at(0, items);
    }

    fn insert_items_back(&mut self, items: &[I]) {
        let index = self.header().item_count.into();
        self.insert_items_at(index, items);
    }

    fn remove_items_at(&mut self, index: usize, count: usize) {
        let old_count: usize = self.header().item_count.into();
        let new_count = old_count - count;
        let delta: usize = count * I::SIZE;

        // Shift items
        let dest = Header::SIZE + index * I::SIZE;
        let start = dest + delta;
        let end = start + (new_count - index) * I::SIZE;
        self.block.copy_within(start..end, dest);

        let new_count: u16 = new_count.try_into().unwrap();
        self.header_mut().item_count.set(new_count);
    }

    fn take_items_from_right(&mut self, right: &mut Self, count: usize) {
        let items_take = &right.items()[..count.into()];
        self.insert_items_back(items_take);
        right.remove_items_at(0, count);
    }

    fn take_items_from_left(&mut self, left: &mut Self, count: usize) {
        let index = usize::from(left.header().item_count) - count;
        let items_take = &left.items()[index..];
        self.insert_items_front(items_take);
        left.remove_items_at(index, count);
    }
}

impl Node<BranchItem> {
    pub fn get_child(&self, key: Key) -> Option<(usize, BlockAddr)> {
        let index = self.get_child_index(key)?;
        let child = self.items()[index].child.into();
        Some((index, child))
    }

    fn get_child_index(&self, key: Key) -> Option<usize> {
        let index = self.items().partition_point(|item| item.key() <= key);
        if index == 0 { None } else { Some(index - 1) }
    }

    pub fn get_child_at(&self, index: usize) -> Option<BlockAddr> {
        let item = self.items().get(index)?;
        let child = item.child.into();
        Some(child)
    }

    pub fn insert_child(&mut self, key: Key, child: BlockAddr) -> Result<(), InsertError> {
        if !self.can_insert(1) {
            return Err(InsertError::Overfull);
        }

        let index = self
            .get_item_index(key)
            .expect_err("must not insert existing child");

        let item = BranchItem::new(key, child);
        self.insert_items_at(index, &[item]);

        Ok(())
    }

    pub fn remove_child_at(&mut self, index: usize) -> BlockAddr {
        let child = self.items()[index].child.into();
        self.remove_items_at(index, 1);
        child
    }

    /// How many items a branch can fit.
    const CAPACITY: usize = (BLOCK_SIZE as usize - Header::SIZE) / BranchItem::SIZE;

    /// Returns the remaining capacity of the branch.
    fn available(&self) -> usize {
        Self::CAPACITY - usize::from(self.header().item_count)
    }

    fn can_insert(&self, item_count: usize) -> bool {
        self.available() >= item_count
    }

    const MIN_OCCUPANCY: usize = Self::CAPACITY / 2;

    fn is_underfull(&self) -> bool {
        usize::from(self.header().item_count) < Self::MIN_OCCUPANCY
    }
}

impl Node<LeafItem> {
    pub fn get_data(&self, key: Key) -> Option<&[u8]> {
        let item = self.get_item(key)?;
        let data = self.get_data_for_item(item);
        Some(data)
    }

    fn get_data_for_item(&self, item: &LeafItem) -> &[u8] {
        let start = usize::from(item.offset);
        let end = start + usize::from(item.size);
        &self.block[start..end]
    }

    pub fn insert_entry(&mut self, key: Key, data: &[u8]) -> Result<(), InsertError> {
        if !self.can_insert(1, data.len()) {
            return Err(InsertError::Overfull);
        }

        let index = if let Err(index) = self.get_item_index(key) {
            index
        } else {
            return Err(InsertError::Occupied);
        };

        self.insert_entry_at(index, key, data);

        Ok(())
    }

    fn insert_entry_at(&mut self, index: usize, key: Key, data: &[u8]) {
        // Construct item
        let size = data.len().try_into().unwrap();
        let offset = self.header().data_offset.get() - size;
        let item = LeafItem::new(key, offset, size);

        self.insert_items_at(index, &[item]);

        // Insert data
        let start = usize::from(offset);
        let end = start + usize::from(size);
        self.block[start..end].copy_from_slice(data);
        self.header_mut().data_offset.set(offset);
    }

    fn insert_entry_front(&mut self, key: Key, data: &[u8]) {
        self.insert_entry_at(0, key, data);
    }

    fn insert_entry_back(&mut self, key: Key, data: &[u8]) {
        let index = self.header().item_count.into();
        self.insert_entry_at(index, key, data);
    }

    pub fn remove_entry(&mut self, key: Key) -> bool {
        if let Ok(index) = self.get_item_index(key) {
            self.remove_entry_at(index);
            true
        } else {
            false
        }
    }

    fn remove_entry_at(&mut self, index: usize) {
        let target = self.items()[index];

        // Calculate span of data that needs to be shifted
        let start = usize::from(self.header().data_offset);
        let end = usize::from(target.offset);
        self.header_mut().data_offset += target.size;

        if start != end {
            // Compact the data area
            let dest = start + usize::from(target.size);
            self.block.copy_within(start..end, dest);
        }

        // Update the items' data offsets
        let items = self.items_mut();
        for item in items {
            if item.offset <= target.offset {
                item.offset += target.size;
            }
        }
    }

    fn take_entries_from_left(&mut self, left: &mut Self, item_count: usize) {
        let start = usize::from(left.header().item_count) - item_count;
        let items_to_take = &left.items()[start..];

        for item in items_to_take.iter().rev() {
            let data = left.get_data_for_item(item);
            self.insert_entry_front(item.key, data);
        }

        for _ in 0..item_count {
            left.remove_entry_at(start.into());
        }
    }

    fn take_entries_from_right(&mut self, right: &mut Self, item_count: usize) {
        let items_to_take = &right.items()[..item_count.into()];

        for item in items_to_take {
            let data = right.get_data_for_item(item);
            self.insert_entry_back(item.key, data);
        }

        for _ in 0..item_count {
            right.remove_entry_at(0);
        }
    }

    /// Returns the number of used bytes in the leaf.
    fn used(&self) -> usize {
        let items_size = usize::from(self.header().item_count.get()) * LeafItem::SIZE;
        let data_size = BLOCK_SIZE as usize - usize::from(self.header().data_offset.get());
        items_size + data_size
    }

    /// How many bytes a leaf can fit.
    const CAPACITY: usize = BLOCK_SIZE as usize - Header::SIZE;

    /// Returns the remaining capacity of the leaf.
    fn available(&self) -> usize {
        Self::CAPACITY - (self.used())
    }

    fn can_insert(&self, item_count: usize, data_size: usize) -> bool {
        let items_size = item_count * LeafItem::SIZE;
        self.available() >= (items_size + data_size)
    }

    const MIN_OCCUPANCY: usize = Self::CAPACITY / 2;

    fn is_underfull(&self) -> bool {
        (self.used()) < Self::MIN_OCCUPANCY
    }

    fn rotate_count<'a>(&self, sibling: &Self, items: impl Iterator<Item = &'a LeafItem>) -> u16 {
        let mut self_used = self.used();
        let mut sibling_used = sibling.used();

        for (i, item) in items.enumerate() {
            let diff = LeafItem::SIZE + usize::from(item.size);

            sibling_used -= diff;
            if sibling_used < Self::MIN_OCCUPANCY {
                break;
            }

            self_used += diff;
            if self_used >= Self::MIN_OCCUPANCY {
                return (i + 1) as u16;
            }
        }

        0
    }

    fn split_count(&self) -> u16 {
        let mut self_used = self.used();
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
}

// pub trait Rotate<I: Item> {
//     fn rotate_left(&mut self, right: &mut NodeMut<'_, I>) -> Result<(), SiblingBecomesDeficient>;
//
//     fn rotate_right(&mut self, left: &mut NodeMut<'_, I>) -> Result<(), SiblingBecomesDeficient>;
//
//     fn merge(&mut self, right: &Node<'_, I>) -> Result<(), Overflows>;
// }
//
// impl<'a> Rotate<BranchItem> for NodeMut<'a, BranchItem> {
//     fn rotate_left(
//         &mut self,
//         right: &mut NodeMut<'_, BranchItem>,
//     ) -> Result<(), SiblingBecomesDeficient> {
//         let req_count = Self::ITEM_OCCUPANCY_THRESH - self.item_count();
//         let right_count = right.item_count();
//
//         if (right_count.saturating_sub(req_count)) < Self::ITEM_OCCUPANCY_THRESH {
//             return Err(SiblingBecomesDeficient);
//         }
//
//         self.take_items_from_right(right, req_count.into());
//         Ok(())
//     }
//
//     fn rotate_right(
//         &mut self,
//         left: &mut NodeMut<'_, BranchItem>,
//     ) -> Result<(), SiblingBecomesDeficient> {
//         let req_count = Self::ITEM_OCCUPANCY_THRESH - self.item_count();
//         let left_count = left.item_count();
//
//         if (left_count.saturating_sub(req_count)) < Self::ITEM_OCCUPANCY_THRESH {
//             return Err(SiblingBecomesDeficient);
//         }
//
//         self.take_items_from_left(left, req_count.into());
//         Ok(())
//     }
//
//     fn merge(&mut self, right: &Node<'_, BranchItem>) -> Result<(), Overflows> {
//         self.insert_items_back(right.items());
//         Ok(())
//     }
// }
//
// impl<'a> Rotate<LeafItem> for NodeMut<'a, LeafItem> {
//     fn rotate_left(
//         &mut self,
//         right: &mut NodeMut<'_, LeafItem>,
//     ) -> Result<(), SiblingBecomesDeficient> {
//         let count = self.rotate_count(right, right.items().iter());
//         if count == 0 {
//             return Err(SiblingBecomesDeficient);
//         }
//
//         self.take_from_right(right, count);
//         Ok(())
//     }
//
//     fn rotate_right(
//         &mut self,
//         left: &mut NodeMut<'_, LeafItem>,
//     ) -> Result<(), SiblingBecomesDeficient> {
//         let count = self.rotate_count(left, left.items().iter().rev());
//         if count == 0 {
//             return Err(SiblingBecomesDeficient);
//         }
//
//         self.take_from_left(left, count);
//         Ok(())
//     }
//
//     fn merge(&mut self, right: &Node<'_, LeafItem>) -> Result<(), Overflows> {
//         let right_items = right.items();
//         let right_data_size = BLOCK_SIZE as usize - usize::from(right.data_offset());
//
//         if !self.can_insert(right_items.len(), right_data_size) {
//             return Err(Overflows);
//         }
//
//         self.copy_with_strategy(right, right_items.iter(), |node, key, data| {
//             node.insert_back(key, data)
//         });
//
//         Ok(())
//     }
// }
//
// pub trait Split<I: Item> {
//     fn split(&mut self, right: &mut NodeMut<'_, I>);
// }
//
// impl<'a> Split<BranchItem> for NodeMut<'a, BranchItem> {
//     fn split(&mut self, right: &mut NodeMut<'_, BranchItem>) {
//         let count = usize::from(self.item_count() / 2);
//         right.take_items_from_left(self, count);
//     }
// }
//
// impl<'a> Split<LeafItem> for NodeMut<'a, LeafItem> {
//     fn split(&mut self, right: &mut NodeMut<'_, LeafItem>) {
//         let count = self.split_count();
//         right.take_from_left(self, count);
//     }
// }

/// A header stored at the beginning of a node.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
pub struct Header {
    // The distance from this node to a leaf node
    height: U16,
    item_count: U16,
    // The absolute offset of the data area in a leaf node
    data_offset: U16,
}

impl Header {
    pub fn format(block: &mut Block, height: u16) {
        let mut header = Header::default();
        header.height.set(height);
        block[..Header::SIZE].copy_from_slice(header.as_bytes());
    }

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
