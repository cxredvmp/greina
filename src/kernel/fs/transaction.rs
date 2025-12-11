use std::collections::HashMap;

use zerocopy::{FromBytes, IntoBytes};

use crate::{
    hardware::storage::{
        Storage,
        block::{BLOCK_SIZE, Block},
    },
    kernel::fs::{
        FileSystem,
        node::{NODE_SIZE, NODES_PER_BLOCK, Node},
    },
};

/// Represents a filesystem operation that buffers changes in memory before commiting them to
/// persistent storage.
pub struct Transaction<'a> {
    fs: &'a mut FileSystem,
    storage: &'a mut Storage,
    changes: HashMap<usize, Block>,
}

impl<'a> Transaction<'a> {
    /// Constructs a [Transaction] for the given filesystem and storage.
    pub fn new(fs: &'a mut FileSystem, storage: &'a mut Storage) -> Self {
        Self {
            fs,
            storage,
            changes: HashMap::new(),
        }
    }

    /// Commits the transaction to persistent storage, consuming the transaction.
    pub fn commit(self) {
        for (&block_index, block) in self.changes.iter() {
            self.storage
                .write_block(block_index, block)
                .expect("'block_index' must be a valid block index")
        }
    }

    /// Reads the node at a given node index from the node table.
    pub fn read_node(&self, node_index: usize) -> Result<Node, Error> {
        let block_index = self
            .get_node_block_index(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        let block = self.read_block(block_index)?;
        let byte_offset = self
            .get_node_byte_offset(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        Ok(
            Node::read_from_bytes(&block.data[byte_offset..(byte_offset + NODE_SIZE)])
                .expect("'bytes' must have length 'NODE_SIZE'"),
        )
    }

    // Writes the node at a given node index to the node table.
    pub fn write_node(&mut self, node_index: usize, node: Node) -> Result<(), Error> {
        let block_index = self
            .get_node_block_index(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        let mut block = self.read_block(block_index)?;
        let byte_offset = self
            .get_node_byte_offset(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        block.data[byte_offset..(byte_offset + NODE_SIZE)].copy_from_slice(node.as_bytes());
        self.write_block(block_index, &block);
        Ok(())
    }

    /// Reads the physical block at a given block index.
    pub fn read_block(&self, block_index: usize) -> Result<Block, Error> {
        // Check cached changes
        match self.changes.get(&block_index) {
            Some(block) => Ok(*block),
            None => self
                .storage
                .read_block(block_index)
                .map_err(|_| Error::BlockIndexOutOfBounds),
        }
    }

    /// Writes the physical block at a given block index.
    pub fn write_block(&mut self, block_index: usize, block: &Block) {
        self.changes.insert(block_index, *block);
    }

    /// Reads the logical block that belongs to the node.
    pub fn read_logical_block(&self, node: &Node, logical_index: usize) -> Result<Block, Error> {
        let block_index = node
            .get_physical_block(logical_index)
            .ok_or(Error::LogicalIndexOutOfBounds)?;
        self.read_block(block_index)
    }

    /// Writes the logical block that belongs to the node.
    pub fn write_logical_block(
        &mut self,
        node: Node,
        logical_index: usize,
        block: &Block,
    ) -> Result<(), Error> {
        let block_index = node
            .get_physical_block(logical_index)
            .ok_or(Error::LogicalIndexOutOfBounds)?;
        self.write_block(block_index, block);
        Ok(())
    }

    /// Returns the index of the block in which the node resides.
    fn get_node_block_index(&self, node_index: usize) -> Option<usize> {
        if node_index < self.fs.superblock.node_count {
            Some(self.fs.superblock.node_table_offset + (node_index * NODE_SIZE / BLOCK_SIZE))
        } else {
            None
        }
    }

    /// Returns the byte offset of the node within the block.
    fn get_node_byte_offset(&self, node_index: usize) -> Option<usize> {
        if node_index < self.fs.superblock.node_count {
            Some(node_index % NODES_PER_BLOCK * NODE_SIZE)
        } else {
            None
        }
    }
}

pub enum Error {
    BlockIndexOutOfBounds,
    NodeIndexOutOfBounds,
    LogicalIndexOutOfBounds,
}
