use std::collections::BTreeMap;

use crate::kernel::fs::node::{FileType, Node};

/// Tracks opened files.
pub type OpenFileTable = BTreeMap<FileDescriptorNumber, FileDescriptor>;

/// Used to index the open file table.
pub type FileDescriptorNumber = usize;

/// A unique handle to a file.
pub struct FileDescriptor {
    node_index: usize,
    pub offset: usize,
}

impl FileDescriptor {
    /// Creates a new [FileDescriptor] for the file.
    pub fn new(node_index: usize) -> Self {
        Self {
            node_index,
            offset: 0,
        }
    }

    pub fn node_index(&self) -> usize {
        self.node_index
    }
}

pub struct FileStats {
    pub node_index: usize,
    pub filetype: FileType,
    pub link_count: u32,
    pub size: usize,
    pub block_count: usize,
}

impl FileStats {
    pub fn new(node_index: usize, node: Node) -> Self {
        Self {
            node_index,
            filetype: node.filetype(),
            link_count: node.link_count,
            size: node.size,
            block_count: node.block_count(),
        }
    }
}
