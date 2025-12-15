use std::collections::BTreeMap;

use crate::kernel::fs::node::{FileType, Node, NodePtr};

/// Tracks opened files.
pub type OpenFileTable = BTreeMap<FileDescriptor, FileDescription>;

/// A unique id used to track opened files.
pub type FileDescriptor = usize;

/// A unique handle to a file.
pub struct FileDescription {
    node_ptr: NodePtr,
    pub offset: usize,
}

impl FileDescription {
    /// Creates a new [FileDescriptor] for the file.
    pub fn new(node_ptr: NodePtr) -> Self {
        Self {
            node_ptr,
            offset: 0,
        }
    }

    pub fn node_ptr(&self) -> NodePtr {
        self.node_ptr
    }
}

pub struct FileStats {
    pub node_id: usize,
    pub filetype: FileType,
    pub link_count: u32,
    pub size: usize,
    pub block_count: usize,
}

impl FileStats {
    pub fn new(node_ptr: NodePtr, node: Node) -> Self {
        Self {
            node_id: node_ptr.id(),
            filetype: node.filetype(),
            link_count: node.link_count,
            size: node.size,
            block_count: node.block_count(),
        }
    }
}
