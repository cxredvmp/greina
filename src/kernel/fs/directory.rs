use zerocopy::{FromBytes, Immutable, IntoBytes, TryFromBytes};

use crate::kernel::fs::node::{FileType, NodePtr};

/// Tracks entries within a directory.
pub struct Dir {
    entries: Vec<DirEntry>,
}

impl Dir {
    /// Constructs an empty [Dir] with given node pointer and parent node pointer.
    pub fn new(node_ptr: NodePtr, parent_ptr: NodePtr) -> Self {
        let mut dir = Self {
            entries: Vec::new(),
        };
        dir.add_entry(DirEntry::itself(node_ptr));
        dir.add_entry(DirEntry::parent(parent_ptr));
        dir
    }

    /// Returns a reference to the entry with given name.
    pub fn get_entry(&self, name: DirEntryName) -> Option<&DirEntry> {
        self.entries.iter().find(|e| e.name == name && !e.is_null())
    }

    /// Returns a mutable reference to the entry with given name.
    pub fn get_mut_entry(&mut self, name: DirEntryName) -> Option<&mut DirEntry> {
        self.entries
            .iter_mut()
            .find(|e| e.name == name && !e.is_null())
    }

    /// Adds an entry to the directory.
    pub fn add_entry(&mut self, entry: DirEntry) {
        let vacancy = self.entries.iter_mut().find(|e| e.is_null());
        match vacancy {
            Some(v) => *v = entry,
            None => self.entries.push(entry),
        }
    }

    /// Removes the entry from the directory, returning its node pointer.
    pub fn remove_entry(&mut self, name: DirEntryName) -> Result<NodePtr> {
        let entry = self.get_mut_entry(name).ok_or(Error::EntryNotFound)?;
        let node_ptr = entry.node_ptr;
        entry.node_ptr = NodePtr::default();
        Ok(node_ptr)
    }

    /// Checks if the directory is empty (contains only `.` and `..` entries).
    pub fn is_empty(&self) -> bool {
        self.entries.iter().filter(|e| !e.is_null()).count() == 2
    }

    /// Returns a view of the directory as a slice of [DirEntry].
    pub fn as_slice(&self) -> &[DirEntry] {
        self.entries.as_slice()
    }

    /// Constructs a [Dir] from a slice of [DirEntry].
    pub fn from_slice(entries: &[DirEntry]) -> Self {
        Self {
            entries: entries.to_vec(),
        }
    }
}

/// Represents a [Dir] entry.
#[repr(C)]
#[derive(Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable)]
pub struct DirEntry {
    filetype: FileType,
    _pad: [u8; 7],
    node_ptr: NodePtr,
    name: DirEntryName,
}

impl DirEntry {
    /// Constructs a directory entry with given node pointer, file type and name.
    pub fn new(node_ptr: NodePtr, filetype: FileType, name: DirEntryName) -> Self {
        Self {
            node_ptr,
            _pad: [0u8; 7],
            filetype,
            name,
        }
    }

    /// Constructs a `.` directory entry with given node pointer.
    pub fn itself(node_ptr: NodePtr) -> Self {
        Self::new(
            node_ptr,
            FileType::Dir,
            DirEntryName::try_from(".").expect("'.' must be a valid directory entry name"),
        )
    }

    /// Constructs a `..` directory entry with given node pointer.
    pub fn parent(node_ptr: NodePtr) -> Self {
        Self::new(
            node_ptr,
            FileType::Dir,
            DirEntryName::try_from("..").expect("'..' must be a valid directory entry name"),
        )
    }

    /// Checks if the entry contains a null node pointer.
    pub fn is_null(&self) -> bool {
        self.node_ptr.is_null()
    }

    pub fn filetype(&self) -> FileType {
        self.filetype
    }

    pub fn node_ptr(&self) -> NodePtr {
        self.node_ptr
    }

    pub fn name(&self) -> Result<&str> {
        self.name.as_str()
    }
}

/// How long a directory entry name can be.
const NAME_MAX: usize = 64;

/// Represents the name of a directory entry.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct DirEntryName {
    bytes: [u8; NAME_MAX],
}

impl DirEntryName {
    /// Returns the directory entry name as a string slice `&str`.
    ///
    /// # Errors
    /// Returns `Err` if:
    /// - `self.bytes` is not a valid UTF-8 string (data corruption?)
    pub fn as_str(&self) -> Result<&str> {
        <&str>::try_from(self)
    }
}

impl TryFrom<&str> for DirEntryName {
    type Error = Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        let len = value.len();
        if len > NAME_MAX {
            return Err(Error::NameTooLong);
        }
        let mut bytes = [0u8; NAME_MAX];
        bytes[..len].copy_from_slice(value.as_bytes());
        Ok(Self { bytes })
    }
}

impl<'a> TryFrom<&'a DirEntryName> for &'a str {
    type Error = Error;

    fn try_from(value: &'a DirEntryName) -> std::result::Result<Self, Self::Error> {
        let len = value.bytes.iter().position(|&b| b == 0).unwrap_or(NAME_MAX);
        str::from_utf8(&value.bytes[..len]).map_err(|_| Error::CorruptedName)
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    EntryNotFound,
    NameTooLong,
    CorruptedName,
}
