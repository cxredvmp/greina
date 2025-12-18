use zerocopy::{FromBytes, Immutable, IntoBytes, TryFromBytes};

use crate::fs::node::{FileType, NodePtr};

/// Tracks entries within a directory.
pub struct Dir {
    // TODO: Parse directories lazily
    entries: Vec<DirEntry>,
}

impl Dir {
    /// Constructs an empty [Dir] with given node pointer and parent node pointer.
    pub fn new(node_ptr: NodePtr, parent_ptr: NodePtr) -> Self {
        let mut dir = Self {
            entries: Vec::new(),
        };
        dir.add_entry(DirEntry::itself(node_ptr))
            .expect("'.' entry must be added");
        dir.add_entry(DirEntry::parent(parent_ptr))
            .expect("'..' entry must be added");
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

    /// Returns a mutable reference to the `..` entry.
    pub fn get_mut_parent(&mut self) -> &mut DirEntry {
        self.get_mut_entry(DirEntryName::parent())
            .expect("'..' entry must exist")
    }

    /// Returns the pointer to directory's parent.
    pub fn parent_ptr(&self) -> NodePtr {
        self.get_entry(DirEntryName::parent())
            .expect("'..' entry must exist")
            .node_ptr
    }

    /// Adds an entry to the directory.
    pub fn add_entry(&mut self, entry: DirEntry) -> Result<()> {
        if self.get_entry(entry.name).is_some() {
            return Err(Error::EntryExists);
        }
        let vacancy = self.entries.iter_mut().find(|e| e.is_null());
        match vacancy {
            Some(v) => *v = entry,
            None => self.entries.push(entry),
        }
        Ok(())
    }

    /// Removes the entry from the directory, returning it.
    pub fn remove_entry(&mut self, name: DirEntryName) -> Option<DirEntry> {
        let entry = self.get_mut_entry(name)?;
        let old_entry = *entry;
        entry.node_ptr = NodePtr::default();
        Some(old_entry)
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
    pub file_type: FileType,
    _pad: [u8; 7],
    pub node_ptr: NodePtr,
    pub name: DirEntryName,
}

impl DirEntry {
    /// Constructs a directory entry with a given node pointer, file type and name.
    pub fn new(node_ptr: NodePtr, file_type: FileType, name: DirEntryName) -> Self {
        Self {
            node_ptr,
            _pad: [0u8; 7],
            file_type,
            name,
        }
    }

    /// Constructs a `.` directory entry with a given node pointer.
    pub fn itself(node_ptr: NodePtr) -> Self {
        Self::new(node_ptr, FileType::Dir, DirEntryName::itself())
    }

    /// Constructs a `..` directory entry with a given node pointer.
    pub fn parent(node_ptr: NodePtr) -> Self {
        Self::new(node_ptr, FileType::Dir, DirEntryName::parent())
    }

    /// Checks if the entry contains a null node pointer.
    pub fn is_null(&self) -> bool {
        self.node_ptr.is_null()
    }
}

/// How long a directory entry name can be.
pub const NAME_MAX: usize = 64;

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

    /// Returns the `.` directory entry name.
    pub fn itself() -> Self {
        Self::try_from(".").expect("'.' must be a valid entry name")
    }

    /// Returns the `..` directory entry name.
    pub fn parent() -> Self {
        Self::try_from("..").expect("'..' must be a valid entry name")
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
    EntryExists,
    NameTooLong,

    // Filesystem corruption or logic error
    CorruptedName,
}

impl From<Error> for libc::c_int {
    fn from(err: Error) -> Self {
        match err {
            Error::EntryNotFound => libc::ENOENT,
            Error::EntryExists => libc::EEXIST,
            Error::NameTooLong => libc::ENAMETOOLONG,

            Error::CorruptedName => libc::EIO,
        }
    }
}
