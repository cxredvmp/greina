use crate::{block::allocator, tree};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Storage(libc::c_int),
    Allocator(allocator::Error),
    Tree(tree::Error),

    Uninterpretable,

    // Node
    NodeNotFound,
    NodeExists,

    // DirEntry
    InvalidName,
    DirEntryNotFound,
    DirEntryExists,

    // Dir
    DirNotEmpty,
    IsDir,
    NotDir,
    InvalidMove,

    // Symlink
    NotSymlink,
}

impl From<libc::c_int> for Error {
    fn from(errno: libc::c_int) -> Self {
        Self::Storage(errno)
    }
}

impl From<allocator::Error> for Error {
    fn from(err: allocator::Error) -> Self {
        Self::Allocator(err)
    }
}

impl From<tree::Error> for Error {
    fn from(err: tree::Error) -> Self {
        match err {
            tree::Error::Storage(err) => Self::Storage(err),
            tree::Error::Allocator(err) => Self::Allocator(err),
            err => Self::Tree(err),
        }
    }
}

impl From<Error> for libc::c_int {
    fn from(err: Error) -> Self {
        match err {
            Error::Storage(errno) => errno,
            Error::Allocator(err) => err.into(),
            Error::Tree(err) => match err {
                tree::Error::Storage(errno) => errno,
                tree::Error::Allocator(err) => err.into(),
                _ => libc::EIO,
            },
            Error::Uninterpretable => libc::EIO,
            Error::NodeNotFound => libc::EIO,
            Error::NodeExists => libc::EIO,
            Error::InvalidName => libc::EINVAL,
            Error::DirEntryNotFound => libc::ENOENT,
            Error::DirEntryExists => libc::EEXIST,
            Error::DirNotEmpty => libc::ENOTEMPTY,
            Error::IsDir => libc::EISDIR,
            Error::NotDir => libc::ENOTDIR,
            Error::InvalidMove => libc::EINVAL,
            Error::NotSymlink => libc::EINVAL,
        }
    }
}
