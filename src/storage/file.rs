use std::{
    fs::{File, OpenOptions},
    io::{self, Seek},
    os::unix::fs::FileExt,
};

use libc::EIO;

use crate::{
    block::{BLOCK_SIZE, Block, BlockAddr},
    storage::{Result, Storage},
};

/// Storage that uses a file to store data.
pub struct FileStorage {
    file: File,
}

impl FileStorage {
    /// Opens a file to be used as `FileStorage`.
    /// If file's size is not a multiple of `BLOCK_SIZE` the remaining bytes are not addressable.
    pub fn open(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .into_errno()?;
        Ok(Self { file })
    }

    /// Creates a file to be used as `FileStorage`.
    /// The file's size is `block_count * BLOCK_SIZE` bytes.
    pub fn create(path: &str, block_count: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .into_errno()?;
        file.set_len(block_count * BLOCK_SIZE).into_errno()?;
        Ok(Self { file })
    }
}

impl Storage for FileStorage {
    fn read_block_at(&self, block: &mut Block, addr: BlockAddr) -> Result<()> {
        self.file
            .read_at(&mut block.data, addr * BLOCK_SIZE)
            .into_errno()
            .and_then(|b| {
                if b != BLOCK_SIZE as usize {
                    Err(EIO)
                } else {
                    Ok(())
                }
            })
    }

    fn write_block_at(&mut self, block: &Block, addr: BlockAddr) -> Result<()> {
        self.file
            .write_at(&block.data, addr * BLOCK_SIZE)
            .into_errno()
            .and_then(|b| {
                if b != BLOCK_SIZE as usize {
                    Err(EIO)
                } else {
                    Ok(())
                }
            })
    }

    fn block_count(&mut self) -> Result<u64> {
        let size = self.file.seek(io::SeekFrom::End(0)).into_errno()?;
        Ok(size / BLOCK_SIZE)
    }
}

trait IntoErrno {
    type T;

    fn into_errno(self) -> Result<Self::T>;
}

impl<T> IntoErrno for io::Result<T> {
    type T = T;

    fn into_errno(self) -> Result<Self::T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(e.raw_os_error().unwrap_or(EIO)),
        }
    }
}
