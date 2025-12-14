use crate::kernel::{
    Kernel,
    file::{FileDescription, FileDescriptor, FileStats},
    fs::{
        Filesystem,
        directory::{self},
        node::FileType,
        path::Path,
        transaction::{self, Transaction},
    },
};

impl Kernel {
    /// Creates a file at `path`, if it doesn't exist.
    pub fn create(&mut self, path: &str) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let mut tx = Transaction::new(fs, &mut self.storage);

        let path = Path::new(path);
        let (parent, name) = path.split_last().ok_or(Error::NotPermitted)?;
        let parent = tx.path_node(&parent, self.curr_dir)?;

        tx.create_file(parent, &name, FileType::File)?;
        tx.commit();
        Ok(())
    }

    /// Opens the file at `path`, returning a corresponding file descriptor.
    pub fn open(&mut self, path: &str) -> Result<FileDescriptor> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let tx = Transaction::new(fs, &mut self.storage);

        let path = Path::new(path);
        let node_index = tx.path_node(&path, self.curr_dir)?;
        tx.commit();

        let fd = FileDescription::new(node_index);
        Ok(self.open_file(fd))
    }

    /// Close the file descriptor referenced by `fd`.
    pub fn close(&mut self, fd: FileDescriptor) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let desc = self
            .open_files
            .remove(&fd)
            .ok_or(Error::InvalidFileDescriptor)?;
        let is_opened = self
            .open_files
            .values()
            .any(|d| d.node_index() == desc.node_index());
        if !is_opened {
            let mut tx = Transaction::new(fs, &mut self.storage);
            let node = tx.read_node(desc.node_index())?;
            if node.link_count == 0 {
                tx.remove_node(desc.node_index())?;
            };
            tx.commit();
        }
        Ok(())
    }

    /// Reposition the offset of the file descriptor referenced by `fd`.
    pub fn seek(&mut self, fd: FileDescriptor, offset: usize) -> Result<()> {
        let desc = self
            .open_files
            .get_mut(&fd)
            .ok_or(Error::InvalidFileDescriptor)?;
        desc.offset = offset;
        Ok(())
    }

    /// Reads up to `buf.len()` bytes into `buf` from the file referenced by `fd`.
    /// Returns the number of bytes read.
    pub fn read(&mut self, fd: FileDescriptor, buf: &mut [u8]) -> Result<usize> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let desc = self
            .open_files
            .get_mut(&fd)
            .ok_or(Error::InvalidFileDescriptor)?;
        let tx = Transaction::new(fs, &mut self.storage);
        let bytes_read = tx.read_file_at(desc.node_index(), desc.offset, buf)?;
        tx.commit();
        desc.offset += bytes_read;
        Ok(bytes_read)
    }

    /// Writes up to `buf.len()` bytes from `buf` to the file referenced by `fd`.
    /// Returns the number of bytes written.
    pub fn write(&mut self, fd: FileDescriptor, buf: &[u8]) -> Result<usize> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let desc = self
            .open_files
            .get_mut(&fd)
            .ok_or(Error::InvalidFileDescriptor)?;
        let mut tx = Transaction::new(fs, &mut self.storage);
        let bytes_written = tx.write_file_at(desc.node_index(), desc.offset, buf)?;
        tx.commit();
        desc.offset += bytes_written;
        Ok(bytes_written)
    }

    /// Creates a hard link at `new_path` to the file at `old_path`.
    pub fn link(&mut self, old_path: &str, new_path: &str) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let mut tx = Transaction::new(fs, &mut self.storage);

        let old_path = Path::new(old_path);
        let node_index = tx.path_node(&old_path, self.curr_dir)?;

        let new_path = Path::new(new_path);
        let (parent, name) = new_path.split_last().ok_or(Error::NotPermitted)?;
        let parent = tx.path_node(&parent, self.curr_dir)?;

        tx.link_file(parent, node_index, &name)?;
        tx.commit();
        Ok(())
    }

    /// Removes the hard link at `path` from the filesystem.
    /// If it was the last hard link to the file, it is deleted.
    /// If the file is currently opened, it is deleted after it's closed.
    pub fn unlink(&mut self, path: &str) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let mut tx = Transaction::new(fs, &mut self.storage);

        let path = Path::new(path);
        let node_index = tx.path_node(&path, self.curr_dir)?;

        let (parent, name) = path.split_last().ok_or(Error::NotPermitted)?;
        let parent = tx.path_node(&parent, self.curr_dir)?;

        let is_opened = self
            .open_files
            .values()
            .any(|desc| desc.node_index() == node_index);

        tx.unlink_file(parent, &name, !is_opened)?;
        tx.commit();
        Ok(())
    }

    /// Truncates the file at `path` to be truncated to a size of `size` bytes.
    pub fn truncate(&mut self, path: &str, size: usize) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let mut tx = Transaction::new(fs, &mut self.storage);

        let path = Path::new(path);
        let node_index = tx.path_node(&path, self.curr_dir)?;

        tx.truncate_file(node_index, size)?;
        tx.commit();
        Ok(())
    }

    /// Returns statistics about a file `path`.
    pub fn stat(&mut self, path: &str) -> Result<FileStats> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let tx = Transaction::new(fs, &mut self.storage);

        let path = Path::new(path);
        let node_index = tx.path_node(&path, self.curr_dir)?;
        let node = tx.read_node(node_index)?;
        tx.commit();
        Ok(FileStats::new(node_index, node))
    }

    /// Creates a directory at `path`.
    pub fn mkdir(&mut self, path: &str) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let mut tx = Transaction::new(fs, &mut self.storage);

        let path = Path::new(path);
        let (parent, name) = path.split_last().ok_or(Error::NotPermitted)?;
        let parent = tx.path_node(&parent, self.curr_dir)?;

        tx.create_directory(parent, &name)?;
        tx.commit();
        Ok(())
    }

    /// Deletes the directory at `path`.
    pub fn rmdir(&mut self, path: &str) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let mut tx = Transaction::new(fs, &mut self.storage);

        let path = Path::new(path);
        let (parent, name) = path.split_last().ok_or(Error::NotPermitted)?;
        if name == "." || name == ".." {
            return Err(Error::NotPermitted);
        }
        let parent = tx.path_node(&parent, self.curr_dir)?;

        tx.remove_directory(parent, &name)?;
        tx.commit();
        Ok(())
    }

    /// Changes the current directory.
    pub fn cd(&mut self, path: &str) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let tx = Transaction::new(fs, &mut self.storage);

        let path = Path::new(path);
        let node_index = tx.path_node(&path, self.curr_dir)?;
        let node = tx.read_node(node_index)?;
        if node.filetype() != FileType::Dir {
            return Err(Error::NotDir);
        }
        tx.commit();

        self.curr_dir = node_index;
        Ok(())
    }

    /// Returns the list of hard links inside the directory at `path`.
    pub fn ls(&mut self, path: &str) -> Result<Vec<(String, usize)>> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let tx = Transaction::new(fs, &mut self.storage);

        let path = Path::new(path);
        let node_index = tx.path_node(&path, self.curr_dir)?;
        let dir = tx.read_directory(node_index)?;
        tx.commit();

        dir.as_slice()
            .iter()
            .filter(|e| !e.is_null())
            .map(|e| {
                let name = e.name().map_err(transaction::Error::from)?.to_string();
                Ok((name, e.node_index()))
            })
            .collect()
    }

    /// Formats the whole storage device with a filesystem capable of handling `node_count` nodes.
    pub fn mkfs(&mut self, node_count: usize) -> Result<()> {
        let block_count = self.storage.block_count();
        self.fs = Some(Filesystem::format(
            &mut self.storage,
            block_count,
            node_count,
        ));
        self.open_files.clear();
        Ok(())
    }

    /// Mounts the filesystem.
    pub fn mount(&mut self) -> Result<()> {
        let fs = Filesystem::mount(&mut self.storage).ok_or(Error::InvalidFilesystem)?;
        self.fs = Some(fs);
        self.open_files.clear();
        Ok(())
    }

    /// Opens the file by inserting the file description into the open files table.
    /// Returns the corresponding file descriptor.
    fn open_file(&mut self, desc: FileDescription) -> FileDescriptor {
        let fd = self.find_free_fd();
        self.open_files.insert(fd, desc);
        fd
    }

    /// Returns a file descriptor that can be used to open a file.
    fn find_free_fd(&self) -> FileDescriptor {
        let mut fd = 0;
        for &occupied_fd in self.open_files.keys() {
            if fd < occupied_fd {
                return fd;
            }
            fd = occupied_fd + 1;
        }
        fd
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FilesystemNotMounted,
    InvalidFilesystem,
    Filesystem(transaction::Error),
    InvalidFileDescriptor,
    NotPermitted,
    NotDir,
}

impl From<transaction::Error> for Error {
    fn from(value: transaction::Error) -> Self {
        Self::Filesystem(value)
    }
}

impl From<directory::Error> for Error {
    fn from(value: directory::Error) -> Self {
        Self::Filesystem(transaction::Error::from(value))
    }
}
