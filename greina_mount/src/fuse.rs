use std::{
    ffi::OsStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fuser::{FileAttr, FileType, Filesystem};

use crate::{
    block::{Allocator, BLOCK_SIZE, storage::Storage},
    fs::{
        self,
        node::{self, Node, NodeId, dir::NAME_MAX_LEN},
    },
};

/// How long the kernel should cache node attributes
const TTL: Duration = Duration::from_secs(1);

pub struct Fuse<S: Storage> {
    fs: fs::Filesystem<S>,
}

impl<S: Storage> Fuse<S> {
    pub fn new(fs: fs::Filesystem<S>) -> Self {
        Self { fs }
    }
}

impl<S: Storage> Filesystem for Fuse<S> {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let parent_id = NodeId::new(parent);
        let name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };
        let res = self.fs.tx(|tx| {
            let entry = tx.find_entry(parent_id, name)?;
            let node_id = entry.id;
            let node = tx.read_node(node_id)?;
            Ok((node_id, node))
        });
        match res {
            Ok((node_id, node)) => reply.entry(&TTL, &node_attr(node_id, &node), 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        let node_id = NodeId::new(ino);
        let res = self.fs.tx(|tx| tx.read_node(node_id));
        match res {
            Ok(node) => reply.attr(&TTL, &node_attr(node_id, &node)),
            Err(e) => reply.error(e.into()),
        }
    }

    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        let node_id = NodeId::new(ino);
        let res = self.fs.tx(|tx| {
            if let Some(size) = size {
                tx.truncate_file(node_id, size)?;
            }
            let node = tx.read_node(node_id)?;
            Ok(node)
        });

        match res {
            Ok(node) => reply.attr(&TTL, &node_attr(node_id, &node)),
            Err(e) => reply.error(e.into()),
        }
    }

    fn mkdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let parent_id = NodeId::new(parent);

        let name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };

        let res = self.fs.tx(|tx| {
            let node_id = tx.create_dir(parent_id, name)?;
            let node = tx.read_node(node_id)?;
            Ok((node_id, node))
        });

        match res {
            Ok((node_id, node)) => reply.entry(&TTL, &node_attr(node_id, &node), 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let parent_id = NodeId::new(parent);
        let name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };
        let res = self.fs.tx(|tx| {
            tx.remove_dir(parent_id, name)?;
            Ok(())
        });
        match res {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn symlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: fuser::ReplyEntry,
    ) {
        let parent_id = NodeId::new(parent);
        let name = match link_name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };
        let target = match target.to_str() {
            Some(target) => target,
            None => return reply.error(libc::EILSEQ),
        };

        let res = self.fs.tx(|tx| {
            let node_id = tx.create_symlink(parent_id, name, target)?;
            let node = tx.read_node(node_id)?;
            Ok((node_id, node))
        });

        match res {
            Ok((node_id, node)) => reply.entry(&TTL, &node_attr(node_id, &node), 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn readlink(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        let symlink_id = NodeId::new(ino);
        let res = self.fs.tx(|tx| tx.read_symlink(symlink_id));
        match res {
            Ok(path) => reply.data(&path),
            Err(e) => reply.error(e.into()),
        }
    }

    fn link(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let node_id = NodeId::new(ino);
        let parent_id = NodeId::new(newparent);
        let name = match newname.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };

        let res = self.fs.tx(|tx| {
            tx.link_file(parent_id, node_id, name)?;
            let node = tx.read_node(node_id)?;
            Ok((node_id, node))
        });

        match res {
            Ok((node_id, node)) => reply.entry(&TTL, &node_attr(node_id, &node), 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let parent_id = NodeId::new(parent);
        let name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };
        let res = self.fs.tx(|tx| tx.unlink_file(parent_id, name));
        match res {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn rename(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let old_parent_id = NodeId::new(parent);
        let old_name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };

        let new_parent_id = NodeId::new(newparent);
        let new_name = match newname.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };

        let res = self
            .fs
            .tx(|tx| tx.rename_entry(old_parent_id, old_name, new_parent_id, new_name));

        match res {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let parent_id = NodeId::new(parent);

        let name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };

        let file_type = match node::FileType::try_from(mode) {
            Ok(ft) => ft,
            Err(e) => return reply.error(e),
        };
        match file_type {
            node::FileType::File => (),
            _ => return reply.error(libc::EINVAL),
        }

        let res = self.fs.tx(|tx| {
            let node_id = tx.create_file(parent_id, name, file_type)?;
            let node = tx.read_node(node_id)?;
            Ok((node_id, node))
        });

        match res {
            Ok((node_id, node)) => reply.created(&TTL, &node_attr(node_id, &node), 0, 0, 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let node_id = NodeId::new(ino);
        let mut buf = vec![0u8; size as usize];
        let res = self
            .fs
            .tx(|tx| tx.read_file_at(node_id, offset as u64, &mut buf));
        match res {
            Ok(read) => reply.data(&buf[..read as usize]),
            Err(e) => reply.error(e.into()),
        }
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let node_id = NodeId::new(ino);
        let res = self
            .fs
            .tx(|tx| tx.write_file_at(node_id, offset as u64, data));
        match res {
            Ok(written) => reply.written(written as u32),
            Err(e) => reply.error(e.into()),
        }
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let node_id = NodeId::new(ino);
        let res = self.fs.tx(|tx| tx.read_dir(node_id));
        match res {
            Ok(dir) => {
                for (i, entry) in dir.iter().enumerate().skip(offset as usize) {
                    let is_full = reply.add(
                        entry.id.get(),
                        (i + 1) as i64,
                        entry.filetype.into(),
                        entry.name.as_str(),
                    );
                    if is_full {
                        break;
                    };
                }
                reply.ok();
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        let blocks = self.fs.superblock().block_count;
        let blocks_free = self.fs.block_alloc().available();

        reply.statfs(
            blocks,
            blocks_free,
            blocks_free,
            0,
            0,
            BLOCK_SIZE as u32,
            NAME_MAX_LEN as u32,
            0,
        );
    }
}

fn node_attr(node_id: NodeId, node: &Node) -> FileAttr {
    let perm = match node.filetype {
        node::FileType::Dir => 0o777,
        _ => 0o666,
    };

    FileAttr {
        ino: node_id.get(),
        size: node.size.get(),
        blocks: node.size.get().div_ceil(BLOCK_SIZE),
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: FileType::from(node.filetype),
        perm,
        nlink: node.links.get(),
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: BLOCK_SIZE as u32,
        flags: 0,
    }
}

impl From<node::FileType> for FileType {
    fn from(value: node::FileType) -> Self {
        match value {
            node::FileType::File => Self::RegularFile,
            node::FileType::Dir => FileType::Directory,
            node::FileType::Symlink => FileType::Symlink,
        }
    }
}

#[cfg(target_os = "linux")]
impl TryFrom<u32> for node::FileType {
    type Error = libc::c_int;

    fn try_from(mode: u32) -> Result<Self, Self::Error> {
        let file_type = mode & libc::S_IFMT;
        match file_type {
            libc::S_IFREG => Ok(Self::File),
            libc::S_IFDIR => Ok(Self::Dir),
            libc::S_IFLNK => Ok(Self::Symlink),
            _ => Err(libc::EINVAL),
        }
    }
}

#[cfg(target_os = "macos")]
impl TryFrom<u32> for node::FileType {
    type Error = libc::c_int;

    fn try_from(mode: u32) -> Result<Self, Self::Error> {
        let file_type = mode & libc::S_IFMT as u32;
        if file_type == (libc::S_IFREG as u32) {
            Ok(Self::File)
        } else if file_type == (libc::S_IFDIR as u32) {
            Ok(Self::Dir)
        } else if file_type == (libc::S_IFLNK as u32) {
            Ok(Self::Symlink)
        } else {
            Err(libc::EINVAL)
        }
    }
}
