use std::time::{Duration, SystemTime};

use fuser::{FileAttr, FileType, Filesystem};

use crate::{
    block::{BLOCK_SIZE, allocator::Allocator, storage::Storage},
    fs::{
        self,
        dir::NAME_MAX,
        node::{self, Node, NodePtr, NodeTime},
    },
};

/// How long the kernel should cache node attributes
const TTL: Duration = Duration::from_secs(1);

/// Bitmask for extracting file permissions from file mode
const PERMS: u32 = 0o7777;

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
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let parent_ptr = NodePtr(parent);
        let name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };
        let res = self.fs.tx(|tx| {
            let entry = tx.find_entry(parent_ptr, name)?;
            let node_ptr = entry.node_ptr;
            let node = tx.read_node(node_ptr)?;
            Ok((node_ptr, node))
        });
        match res {
            Ok((node_ptr, node)) => reply.entry(&TTL, &node_attr(node_ptr, &node), 0),
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
        let node_ptr = NodePtr(ino);
        let res = self.fs.tx(|tx| tx.read_node(node_ptr));
        match res {
            Ok(node) => reply.attr(&TTL, &node_attr(node_ptr, &node)),
            Err(e) => reply.error(e.into()),
        }
    }

    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        let node_ptr = NodePtr(ino);
        let res = self.fs.tx(|tx| {
            if let Some(size) = size {
                tx.truncate_file(node_ptr, size)?;
            }

            let mut node = tx.read_node(node_ptr)?;
            let mut changed = false;

            if let Some(mode) = mode {
                let perms = (mode & PERMS) as u16;
                node.perms = perms;
                changed = true;
            }

            if let Some(uid) = uid {
                node.uid = uid;
                changed = true;
            }

            if let Some(gid) = gid {
                node.gid = gid;
                changed = true;
            }

            if let Some(atime) = atime {
                let atime = match atime {
                    fuser::TimeOrNow::SpecificTime(st) => st,
                    fuser::TimeOrNow::Now => SystemTime::now(),
                };
                node.access_time = NodeTime::from(atime);
                changed = true;
            }

            if let Some(mtime) = mtime {
                let mtime = match mtime {
                    fuser::TimeOrNow::SpecificTime(st) => st,
                    fuser::TimeOrNow::Now => SystemTime::now(),
                };
                node.mod_time = NodeTime::from(mtime);
                changed = true;
            }

            if let Some(crtime) = crtime {
                node.create_time = NodeTime::from(crtime);
                changed = true;
            }

            if let Some(ctime) = ctime {
                node.change_time = NodeTime::from(ctime);
                changed = true;
            } else if changed {
                node.change_time = NodeTime::now();
            }

            if changed {
                tx.write_node(&mut node, node_ptr)?;
            }

            Ok(node)
        });

        match res {
            Ok(node) => reply.attr(&TTL, &node_attr(node_ptr, &node)),
            Err(e) => reply.error(e.into()),
        }
    }

    fn mkdir(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let parent_ptr = NodePtr(parent);

        let name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };

        let perms = ((mode & PERMS) & !umask) as u16;

        let res = self.fs.tx(|tx| {
            let node_ptr = tx.create_dir(parent_ptr, name, perms, req.uid(), req.gid())?;
            let node = tx.read_node(node_ptr)?;
            Ok((node_ptr, node))
        });

        match res {
            Ok((node_ptr, node)) => reply.entry(&TTL, &node_attr(node_ptr, &node), 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let parent_ptr = NodePtr(parent);
        let name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };
        let res = self.fs.tx(|tx| tx.remove_dir(parent_ptr, name));
        match res {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn symlink(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        link_name: &std::ffi::OsStr,
        target: &std::path::Path,
        reply: fuser::ReplyEntry,
    ) {
        let parent_ptr = NodePtr(parent);
        let name = match link_name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };
        let target = match target.to_str() {
            Some(target) => target,
            None => return reply.error(libc::EILSEQ),
        };

        let res = self.fs.tx(|tx| {
            let node_ptr = tx.create_symlink(parent_ptr, name, target, req.uid(), req.gid())?;
            let node = tx.read_node(node_ptr)?;
            Ok((node_ptr, node))
        });

        match res {
            Ok((node_ptr, node)) => reply.entry(&TTL, &node_attr(node_ptr, &node), 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn readlink(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        let symlink_ptr = NodePtr(ino);
        let res = self.fs.tx(|tx| tx.read_symlink(symlink_ptr));
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
        newname: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let node_ptr = NodePtr(ino);
        let parent_ptr = NodePtr(newparent);
        let name = match newname.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };

        let res = self.fs.tx(|tx| {
            tx.link_file(parent_ptr, node_ptr, name)?;
            let node = tx.read_node(node_ptr)?;
            Ok((node_ptr, node))
        });

        match res {
            Ok((node_ptr, node)) => reply.entry(&TTL, &node_attr(node_ptr, &node), 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let parent_ptr = NodePtr(parent);
        let name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };
        let res = self.fs.tx(|tx| tx.unlink_file(parent_ptr, name, true));
        match res {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn rename(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        newparent: u64,
        newname: &std::ffi::OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let old_parent_ptr = NodePtr(parent);
        let old_name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };

        let new_parent_ptr = NodePtr(newparent);
        let new_name = match newname.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EILSEQ),
        };

        let res = self
            .fs
            .tx(|tx| tx.rename_entry(old_parent_ptr, old_name, new_parent_ptr, new_name));

        match res {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn create(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let parent_ptr = NodePtr(parent);

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

        let perms = ((mode & PERMS) & !umask) as u16;

        let res = self.fs.tx(|tx| {
            let node_ptr =
                tx.create_file(parent_ptr, name, file_type, perms, req.uid(), req.gid())?;
            let node = tx.read_node(node_ptr)?;
            Ok((node_ptr, node))
        });

        match res {
            Ok((node_ptr, node)) => reply.created(&TTL, &node_attr(node_ptr, &node), 0, 0, 0),
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
        let node_ptr = NodePtr(ino);
        let mut buf = vec![0u8; size as usize];
        let res = self
            .fs
            .tx(|tx| tx.read_file_at(node_ptr, offset as u64, &mut buf));
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
        let node_ptr = NodePtr(ino);
        let res = self
            .fs
            .tx(|tx| tx.write_file_at(node_ptr, offset as u64, data));
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
        let node_ptr = NodePtr(ino);
        let res = self.fs.tx(|tx| tx.read_dir(node_ptr));
        match res {
            Ok(dir) => {
                for (i, entry) in dir.as_slice().iter().enumerate().skip(offset as usize) {
                    if entry.is_null() {
                        continue;
                    }
                    let name = match entry.name.as_str() {
                        Ok(name) => name,
                        Err(e) => return reply.error(e.into()),
                    };
                    let is_full = reply.add(
                        entry.node_ptr.0,
                        (i + 1) as i64,
                        entry.file_type.into(),
                        name,
                    );
                    if is_full {
                        reply.ok();
                        return;
                    };
                }
                reply.ok();
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        let blocks = self.fs.superblock().block_count;
        let blocks_free = self.fs.allocator().available();

        let nodes = self.fs.superblock().node_count;
        let nodes_free = self.fs.node_map().count_free();

        reply.statfs(
            blocks,
            blocks_free,
            blocks_free,
            nodes,
            nodes_free,
            BLOCK_SIZE as u32,
            NAME_MAX as u32,
            0,
        );
    }
}

fn node_attr(node_ptr: NodePtr, node: &Node) -> FileAttr {
    FileAttr {
        ino: node_ptr.0,
        size: node.size,
        blocks: node.blocks(),
        atime: SystemTime::from(node.access_time),
        mtime: SystemTime::from(node.mod_time),
        ctime: SystemTime::from(node.change_time),
        crtime: SystemTime::from(node.create_time),
        kind: FileType::from(node.file_type),
        perm: node.perms,
        nlink: node.links,
        uid: node.uid,
        gid: node.gid,
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
