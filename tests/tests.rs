use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, symlink};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

const MKFS_BIN: &str = env!("CARGO_BIN_EXE_mkfs");
const MOUNT_BIN: &str = env!("CARGO_BIN_EXE_mount");

struct MountedContext {
    mount_process: Child,
    _mount_dir: TempDir,
    _storage_dir: TempDir,
    mount_path: PathBuf,
}

impl MountedContext {
    fn new() -> Self {
        let storage_dir = tempfile::tempdir().expect("failed to create storage dir");
        let storage_path = storage_dir.path().join("disk.img");

        let file = File::create(&storage_path).expect("failed to create storage file");
        file.set_len(16 * 1024 * 1024)
            .expect("failed to set length");
        drop(file);

        let status = Command::new(MKFS_BIN)
            .arg(&storage_path)
            .status()
            .expect("failed to run mkfs");
        assert!(status.success(), "mkfs failed");

        let mount_dir = tempfile::tempdir().expect("failed to create mount dir");
        let mount_path = mount_dir.path().to_owned();

        let mut child = Command::new(MOUNT_BIN)
            .arg(&storage_path)
            .arg(&mount_path)
            .spawn()
            .expect("failed to run mount");

        let mut mounted = false;
        for _ in 0..10 {
            if let Ok(metadata) = fs::metadata(&mount_path) {
                let parent = mount_path.parent().unwrap();
                if let Ok(parent_metadata) = fs::metadata(parent) {
                    if metadata.dev() != parent_metadata.dev() {
                        mounted = true;
                        break;
                    }
                }
            }

            if let Ok(Some(status)) = child.try_wait() {
                panic!("mount process exited prematurely with {}", status);
            }

            thread::sleep(Duration::from_millis(500));
        }

        if !mounted {
            panic!("timed out waiting for filesystem to mount");
        }

        Self {
            mount_process: child,
            _mount_dir: mount_dir,
            _storage_dir: storage_dir,
            mount_path,
        }
    }
}

impl Drop for MountedContext {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.mount_process.id() as i32, libc::SIGINT);
        }
        let _ = self.mount_process.wait();
    }
}

#[test]
fn test_mkfs_and_mount() {
    let _ctx = MountedContext::new();
}

#[test]
fn test_file() {
    let ctx = MountedContext::new();
    let root = &ctx.mount_path;
    let file_path = root.join("hello.txt");

    {
        let mut file = File::create(&file_path).expect("failed to create file");
        file.write_all(b"Hello from Greina!")
            .expect("failed to write to file");
    }

    let content = fs::read_to_string(&file_path).expect("failed to read file");
    assert_eq!(content, "Hello from Greina!");

    fs::remove_file(&file_path).expect("failed to remove file");
    assert!(!file_path.exists());
}

#[test]
fn test_dir() {
    let ctx = MountedContext::new();
    let root = &ctx.mount_path;
    let dir_path = root.join("foo");

    fs::create_dir(&dir_path).expect("failed to create directory");
    assert!(dir_path.exists());
    assert!(dir_path.is_dir());

    let file_path = dir_path.join("hello.txt");
    {
        let mut file = File::create(&file_path).expect("failed to create nested file");
        file.write_all(b"Hello from Greina!")
            .expect("failed to write to nested file");
    }
    assert!(file_path.exists());

    let content = fs::read_to_string(&file_path).expect("failed to read nested file");
    assert_eq!(content, "Hello from Greina!");

    fs::remove_file(&file_path).expect("failed to remove nested file");
    fs::remove_dir(&dir_path).expect("failed to remove directory");
    assert!(!dir_path.exists());
}

#[test]
fn test_symlink() {
    let ctx = MountedContext::new();
    let root = &ctx.mount_path;
    let target_path = root.join("target.txt");
    let link_path = root.join("link.txt");

    {
        let mut file = File::create(&target_path).expect("failed to create target file");
        file.write_all(b"Hello from Greina!")
            .expect("failed to write to target file");
    }

    symlink(&target_path, &link_path).expect("failed to create symlink");
    assert!(link_path.exists());
    assert!(fs::symlink_metadata(&link_path).unwrap().is_symlink());
    let content = fs::read_to_string(&link_path).expect("failed to read through symlink");
    assert_eq!(content, "Hello from Greina!");

    fs::remove_file(&link_path).expect("failed to remove symlink");
    fs::remove_file(&target_path).expect("failed to remove target file");
}
