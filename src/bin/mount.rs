use fuser::{MountOption, spawn_mount2};
use greina::{block::storage::file::FileStorage, fs::Filesystem, fuse::Fuse};

fn usage() -> ! {
    eprintln!("mount.greina device mountpoint");
    std::process::exit(1);
}

fn main() {
    env_logger::init();

    let mut storage_path = None;
    let mut mount_point = None;
    let args = std::env::args().skip(1);
    for arg in args {
        if storage_path.is_none() {
            storage_path = Some(arg);
        } else if mount_point.is_none() {
            mount_point = Some(arg);
        } else {
            eprintln!("mount.greina: too many arguments");
            usage();
        }
    }

    let storage_path = if let Some(path) = storage_path {
        path
    } else {
        eprintln!("mount.greina: no device specified");
        std::process::exit(1);
    };

    let mount_point = if let Some(point) = mount_point {
        point
    } else {
        eprintln!("mount.greina: no mountpoint specified");
        std::process::exit(1);
    };

    let storage = match FileStorage::open(&storage_path) {
        Ok(storage) => storage,
        Err(e) => {
            eprintln!(
                "mount.greina: failed to open device {}: {}",
                &storage_path,
                std::io::Error::from_raw_os_error(e)
            );
            std::process::exit(1);
        }
    };

    let fs = match Filesystem::mount(storage) {
        Ok(fs) => fs,
        Err(e) => {
            eprintln!(
                "mount.greina: failed to read filesystem from device {}: {}",
                &storage_path,
                std::io::Error::from_raw_os_error(e)
            );
            std::process::exit(1);
        }
    };

    let fuse = Fuse::new(fs);

    let opts = vec![
        MountOption::DefaultPermissions,
        MountOption::FSName("greina".to_string()),
    ];

    let session = match spawn_mount2(fuse, &mount_point, &opts) {
        Ok(session) => {
            eprintln!(
                "mount.greina: mounted filesystem from device {} on mountpoint {}",
                &storage_path, &mount_point
            );
            session
        }
        Err(e) => {
            eprintln!(
                "mount.greina: failed to mount filesystem from device {}: {}",
                &storage_path, e
            );
            std::process::exit(1);
        }
    };

    eprintln!("mount.greina: to unmount press Ctrl+C");

    // Create a signal set with SIGINT
    let mut sigset: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::sigemptyset(&mut sigset);
        libc::sigaddset(&mut sigset, libc::SIGINT);
        libc::pthread_sigmask(libc::SIG_BLOCK, &sigset, std::ptr::null_mut());
    }

    // Wait for the SIGINT signal
    let mut sig = 0;
    unsafe { libc::sigwait(&sigset, &mut sig) };

    drop(session);
}
