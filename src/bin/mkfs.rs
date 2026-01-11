use greina::{
    block::storage::{Storage, file::FileStorage},
    fs::Filesystem,
};

fn usage() -> ! {
    eprintln!("mkfs.greina device");
    std::process::exit(1);
}

fn main() {
    let mut storage_path = None;
    let args = std::env::args().skip(1);
    for arg in args {
        if storage_path.is_none() {
            storage_path = Some(arg);
        } else {
            eprintln!("mkfs.greina: too many arguments");
            usage();
        }
    }

    let storage_path = if let Some(path) = storage_path {
        path
    } else {
        eprintln!("mkfs.greina: no device specified");
        std::process::exit(1);
    };

    let mut storage = match FileStorage::open(&storage_path) {
        Ok(storage) => storage,
        Err(e) => {
            eprintln!(
                "mkfs.greina: failed to open device {}: {}",
                storage_path,
                std::io::Error::from_raw_os_error(e)
            );
            std::process::exit(1);
        }
    };

    let block_count = storage
        .capacity()
        .expect("must be able to count the number of blocks");
    // 1 node per 4 blocks = 512 bytes / 16384 bytes = 3,125 % of space for nodes
    let node_count = block_count / 4;

    match Filesystem::create(storage, node_count) {
        Ok(fs) => {
            eprintln!(
                "mkfs.greina: created filesystem on {} with {} blocks and {} nodes",
                storage_path,
                fs.superblock().block_count,
                fs.superblock().node_count
            );
        }
        Err(e) => {
            eprintln!(
                "mkfs.greina: failed to create filesystem on {}: {}",
                storage_path,
                std::io::Error::from_raw_os_error(e.into())
            );
            std::process::exit(1);
        }
    }
}
