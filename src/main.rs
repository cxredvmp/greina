use os_lab_4::hardware::storage::Storage;
use os_lab_4::kernel::Kernel;
use std::io::{self, Write};

fn main() {
    // Initialize a 1 MiB in-memory storage
    let storage_size = 1024 * 1024;
    let storage = Storage::new(storage_size);
    let mut kernel = Kernel::new(storage);

    println!("Filesystem shell opened.");
    println!("Type 'help' for commands.");

    loop {
        // Print prompt
        print!("> ");
        io::stdout().flush().unwrap();

        // Read input
        let mut input = String::new();
        if io::stdin().read_line(&mut input).unwrap() == 0 {
            break;
        }

        // Parse command
        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        let command = parts[0];
        let args = &parts[1..];

        // Execute the command as a system call
        match command {
            "mkfs" => {
                if let Some(n) = args.get(0).and_then(|s| s.parse().ok()) {
                    match kernel.mkfs(n) {
                        Ok(_) => println!("Filesystem formatted with {} nodes.", n),
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Usage: mkfs <node_count>");
                }
            }
            "mount" => match kernel.mount() {
                Ok(_) => println!("Filesystem mounted."),
                Err(e) => println!("Error: {:?}", e),
            },
            "create" => {
                if let Some(path) = args.get(0) {
                    println!("{:?}", kernel.create(path));
                } else {
                    println!("Usage: create <path>");
                }
            }
            "mkdir" => {
                if let Some(path) = args.get(0) {
                    println!("{:?}", kernel.mkdir(path));
                } else {
                    println!("Usage: mkdir <path>");
                }
            }
            "rmdir" => {
                if let Some(path) = args.get(0) {
                    println!("{:?}", kernel.rmdir(path));
                } else {
                    println!("Usage: rmdir <path>");
                }
            }
            "cd" => {
                if let Some(path) = args.get(0) {
                    println!("{:?}", kernel.cd(path));
                } else {
                    println!("Usage: cd <path>");
                }
            }
            "open" => {
                if let Some(path) = args.get(0) {
                    match kernel.open(path) {
                        Ok(fd) => println!("File opened.\nfd: {}", fd),
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Usage: open <path>");
                }
            }
            "close" => {
                if let Some(fd) = args.get(0).and_then(|s| s.parse().ok()) {
                    println!("{:?}", kernel.close(fd));
                } else {
                    println!("Usage: close <fd>");
                }
            }
            "read" => {
                if args.len() >= 2 {
                    let fd = args[0].parse().unwrap_or(usize::MAX);
                    let size = args[1].parse().unwrap_or(0);
                    let mut buf = vec![0u8; size];

                    match kernel.read(fd, &mut buf) {
                        Ok(bytes_read) => {
                            // Try to print as string, otherwise print bytes
                            let output = String::from_utf8_lossy(&buf[..bytes_read]);
                            println!("Read {} bytes: {:?}", bytes_read, output);
                        }
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Usage: read <fd> <size>");
                }
            }
            "write" => {
                if args.len() >= 2 {
                    let fd = args[0].parse().unwrap_or(usize::MAX);
                    // Join the rest of the arguments as data
                    let data = args[1..].join(" ");
                    match kernel.write(fd, data.as_bytes()) {
                        Ok(bytes_written) => println!("Written {} bytes.", bytes_written),
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Usage: write <fd> <data>");
                }
            }
            "seek" => {
                if args.len() >= 2 {
                    let fd = args[0].parse().unwrap_or(usize::MAX);
                    let offset = args[1].parse().unwrap_or(0);
                    println!("{:?}", kernel.seek(fd, offset));
                } else {
                    println!("Usage: seek <fd> <offset>");
                }
            }
            "link" => {
                if args.len() >= 2 {
                    println!("{:?}", kernel.link(args[0], args[1]));
                } else {
                    println!("Usage: link <old_path> <new_path>");
                }
            }
            "unlink" => {
                if let Some(path) = args.get(0) {
                    println!("{:?}", kernel.unlink(path));
                } else {
                    println!("Usage: unlink <path>");
                }
            }
            "symlink" => {
                if args.len() >= 2 {
                    println!("{:?}", kernel.symlink(args[0], args[1]));
                } else {
                    println!("Usage: symlink <target> <path>");
                }
            }
            "truncate" => {
                if args.len() >= 2 {
                    let path = args[0];
                    let size = args[1].parse().unwrap_or(0);
                    println!("{:?}", kernel.truncate(path, size));
                } else {
                    println!("Usage: truncate <path> <size>");
                }
            }
            "stat" => {
                if let Some(path) = args.get(0) {
                    match kernel.stat(path) {
                        Ok(stats) => {
                            println!("File: {}", path);
                            println!("Type: {:?}", stats.filetype);
                            println!("Size: {}", stats.size);
                            println!("Links: {}", stats.link_count);
                            println!("Blocks: {}", stats.block_count);
                            println!("Node id: {}", stats.node_id);
                        }
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Usage: stat <path>");
                }
            }
            "ls" => {
                let path = args.get(0).copied().unwrap_or(".");
                match kernel.ls(path) {
                    Ok(list) => {
                        for (name, node) in list {
                            println!("{} {}", node, name);
                        }
                    }
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            "clear" => {
                print!("\x1b[2J\x1b[1;1H");
            }
            "exit" => break,
            "help" => {
                println!("COMMANDS");
                let commands = [
                    ("mkfs <nodes>", "format filesystem"),
                    ("mount", "mount filesystem"),
                    ("create <path>", "create a file"),
                    ("mkdir <path>", "create a directory"),
                    ("rmdir <path>", "remove a directory"),
                    ("cd <path>", "change current directory"),
                    ("open <path>", "open file"),
                    ("close <fd>", "close file"),
                    ("read <fd> <size>", "read bytes from file"),
                    ("write <fd> <string>", "write string to file"),
                    ("seek <fd> <offset>", "seek to offset"),
                    ("link <old> <new>", "create hard link"),
                    ("unlink <path>", "remove file/link"),
                    ("symlink <target> <path>", "create symbolic link"),
                    ("truncate <path> <size>", "resize file"),
                    ("stat <path>", "display file stats"),
                    ("ls [path]", "list directory"),
                    ("clear", "clear the screen"),
                    ("exit", "exit the shell"),
                ];
                for (cmd, desc) in commands {
                    println!("  {:<25} {}", cmd, desc);
                }
            }
            _ => println!("Unknown command: {}", command),
        }
    }
}
