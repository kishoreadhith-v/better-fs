use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::ENOENT;
use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

mod chunker;
mod storage;
mod file_manager;

// Constants
const TTL: Duration = Duration::from_secs(1); // Cache time
const HELLO_TXT_CONTENT: &str = "Hello! This file lives in Rust memory, not on disk!\nThis is a test for betterfs.\n";

struct HelloFS;

impl Filesystem for HelloFS {
    // 1. LOOKUP: The OS asks "Does 'hello.txt' exist?"
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == 1 && name.to_str() == Some("hello.txt") {
            reply.entry(&TTL, &FILE_ATTR, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    // 2. GETATTR: The OS asks "What are the permissions/size of inode 1 (root) or 2 (file)?"
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match ino {
            1 => reply.attr(&TTL, &DIR_ATTR),
            2 => reply.attr(&TTL, &FILE_ATTR),
            _ => reply.error(ENOENT),
        }
    }

    // 3. READDIR: The OS asks "List files in the root folder"
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(ENOENT);
            return;
        }

        let entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
            (2, FileType::RegularFile, "hello.txt"),
        ];

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // reply.add(inode, offset, type, name)
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }

    // 4. READ: The OS asks "Give me the bytes for hello.txt"
    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if ino == 2 {
            let data = HELLO_TXT_CONTENT.as_bytes();
            if offset >= data.len() as i64 {
                reply.data(&[]);
            } else {
                reply.data(&data[offset as usize..]);
            }
        } else {
            reply.error(ENOENT);
        }
    }
}

fn main() {
    let mountpoint = "/tmp/betterfs";
    // Create the mount directory if it doesn't exist
    std::fs::create_dir_all(mountpoint).unwrap();
    
    // Mount options
    let options = vec![
        MountOption::RO, // Read-Only for now
        MountOption::FSName("betterfs".to_string()),
    ];

    println!("Mounting filesystem to {}...", mountpoint);
    println!("Run 'cat {}/hello.txt' in another terminal to test.", mountpoint);
    
    // Start the FUSE loop (this blocks until unmounted)
    fuser::mount2(HelloFS, mountpoint, &options).unwrap();
}

// --- HARDCODED ATTRIBUTES FOR DEMO ---
const DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH,
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 1000,
    gid: 1000,
    rdev: 0,
    blksize: 512,
    flags: 0,
};

const FILE_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 56, // Length of hello string
    blocks: 1,
    atime: UNIX_EPOCH,
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 1000,
    gid: 1000,
    rdev: 0,
    blksize: 512,
    flags: 0,
};