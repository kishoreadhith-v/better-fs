// src/fuse_handler.rs
use crate::file_manager::FileManager;
use fuser::{
    FileAttr,
    FileType,
    Filesystem,
    ReplyAttr,
    ReplyData,
    ReplyDirectory,
    ReplyEntry,
    ReplyWrite,
    ReplyCreate,
    ReplyEmpty,
    ReplyOpen,
    Request,
};
use libc::ENOENT; // Removed EIO as it was unused
use std::ffi::OsStr;
use std::time::{ Duration, UNIX_EPOCH };
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{ Hash, Hasher };

const TTL: Duration = Duration::from_secs(1);

// HELPER: Turn a String ("file.txt") into a Number (Inode)
fn calculate_inode(filename: &str) -> u64 {
    let mut s = DefaultHasher::new();
    filename.hash(&mut s);
    s.finish()
}

// Struct to hold a file being written in RAM
struct WriteBuffer {
    filename: String,
    data: Vec<u8>,
}

pub struct BetterFS {
    pub manager: FileManager,
    // Memory buffer for open files: Inode -> Data
    open_files: HashMap<u64, WriteBuffer>,
}

impl BetterFS {
    pub fn new(manager: FileManager) -> Self {
        BetterFS {
            manager,
            open_files: HashMap::new(),
        }
    }
}

impl Filesystem for BetterFS {
    // 1. LOOKUP (Existing logic)
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let filename = name.to_str().unwrap();
        if parent == 1 && (filename == "." || filename == "..") {
            // skip logic handled by readdir usually
        }

        let inode = calculate_inode(filename);

        // 1. Check RAM Buffer first (Is it an open file being written?)
        if let Some(buffer) = self.open_files.get(&inode) {
            let size = buffer.data.len() as u64;
            let attr = FileAttr {
                ino: inode,
                size,
                blocks: (size + 511) / 512,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::RegularFile, // Buffers are always files
                perm: 0o644,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            return reply.entry(&TTL, &attr, 0);
        }

        // 2. Check Backend (Database)
        // We now get both Size AND Kind (File vs Directory)
        if let Some((size, kind)) = self.manager.get_file_metadata(filename) {
            // Map our specific 'FileKind' to FUSE 'FileType'
            let (file_type, perm) = match kind {
                crate::file_manager::FileKind::File => (FileType::RegularFile, 0o644),
                crate::file_manager::FileKind::Directory => (FileType::Directory, 0o755),
            };

            let attr = FileAttr {
                ino: inode,
                size,
                blocks: (size + 511) / 512,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: file_type, // <--- Dynamic type
                perm, // <--- Dynamic permissions (755 for dirs, 644 for files)
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            reply.entry(&TTL, &attr, 0);
        } else {
            // Not in Buffer, Not in DB -> Doesn't exist
            reply.error(ENOENT);
        }
    }

    // 2. GETATTR (Existing logic)
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if ino == 1 {
            let attr = FileAttr {
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
                flags: 0,
                blksize: 512,
            };
            reply.attr(&TTL, &attr);
        } else {
            // Assume file exists for now to support 'write' flows
            let attr = FileAttr {
                ino: ino,
                size: 0,
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
                flags: 0,
                blksize: 512,
            };
            reply.attr(&TTL, &attr);
        }
    }

    // 3. READDIR (Existing logic)
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory
    ) {
        // Limitation: For now, we only support listing the Root Directory (Inode 1)
        // Implementing full recursive directory listing requires a bigger architectural change (InodeMap).
        if ino != 1 {
            return reply.error(ENOENT);
        }

        let mut entries = vec![
            (1, FileType::Directory, ".".to_string()),
            (1, FileType::Directory, "..".to_string())
        ];

        // 1. Add Backend Files (With Correct Types!)
        for filename in self.manager.list_files() {
            // Ask the manager: "Is this a file or a folder?"
            let kind = if let Some((_, k)) = self.manager.get_file_metadata(&filename) {
                match k {
                    crate::file_manager::FileKind::Directory => FileType::Directory,
                    _ => FileType::RegularFile,
                }
            } else {
                FileType::RegularFile
            };

            entries.push((calculate_inode(&filename), kind, filename));
        }

        // 2. Add files currently being written (RAM Buffers are always files)
        for buffer in self.open_files.values() {
            entries.push((
                calculate_inode(&buffer.filename),
                FileType::RegularFile,
                buffer.filename.clone(),
            ));
        }

        // Standard FUSE pagination
        for (i, entry) in entries
            .into_iter()
            .enumerate()
            .skip(offset as usize) {
            if reply.add(entry.0, offset + (i as i64) + 1, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }
    // 4. READ (Existing logic)
    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData
    ) {
        if let Some(buffer) = self.open_files.get(&ino) {
            if (offset as usize) >= buffer.data.len() {
                return reply.data(&[]);
            }
            return reply.data(&buffer.data[offset as usize..]);
        }

        let all_files = self.manager.list_files();
        if let Some(filename) = all_files.into_iter().find(|n| calculate_inode(n) == ino) {
            if let Ok(data) = self.manager.read_file(&filename) {
                if (offset as usize) >= data.len() {
                    reply.data(&[]);
                } else {
                    reply.data(&data[offset as usize..]);
                }
                return;
            }
        }
        reply.error(ENOENT);
    }

    // =======================================================================
    // NEW: WRITE SUPPORT (FIXED)
    // =======================================================================

    // 5. CREATE: Fixed argument count (added _umask)
    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate
    ) {
        if parent != 1 {
            return reply.error(ENOENT);
        }

        let filename = name.to_str().unwrap().to_string();
        let inode = calculate_inode(&filename);

        let buffer = WriteBuffer {
            filename: filename.clone(),
            data: Vec::new(),
        };
        self.open_files.insert(inode, buffer);

        let attr = FileAttr {
            ino: inode,
            size: 0,
            blocks: 0,
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
            flags: 0,
            blksize: 512,
        };
        reply.created(&TTL, &attr, 0, 0, 0);
        println!("FUSE: Created file '{}'", filename);
    }

    // 6. WRITE
    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite
    ) {
        if let Some(buffer) = self.open_files.get_mut(&ino) {
            let end = (offset as usize) + data.len();
            if end > buffer.data.len() {
                buffer.data.resize(end, 0);
            }
            buffer.data[offset as usize..end].copy_from_slice(data);
            reply.written(data.len() as u32);
        } else {
            reply.error(ENOENT);
        }
    }

    // 7. SETATTR
    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr
    ) {
        if let Some(new_size) = size {
            if let Some(buffer) = self.open_files.get_mut(&ino) {
                buffer.data.resize(new_size as usize, 0);
            }
        }
        self.getattr(_req, ino, reply);
    }

    // 8. RELEASE: Fixed return type (ReplyEmpty)
    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty
    ) {
        if let Some(buffer) = self.open_files.remove(&ino) {
            println!("FUSE: Flushing '{}' to Storage...", buffer.filename);
            let _ = self.manager.write_file(&buffer.filename, &buffer.data);
        }
        reply.ok();
    }

    // 9. UNLINK: "User typed 'rm file.txt'"
    fn unlink(&mut self, _req: &Request, _parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let filename = name.to_str().unwrap();

        // Remove from Backend
        if let Ok(_) = self.manager.delete_file(filename) {
            // Also remove from RAM buffer if it was open
            let inode = calculate_inode(filename);
            self.open_files.remove(&inode);
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    // 10. RENAME: "User typed 'mv old.txt new.txt'"
    fn rename(
        &mut self,
        _req: &Request,
        _parent: u64,
        name: &OsStr,
        _newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty
    ) {
        let old_filename = name.to_str().unwrap();
        let new_filename = newname.to_str().unwrap();

        // 1. Rename in Backend
        if let Ok(_) = self.manager.rename_file(old_filename, new_filename) {
            // 2. Handle RAM Buffers (if the file was currently open/being written)
            let old_inode = calculate_inode(old_filename);
            if let Some(mut buffer) = self.open_files.remove(&old_inode) {
                // Update the buffer's name and re-insert under new inode
                buffer.filename = new_filename.to_string();
                let new_inode = calculate_inode(new_filename);
                self.open_files.insert(new_inode, buffer);
            }

            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    // 11. OPEN: Called when opening an existing file (Critical for Append >>)
    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        // 1. If it's already in the RAM buffer, we are good.
        if self.open_files.contains_key(&ino) {
            reply.opened(0, 0);
            return;
        }

        // 2. If not, we must load it from the Backend (Storage)
        let all_files = self.manager.list_files();

        // (Scan to find the filename matching this Inode)
        if let Some(filename) = all_files.into_iter().find(|n| calculate_inode(n) == ino) {
            // Read the data from disk
            if let Ok(data) = self.manager.read_file(&filename) {
                // Create a new RAM buffer with the existing data
                let buffer = WriteBuffer {
                    filename: filename,
                    data: data,
                };
                self.open_files.insert(ino, buffer);

                reply.opened(0, 0);
                return;
            }
        }

        reply.error(ENOENT);
    }

    // 12. MKDIR
    fn mkdir(
        &mut self,
        _req: &Request,
        _parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry
    ) {
        let filename = name.to_str().unwrap();

        if let Ok(_) = self.manager.create_directory(filename) {
            let inode = calculate_inode(filename);
            let attr = FileAttr {
                ino: inode,
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
                flags: 0,
                blksize: 512,
            };
            reply.entry(&TTL, &attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }
}
