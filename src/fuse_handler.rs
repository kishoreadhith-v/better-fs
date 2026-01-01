// src/fuse_handler.rs
use crate::file_manager::FileManager;
use crate::file_manager::FileKind;
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
use std::time::{ Duration, UNIX_EPOCH, SystemTime };
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
    inode_map: HashMap<u64, String>,
}

impl BetterFS {
    pub fn new(manager: FileManager) -> Self {
        let mut inode_map = HashMap::new();

        // 1. Initialize Root (Inode 1 is empty path "")
        inode_map.insert(1, "".to_string());

        // 2. HYDRATION: Scan DB to restore memory of existing files
        println!("FUSE: Rebuilding Inode Map...");
        let all_files = manager.list_files();
        for filename in all_files {
            let inode = calculate_inode(&filename);
            inode_map.insert(inode, filename);
        }
        println!("FUSE: Restored {} inodes.", inode_map.len());

        BetterFS {
            manager,
            open_files: HashMap::new(),
            inode_map,
        }
    }
}

impl Filesystem for BetterFS {
    // 1. LOOKUP
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_str().unwrap();

        // A. Resolve Parent Path (The "Nesting" Fix)
        let parent_path = match self.inode_map.get(&parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // B. Build Full Path (e.g. "my_folder" + "/" + "inside.png")
        let full_path = if parent_path.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        // C. Calculate Inode for THIS specific file
        let inode = calculate_inode(&full_path);

        // 1. Check RAM Buffer (Is it open?)
        if let Some(buffer) = self.open_files.get(&inode) {
            let size = buffer.data.len() as u64;
            let attr = FileAttr {
                ino: inode,
                size,
                blocks: (size + 511) / 512,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::now(),
                kind: FileType::RegularFile, // Open buffers are usually files
                perm: 0o644,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            // CRITICAL: Memorize this path
            self.inode_map.insert(inode, full_path);
            return reply.entry(&TTL, &attr, 0);
        }

        // 2. Check Backend (Database)
        if let Some((size, kind)) = self.manager.get_file_metadata(&full_path) {
            // CRITICAL: Memorize this path so we can find it again later!
            self.inode_map.insert(inode, full_path);

            let (file_type, perm) = match kind {
                FileKind::File => (FileType::RegularFile, 0o644),
                FileKind::Directory => (FileType::Directory, 0o755),
            };

            let attr = FileAttr {
                ino: inode,
                size,
                blocks: (size + 511) / 512,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::now(),
                kind: file_type,
                perm,
                nlink: 1,
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

    // 2. GETATTR
    // src/fuse_handler.rs -> getattr

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        // 1. Resolve Inode to Path
        let filename = match self.inode_map.get(&ino) {
            Some(name) => name.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // 2. Check RAM Buffer (Files being written)
        if let Some(buffer) = self.open_files.get(&ino) {
            let attr = FileAttr {
                ino,
                size: buffer.data.len() as u64,
                blocks: ((buffer.data.len() as u64) + 511) / 512,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::now(),
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
            return;
        }

        // ===================================================================
        // 3. SPECIAL CASE: ROOT DIRECTORY (The Fix)
        // The Root path is "" and usually not stored in the DB.
        // We must handle it manually.
        // ===================================================================
        if ino == 1 {
            let attr = FileAttr {
                ino: 1,
                size: 0,
                blocks: 0,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::now(),
                kind: FileType::Directory, // Root is always a Directory
                perm: 0o755,
                nlink: 2,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            reply.attr(&TTL, &attr);
            return;
        }

        // 4. Check Backend (Database)
        if let Some((size, kind)) = self.manager.get_file_metadata(&filename) {
            let (file_type, perm) = match kind {
                FileKind::File => (FileType::RegularFile, 0o644),
                FileKind::Directory => (FileType::Directory, 0o755),
            };

            let attr = FileAttr {
                ino,
                size,
                blocks: (size + 511) / 512,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::now(),
                kind: file_type,
                perm,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            reply.attr(&TTL, &attr);
        } else {
            // If it's not in RAM, not Root, and not in DB -> It doesn't exist.
            reply.error(ENOENT);
        }
    }

    // 3. READDIR
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory
    ) {
        // 1. Get the path (CLONE IT to satisfy borrow checker)
        // We clone() so we own the string and stop borrowing 'self.inode_map'
        let dir_path = match self.inode_map.get(&ino) {
            Some(p) => p.clone(), // <--- FIX: Clone the string here
            None => {
                return reply.error(ENOENT);
            }
        };

        if offset == 0 {
            reply.add(1, 0, FileType::Directory, ".");
            reply.add(1, 1, FileType::Directory, "..");

            let all_files = self.manager.list_files();

            for filename in all_files {
                if filename == dir_path {
                    continue;
                }

                // Logic to check if 'filename' is a direct child of 'dir_path'
                let is_child = if dir_path.is_empty() {
                    !filename.contains('/')
                } else {
                    if
                        filename.starts_with(&dir_path) &&
                        filename.chars().nth(dir_path.len()) == Some('/')
                    {
                        let relative_part = &filename[dir_path.len() + 1..];
                        !relative_part.contains('/')
                    } else {
                        false
                    }
                };

                if is_child {
                    let child_inode = calculate_inode(&filename);

                    // We can safely unwrap because we know the file exists in the list
                    if let Some((_size, kind)) = self.manager.get_file_metadata(&filename) {
                        let file_type = match kind {
                            FileKind::File => FileType::RegularFile,
                            FileKind::Directory => FileType::Directory,
                        };

                        let name_only = filename.split('/').last().unwrap();
                        let _ = reply.add(child_inode, offset + 1, file_type, name_only);

                        // NOW this works because 'dir_path' is a clone, not a borrow
                        self.inode_map.insert(child_inode, filename);
                    }
                }
            }
        }
        reply.ok();
    }

    // 4. READ (Optimized with Inode Map)
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
        // 1. Check RAM Buffer
        if let Some(buffer) = self.open_files.get(&ino) {
            let start = offset as usize;
            if start < buffer.data.len() {
                let end = std::cmp::min(start + (_size as usize), buffer.data.len());
                reply.data(&buffer.data[start..end]);
            } else {
                reply.data(&[]);
            }
            return;
        }

        // 2. Check Backend using MAP (Fast!)
        if let Some(filename) = self.inode_map.get(&ino) {
            match self.manager.read_file(filename) {
                Ok(data) => {
                    let start = offset as usize;
                    if start < data.len() {
                        let end = std::cmp::min(start + (_size as usize), data.len());
                        reply.data(&data[start..end]);
                    } else {
                        reply.data(&[]);
                    }
                }
                Err(_) => reply.error(libc::EIO),
            }
        } else {
            reply.error(ENOENT);
        }
    }

    // =======================================================================
    // NEW: WRITE SUPPORT (FIXED)
    // =======================================================================

    // 5. CREATE (Supports Nesting)
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
        let name_str = name.to_str().unwrap();

        // 1. Resolve Parent
        let parent_path = match self.inode_map.get(&parent) {
            Some(p) => p,
            None => {
                return reply.error(ENOENT);
            }
        };

        // 2. Build Full Path
        let full_path = if parent_path.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        let inode = calculate_inode(&full_path);

        // 3. Initialize Buffer
        let buffer = WriteBuffer {
            filename: full_path.clone(),
            data: Vec::new(),
        };
        self.open_files.insert(inode, buffer);

        // 4. Update Map immediately
        self.inode_map.insert(inode, full_path);

        let attr = FileAttr {
            ino: inode,
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
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

    // 9. UNLINK (Fix: Resolve path from parent)
    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap();

        // 1. Resolve Parent
        let parent_path = match self.inode_map.get(&parent) {
            Some(p) => p,
            None => {
                return reply.error(ENOENT);
            }
        };

        // 2. Build Full Path
        let full_path = if parent_path.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        // 3. Delete from Backend
        if let Ok(_) = self.manager.delete_file(&full_path) {
            let inode = calculate_inode(&full_path);

            // 4. Clean up Memory
            self.open_files.remove(&inode);
            self.inode_map.remove(&inode);

            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    // 10. RENAME (Fix: Resolve both paths)
    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty
    ) {
        let name_str = name.to_str().unwrap();
        let new_name_str = newname.to_str().unwrap();

        // 1. Resolve Old Path
        let parent_path = match self.inode_map.get(&parent) {
            Some(p) => p,
            None => {
                return reply.error(ENOENT);
            }
        };
        let old_path = if parent_path.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        // 2. Resolve New Path
        let new_parent_path = match self.inode_map.get(&newparent) {
            Some(p) => p,
            None => {
                return reply.error(ENOENT);
            }
        };
        let new_path = if new_parent_path.is_empty() {
            new_name_str.to_string()
        } else {
            format!("{}/{}", new_parent_path, new_name_str)
        };

        // 3. Rename in Backend
        if let Ok(_) = self.manager.rename_file(&old_path, &new_path) {
            // 4. Update Maps
            let old_inode = calculate_inode(&old_path);
            let new_inode = calculate_inode(&new_path);

            // If it was open, move the buffer
            if let Some(mut buffer) = self.open_files.remove(&old_inode) {
                buffer.filename = new_path.clone();
                self.open_files.insert(new_inode, buffer);
            }

            // Update Inode Map
            self.inode_map.remove(&old_inode);
            self.inode_map.insert(new_inode, new_path);

            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    // 11. OPEN (Optimized with Inode Map)
    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        let is_read_only = (flags & libc::O_ACCMODE) == libc::O_RDONLY;
        if is_read_only {
            reply.opened(0, 0);
            return;
        }

        if self.open_files.contains_key(&ino) {
            reply.opened(0, 0);
            return;
        }

        // Use Map instead of listing all files
        if let Some(filename) = self.inode_map.get(&ino) {
            if let Ok(data) = self.manager.read_file(filename) {
                let buffer = WriteBuffer {
                    filename: filename.clone(),
                    data: data,
                };
                self.open_files.insert(ino, buffer);
                reply.opened(0, 0);
                return;
            }
        }
        reply.opened(0, 0);
    }

    // 12. MKDIR (Supports Nesting)
    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry
    ) {
        let name_str = name.to_str().unwrap();

        // 1. Resolve Parent
        let parent_path = match self.inode_map.get(&parent) {
            Some(p) => p,
            None => {
                return reply.error(ENOENT);
            }
        };

        // 2. Build Full Path
        let full_path = if parent_path.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        // 3. Create
        if let Ok(_) = self.manager.create_directory(&full_path) {
            let inode = calculate_inode(&full_path);

            // 4. Update Map
            self.inode_map.insert(inode, full_path);

            let attr = FileAttr {
                ino: inode,
                size: 0,
                blocks: 0,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::now(),
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

    // 13. RMDIR
    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap();

        // 1. Resolve Parent Path
        let parent_path = match self.inode_map.get(&parent) {
            Some(p) => p,
            None => {
                return reply.error(ENOENT);
            }
        };

        // 2. Build Full Path
        let full_path = if parent_path.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        // 3. Remove from Database
        // Note: Real filesystems check if the directory is empty first.
        // We are skipping that check for simplicity (allowing "force" delete).
        if let Ok(_) = self.manager.delete_file(&full_path) {
            let inode = calculate_inode(&full_path);

            // 4. Clean up Memory
            self.inode_map.remove(&inode);

            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }
}
