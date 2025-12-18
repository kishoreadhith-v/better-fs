// src/storage.rs
use sha2::{Sha256, Digest};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

pub struct Storage {
    root_dir: PathBuf,
}

impl Storage {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let root_dir = path.into();
        // Ensure the storage directory exists (e.g., /tmp/betterfs_data)
        fs::create_dir_all(&root_dir).unwrap();
        Storage { root_dir }
    }

    /// Takes a chunk of bytes, hashes it, and saves it to disk.
    /// Returns the Hash (String) so we can put it in a "Recipe".
    pub fn write_chunk(&self, data: &[u8]) -> String {
        // 1. Calculate SHA-256 Hash
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        let hash_string = hex::encode(result);

        // 2. Determine File Path
        // We split it: "abc12345" -> "root/ab/c12345"
        // This prevents one folder from having 1 million files (which slows down Linux).
        let subdir = self.root_dir.join(&hash_string[0..2]);
        let file_path = subdir.join(&hash_string[2..]);

        // 3. Deduplication Check
        // If file exists, we do NOTHING. We just saved space!
        if file_path.exists() {
            println!("Debug: Deduplicated chunk {}", &hash_string[0..8]);
            return hash_string;
        }

        // 4. Write to Disk
        fs::create_dir_all(&subdir).unwrap();
        let mut file = File::create(&file_path).unwrap();
        file.write_all(data).unwrap();

        println!("Debug: Wrote new chunk {}", &hash_string[0..8]);
        hash_string
    }

    /// Reads a chunk back from disk using its Hash
    pub fn read_chunk(&self, hash: &str) -> Option<Vec<u8>> {
        let subdir = self.root_dir.join(&hash[0..2]);
        let file_path = subdir.join(&hash[2..]);

        if file_path.exists() {
            Some(fs::read(file_path).unwrap())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cas_storage() {
        // Test in a temp folder
        let store = Storage::new("./test_storage_db");
        
        let data1 = b"Hello World";
        let data2 = b"Hello World"; // Same data
        let data3 = b"Different Data";

        // 1. Write first chunk
        let hash1 = store.write_chunk(data1);
        
        // 2. Write duplicate (Should verify it matches hash1)
        let hash2 = store.write_chunk(data2);
        assert_eq!(hash1, hash2, "Hashes must match for identical data");

        // 3. Write different data
        let hash3 = store.write_chunk(data3);
        assert_ne!(hash1, hash3, "Different data must have different hash");

        // 4. Read back
        let loaded = store.read_chunk(&hash1).unwrap();
        assert_eq!(loaded, data1);

        // Cleanup
        fs::remove_dir_all("./test_storage_db").unwrap();
    }
}