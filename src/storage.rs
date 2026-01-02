// src/storage.rs
use sha2::{ Sha256, Digest };
use std::fs::{ self, File };
use std::io::{ Read, Write };
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

    /// Takes a chunk of bytes, hashes it, COMPRESSES it, and saves it to disk.
    pub fn write_chunk(&self, data: &[u8]) -> Result<String, std::io::Error> {
        // 1. Calculate SHA-256 Hash of RAW data
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        let hash_string = hex::encode(result);

        // 2. Determine File Path
        let subdir = self.root_dir.join("cas").join(&hash_string[0..2]); // Added "cas" subfolder for cleanliness
        let file_path = subdir.join(&hash_string[2..]);

        // 3. Deduplication Check
        if file_path.exists() {
            // println!("Debug: Deduplicated chunk {}", &hash_string[0..8]);
            return Ok(hash_string);
        }

        // 4. Compress the data (Level 3 is default)
        let compressed_data = zstd::encode_all(data, 3)?;

        // 5. Write to Disk
        fs::create_dir_all(&subdir)?;
        let mut file = File::create(&file_path)?;
        file.write_all(&compressed_data)?;

        // println!("Debug: Wrote new chunk {}", &hash_string[0..8]);
        Ok(hash_string)
    }

    /// Reads a chunk, DECOMPRESSES it, and returns raw bytes
    pub fn read_chunk(&self, hash: &str) -> Result<Vec<u8>, std::io::Error> {
        let subdir = self.root_dir.join("cas").join(&hash[0..2]);
        let file_path = subdir.join(&hash[2..]);

        if !file_path.exists() {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Chunk not found"));
        }

        // Read compressed file
        let mut file = File::open(file_path)?;
        let mut compressed_data = Vec::new();
        file.read_to_end(&mut compressed_data)?;

        // Decompress
        let raw_data = zstd::decode_all(&compressed_data[..])?;
        Ok(raw_data)
    }

    pub fn list_all_chunks(&self) -> Result<Vec<String>, std::io::Error> {
        let mut chunks = Vec::new();
        let cas_dir = self.root_dir.join("cas");
        
        if !cas_dir.exists() {
            return Ok(chunks);
        }

        // Iterate over subdirectories (e.g., "a1", "b2")
        for entry in fs::read_dir(cas_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                // The folder name is the first 2 chars of hash
                let prefix = entry.file_name().into_string().unwrap();
                
                // Iterate over files inside (the rest of the hash)
                for file_entry in fs::read_dir(path)? {
                    let file_entry = file_entry?;
                    let suffix = file_entry.file_name().into_string().unwrap();
                    
                    // Reconstruct full hash
                    chunks.push(format!("{}{}", prefix, suffix));
                }
            }
        }
        Ok(chunks)
    }

    pub fn delete_chunk(&self, hash: &str) -> Result<(), std::io::Error> {
        let subdir = self.root_dir.join("cas").join(&hash[0..2]);
        let file_path = subdir.join(&hash[2..]);
        if file_path.exists() {
            fs::remove_file(file_path)?;
        }
        // Optional: Remove subdir if empty
        let _ = fs::remove_dir(subdir); 
        Ok(())
    }
}


// src/storage.rs (Replace the bottom testing section)

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_cas_storage() {
        // Test in a temp folder
        let test_dir = "./test_storage_db";
        if Path::new(test_dir).exists() {
            fs::remove_dir_all(test_dir).unwrap();
        }
        let store = Storage::new(test_dir);
        
        let data1 = b"Hello World";
        let data2 = b"Hello World"; // Same data
        let data3 = b"Different Data";

        // 1. Write first chunk (FIX: Added .unwrap())
        let hash1 = store.write_chunk(data1).expect("Write failed");
        
        // 2. Write duplicate (FIX: Added .unwrap())
        let hash2 = store.write_chunk(data2).expect("Write failed");
        assert_eq!(hash1, hash2, "Hashes must match for identical data");

        // 3. Write different data (FIX: Added .unwrap())
        let hash3 = store.write_chunk(data3).expect("Write failed");
        assert_ne!(hash1, hash3, "Different data must have different hash");

        // 4. Read back (FIX: No change needed here if hash1 is now a String)
        let loaded = store.read_chunk(&hash1).expect("Read failed");
        assert_eq!(loaded, data1);

        // Cleanup
        fs::remove_dir_all(test_dir).unwrap();
    }

    #[test]
    fn test_compression_cycle() {
        // 1. Setup
        let test_dir = "./test_storage_zstd";
        if std::path::Path::new(test_dir).exists() {
            fs::remove_dir_all(test_dir).unwrap();
        }
        let store = Storage::new(test_dir);
        
        // 2. Create data
        // We use a repeatable pattern to check compression
        let original_data = b"Restless rust rusts fast. ".repeat(1000); 
        
        // 3. Write
        println!("Writing data...");
        let hash = store.write_chunk(&original_data).expect("Write failed");
        println!("Written Hash: {}", hash);
        
        // 4. Read
        println!("Reading data...");
        let loaded_data = store.read_chunk(&hash).expect("Read failed");
        
        // 5. Verify
        assert_eq!(original_data.to_vec(), loaded_data, "Data mismatch!");
        println!("Success! Data matches.");
        
        // Cleanup
        fs::remove_dir_all(test_dir).unwrap();
    }
}