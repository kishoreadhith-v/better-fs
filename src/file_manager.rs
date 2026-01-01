// src/file_manager.rs
use crate::chunker::Chunker;
use crate::storage::Storage;
use serde::{ Deserialize, Serialize };
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FileKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRecipe {
    pub file_size: u64,
    pub chunks: Vec<String>, // List of Hash IDs in order
    pub kind: FileKind,
}

pub struct FileManager {
    storage: Storage,
    db: sled::Db,
}

impl FileManager {
    pub fn new(storage_path: &str) -> Self {
        let storage = Storage::new(storage_path);

        // Open the database inside the same folder
        // "metadata_db" will be a folder inside your storage path
        let db_path = Path::new(storage_path).join("metadata_db");
        let db = sled::open(db_path).expect("Failed to open metadata database");

        FileManager { storage, db }
    }

    // =======================================================================
    // PUBLIC API (What the FUSE Frontend will call)
    // =======================================================================

    /// 1. WRITE: Ingests data, creates a recipe, and saves it to the DB under 'filename'
    pub fn write_file(&self, filename: &str, data: &[u8]) -> Result<(), String> {
        // A. Run the math engine to create the recipe (Chunking + Storage)
        let recipe = self.create_recipe_from_data(data);

        // B. Convert the Recipe struct into bytes (Serialization)
        let encoded_recipe = bincode
            ::serialize(&recipe)
            .map_err(|e| format!("Serialization error: {}", e))?;

        // C. Save to Database (Key: Filename, Value: RecipeBytes)
        self.db.insert(filename, encoded_recipe).map_err(|e| format!("Database error: {}", e))?;

        // Ensure data is flushed to disk immediately
        self.db.flush().map_err(|e| format!("Flush error: {}", e))?;

        println!("Debug: Saved recipe for '{}' ({} chunks)", filename, recipe.chunks.len());
        Ok(())
    }

    /// 2. READ: Looks up a filename, finds the recipe, and reconstructs the data
    pub fn read_file(&self, filename: &str) -> Result<Vec<u8>, String> {
        // A. Look up the filename in the DB
        match self.db.get(filename) {
            Ok(Some(bytes)) => {
                // B. Decode the binary back into a Struct
                let recipe: FileRecipe = bincode
                    ::deserialize(&bytes)
                    .map_err(|e| format!("Deserialization error: {}", e))?;

                // C. Safety check for Directories
                if recipe.kind == FileKind::Directory {
                    return Ok(Vec::new());
                }

                // D. Reconstruct the file (New Logic handling Results)
                let mut result = Vec::new();
                for hash in recipe.chunks {
                    // We handle the Result from storage.read_chunk here
                    match self.storage.read_chunk(&hash) {
                        Ok(chunk_data) => result.extend_from_slice(&chunk_data),
                        Err(e) => {
                            return Err(format!("Storage corrupted. Chunk {} missing: {}", hash, e));
                        }
                    }
                }
                Ok(result)
            }
            Ok(None) => Err(format!("File not found: {}", filename)),
            Err(e) => Err(format!("Database error: {}", e)),
        }
    }

    /// 3. LIST: Returns a list of all filenames in the system
    pub fn list_files(&self) -> Vec<String> {
        let mut files = Vec::new();
        // Iterate over every key in the DB
        for item in self.db.iter() {
            if let Ok((key, _)) = item {
                if let Ok(filename) = String::from_utf8(key.to_vec()) {
                    files.push(filename);
                }
            }
        }
        files
    }

    // =======================================================================
    // INTERNAL HELPERS (The "Engine Room" - Private)
    // =======================================================================

    /// The core logic from your old write_file
    fn create_recipe_from_data(&self, data: &[u8]) -> FileRecipe {
        let mut chunker = Chunker::new(); // Ensure Chunker is imported
        let mut recipe = Vec::new();
        let mut current_chunk_buffer = Vec::new();
        let mut total_size = 0;

        for &byte in data {
            current_chunk_buffer.push(byte);
            chunker.feed_byte(byte);

            // CHANGED: Simplified logic.
            // We cut if the algorithm says so, AND we have at least 2KB...
            // OR if the buffer gets too big (e.g., 64KB) to prevent massive memory usage.
            if
                (chunker.should_cut() && current_chunk_buffer.len() >= 2048) ||
                current_chunk_buffer.len() >= 65536
            {
                let hash = self.storage
                    .write_chunk(&current_chunk_buffer)
                    .expect("Failed to write chunk");
                recipe.push(hash);
                total_size += current_chunk_buffer.len() as u64;
                current_chunk_buffer.clear();
            }
        }

        // HANDLE THE TAIL (The last piece of the file)
        if !current_chunk_buffer.is_empty() {
            let hash = self.storage
                .write_chunk(&current_chunk_buffer)
                .expect("Failed to write tail chunk");
            recipe.push(hash);
            total_size += current_chunk_buffer.len() as u64;
        }

        FileRecipe {
            file_size: total_size,
            chunks: recipe,
            kind: FileKind::File,
        }
    }

    /// The core logic from your old read_file
    fn reconstruct_from_recipe(&self, recipe: &FileRecipe) -> Vec<u8> {
        let mut data = Vec::new();

        for hash in &recipe.chunks {
            // FIX: Use 'if let Ok' instead of 'if let Some'
            if let Ok(chunk) = self.storage.read_chunk(hash) {
                data.extend_from_slice(&chunk);
            } else {
                eprintln!("Warning: Failed to read chunk {}", hash);
            }
        }

        data
    }
    /// Helper for FUSE: Check if a file exists and return its size
    pub fn get_file_metadata(&self, filename: &str) -> Option<(u64, FileKind)> {
        match self.db.get(filename) {
            Ok(Some(bytes)) => {
                let recipe: FileRecipe = bincode::deserialize(&bytes).ok()?;
                Some((recipe.file_size, recipe.kind))
            }
            _ => None,
        }
    }
    pub fn delete_file(&self, filename: &str) -> Result<(), String> {
        self.db.remove(filename).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn rename_file(&self, old_name: &str, new_name: &str) -> Result<(), String> {
        // 1. Get the recipe for the old name
        if let Some(data) = self.db.get(old_name).map_err(|e| e.to_string())? {
            // 2. Insert it under the new name
            self.db.insert(new_name, data).map_err(|e| e.to_string())?;
            // 3. Remove the old name
            self.db.remove(old_name).map_err(|e| e.to_string())?;
            Ok(())
        } else {
            Err("File not found".to_string())
        }
    }

    pub fn create_directory(&self, path: &str) -> Result<(), String> {
        let recipe = FileRecipe {
            file_size: 0,
            chunks: vec![],
            kind: FileKind::Directory,
        };
        let encoded: Vec<u8> = bincode::serialize(&recipe).map_err(|e| e.to_string())?;
        self.db.insert(path, encoded).map_err(|e| e.to_string())?;
        Ok(())
    }
}

// =======================================================================
// TESTS
// =======================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_database_persistence() {
        let db_path = "./test_db_persistence";
        // Clean up previous runs
        if Path::new(db_path).exists() {
            fs::remove_dir_all(db_path).unwrap();
        }

        {
            // 1. Open the manager and save a file
            let manager = FileManager::new(db_path);
            let content = b"This is a test file for the database.";
            manager.write_file("test.txt", content).expect("Write failed");

            // Verify it exists in memory
            let loaded = manager.read_file("test.txt").expect("Read failed");
            assert_eq!(loaded, content);
        } // <--- Manager is dropped here (Simulates closing the app)

        println!("--- Simulating App Restart ---");

        {
            // 2. Re-open the manager (Simulate restart)
            let manager = FileManager::new(db_path);

            // 3. Try to read the file again
            // If DB works, this should succeed. If DB fails, this returns "File not found".
            let loaded = manager
                .read_file("test.txt")
                .expect("Persistence failed: File not found after restart");

            assert_eq!(loaded, b"This is a test file for the database.");
            println!("Success: Data survived the restart!");
        }

        // Cleanup
        fs::remove_dir_all(db_path).unwrap();
    }
}

// src/file_manager.rs (At the bottom)

#[cfg(test)]
mod integrity_tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_manager_cycle() {
        let path = "./test_fm_db";
        // Clean up old test data
        if std::path::Path::new(path).exists() { 
            fs::remove_dir_all(path).unwrap(); 
        }
        
        let fm = FileManager::new(path);
        // Create data large enough to force multiple chunks (> 4KB)
        let data = b"A repeatable pattern for testing chunking limits...".repeat(500); // ~25KB
        
        println!("1. Writing file via Manager...");
        fm.write_file("test_cycle.txt", &data).expect("Manager Write Failed");
        
        println!("2. Reading file via Manager...");
        let read_back = fm.read_file("test_cycle.txt").expect("Manager Read Failed");
        
        // Compare lengths first for easy debugging
        assert_eq!(data.len(), read_back.len(), "Length mismatch!");
        assert_eq!(data.to_vec(), read_back, "Content mismatch!");
        
        println!("Success! Manager cycle works.");
        fs::remove_dir_all(path).unwrap();
    }
}