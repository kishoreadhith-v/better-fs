// src/file_manager.rs
use crate::chunker::Chunker;
use crate::storage::Storage;

#[derive(Debug, Clone)]
pub struct FileRecipe {
    pub file_size: u64,
    pub chunks: Vec<String>, // List of Hash IDs in order
}

pub struct FileManager {
    storage: Storage,
}

impl FileManager {
    pub fn new(storage_path: &str) -> Self {
        FileManager {
            storage: Storage::new(storage_path),
        }
    }

    /// The Core Logic: Ingests a byte stream, chunks it, deduplicates it.
    pub fn write_file(&self, data: &[u8]) -> FileRecipe {
        let mut chunker = Chunker::new();
        let mut recipe = Vec::new();
        let mut current_chunk_buffer = Vec::new();
        let mut total_size = 0;

        for &byte in data {
            // 1. Keep track of the actual data for this chunk
            current_chunk_buffer.push(byte);
            
            // 2. Feed the math engine
            chunker.feed_byte(byte);

            // 3. Did we hit a magic boundary?
            if chunker.should_cut() {
                // Save to disk (returns hash)
                let hash = self.storage.write_chunk(&current_chunk_buffer);
                recipe.push(hash);
                
                // Track size
                total_size += current_chunk_buffer.len() as u64;
                
                // Reset buffer for the next chunk
                current_chunk_buffer.clear();
                // Note: We do NOT reset the chunker window. 
                // The rolling hash continues flowing across boundaries.
            }
        }

        // 4. Handle the "Leftovers" 
        // (The last piece of the file rarely ends exactly on a boundary)
        if !current_chunk_buffer.is_empty() {
            let hash = self.storage.write_chunk(&current_chunk_buffer);
            recipe.push(hash);
            total_size += current_chunk_buffer.len() as u64;
        }

        FileRecipe {
            file_size: total_size,
            chunks: recipe,
        }
    }

    /// Reconstructs a file by reading all its chunks back
    pub fn read_file(&self, recipe: &FileRecipe) -> Vec<u8> {
        let mut full_data = Vec::new();
        
        for hash in &recipe.chunks {
            // Retrieve data from "Freezer"
            if let Some(chunk_data) = self.storage.read_chunk(hash) {
                full_data.extend(chunk_data);
            } else {
                eprintln!("CRITICAL ERROR: Chunk {} missing from storage!", hash);
                // In a real system, this is where you return an IO Error
            }
        }
        
        full_data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_file_ingestion_and_restoration() {
        let db_path = "./test_file_mgr_db";
        let manager = FileManager::new(db_path);

        // 1. Create a "Virtual File" with repeating patterns
        // "Hello" repeats, so it should technically be deduplicated if chunks align
        let original_content = "RepeatPattern ".repeat(1000); 
        let data = original_content.as_bytes();

        // 2. Write it (Chunk -> Hash -> Store)
        let recipe = manager.write_file(data);
        
        println!("File split into {} chunks", recipe.chunks.len());
        println!("First Chunk Hash: {}", recipe.chunks[0]);

        // 3. Read it back (Hash -> Store -> Data)
        let restored_data = manager.read_file(&recipe);
        let restored_string = String::from_utf8(restored_data).unwrap();

        // 4. Verify exact match
        assert_eq!(original_content, restored_string, "Restored file must match original");
        assert_eq!(original_content.len() as u64, recipe.file_size);

        // Cleanup
        fs::remove_dir_all(db_path).unwrap();
    }
}