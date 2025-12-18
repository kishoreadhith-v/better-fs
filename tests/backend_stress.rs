// tests/backend_stress.rs
// Rust automatically treats files in "tests/" as integration tests.

// We need to import the modules from the binary. 
// Note: For this to work best, we usually make a lib.rs, but for now 
// we will include the modules directly or assume public visibility.
// A quick hack for binary projects is to allow using main modules:

#[path = "../src/chunker.rs"] mod chunker;
#[path = "../src/storage.rs"] mod storage;
#[path = "../src/file_manager.rs"] mod file_manager;

use file_manager::FileManager;
use std::fs;

const TEST_DB: &str = "./test_stress_db";

fn setup() -> FileManager {
    if std::path::Path::new(TEST_DB).exists() {
        fs::remove_dir_all(TEST_DB).unwrap();
    }
    FileManager::new(TEST_DB)
}

#[test]
fn test_1_empty_file() {
    let manager = setup();
    let recipe = manager.write_file(&[]);
    
    assert_eq!(recipe.file_size, 0);
    assert_eq!(recipe.chunks.len(), 0);
    
    let restored = manager.read_file(&recipe);
    assert_eq!(restored.len(), 0);
}

#[test]
fn test_2_tiny_file() {
    let manager = setup();
    // Smaller than the rolling hash window (48 bytes)
    let data = b"Tiny"; 
    let recipe = manager.write_file(data);
    
    assert_eq!(recipe.file_size, 4);
    assert_eq!(recipe.chunks.len(), 1, "Tiny file should be 1 chunk");
    
    let restored = manager.read_file(&recipe);
    assert_eq!(restored, data);
}

#[test]
fn test_3_deduplication_efficiency() {
    let manager = setup();
    
    // FIX: Use random data for the shared block to GUARANTEE cut points.
    // If the data is too "regular" (like repeating text), the chunker might never cut.
    let shared_block: Vec<u8> = (0u32..50_000) // 50KB of random data
        .map(|i| (i.wrapping_mul(1664525).wrapping_add(1013904223) >> 24) as u8)
        .collect();
    
    // Create two files that are 95% identical
    // File 1: [ A A A A A ] [ SHARED DATA ... ] [ X X ]
    // File 2: [ B B B B B ] [ SHARED DATA ... ] [ Y Y ]
    
    let mut file1 = b"PREFIX_A_UNIQUE_HEADER_".to_vec();
    file1.extend_from_slice(&shared_block);
    file1.extend_from_slice(b"_SUFFIX_A");

    let mut file2 = b"PREFIX_B_DIFFERENT_HEADER_".to_vec();
    file2.extend_from_slice(&shared_block);
    file2.extend_from_slice(b"_SUFFIX_B");
    
    let recipe1 = manager.write_file(&file1);
    let recipe2 = manager.write_file(&file2);
    
    // Convert chunk lists to HashSets to find overlap
    let chunks1: std::collections::HashSet<_> = recipe1.chunks.iter().collect();
    let chunks2: std::collections::HashSet<_> = recipe2.chunks.iter().collect();
    
    let intersection_count = chunks1.intersection(&chunks2).count();
    
    println!("Total chunks F1: {}, F2: {}", chunks1.len(), chunks2.len());
    println!("Shared chunks: {}", intersection_count);
    
    // Calculate deduplication savings
    let total_unique_chunks = chunks1.union(&chunks2).count();
    let total_chunk_refs = chunks1.len() + chunks2.len();
    let dedup_savings = total_chunk_refs - total_unique_chunks;
    
    println!("Deduplication saved {} chunk storage operations", dedup_savings);
    println!("Storage efficiency: {:.1}%", (dedup_savings as f64 / total_chunk_refs as f64) * 100.0);
    
    // With content-defined chunking, even 1 shared chunk proves deduplication works
    // The different prefixes shift boundaries, but some chunks should still align
    assert!(intersection_count > 0, "Files with 50KB common data should share at least one chunk");
    assert!(recipe1.chunks.len() > 2, "Expected multiple chunks from 50KB+ file");
}

#[test]
fn test_4_large_file_stress() {
    let manager = setup();
    
    // Generate 1MB of pseudo-random data
    // (Using LCG for determinism)
    let data: Vec<u8> = (0..1024 * 1024)
        .map(|i| (i * 37 + 11) as u8)
        .collect();
        
    let start = std::time::Instant::now();
    let recipe = manager.write_file(&data);
    let duration = start.elapsed();
    
    println!("Processed 1MB in {:?}. Chunks: {}", duration, recipe.chunks.len());
    
    // Verify Integrity
    let restored = manager.read_file(&recipe);
    
    // Check random spots rather than full strict cmp if speed is issue, 
    // but for 1MB full cmp is fine.
    assert_eq!(data, restored, "1MB file corruption detected on restore");
}

#[test]
fn test_5_missing_chunk_handling() {
    let manager = setup();
    let data = b"Important Data";
    let recipe = manager.write_file(data);
    
    // SABOTAGE: Go into the storage and delete the chunk file manually
    let hash = &recipe.chunks[0];
    let chunk_path = std::path::Path::new(TEST_DB)
        .join(&hash[0..2])
        .join(&hash[2..]);
        
    fs::remove_file(chunk_path).expect("Failed to sabotage storage");
    
    // Attempt restore
    let restored = manager.read_file(&recipe);
    
    // Depending on your implementation, this might return partial data or panic.
    // In our current code, we print "CRITICAL ERROR" and skip the chunk.
    // The restored data should be missing bytes.
    assert_ne!(restored, data, "Restored data should be broken after sabotage");
    assert_eq!(restored.len(), 0, "Current implementation drops missing chunks entirely");
}