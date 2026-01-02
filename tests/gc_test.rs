// tests/gc_test.rs

// --- MODULE HACKS (To access your src code from a test file) ---
#[path = "../src/chunker.rs"]
mod chunker;
#[path = "../src/storage.rs"]
mod storage;
#[path = "../src/file_manager.rs"]
mod file_manager;
// --------------------------------------------------------------

use file_manager::FileManager;
use std::fs;
use std::path::Path;

// Helper to start with a clean slate
fn setup_test_env(dir_name: &str) -> FileManager {
    if Path::new(dir_name).exists() {
        fs::remove_dir_all(dir_name).unwrap();
    }
    FileManager::new(dir_name)
}

// Helper to count physical chunk files on disk
fn count_chunks_on_disk(storage_path: &str) -> usize {
    let cas_path = Path::new(storage_path).join("cas");
    if !cas_path.exists() {
        return 0;
    }

    let mut count = 0;
    // Iterate over subdirectories (e.g., "a1", "b2")
    for entry in fs::read_dir(cas_path).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_dir() {
            // Count files inside each subdirectory
            count += fs::read_dir(entry.path()).unwrap().count();
        }
    }
    count
}

#[test]
fn test_gc_removes_orphans_only() {
    let test_dir = "./test_gc_env";
    let manager = setup_test_env(test_dir);

    // 1. Create Data
    // "Shared Data" -> Will be used by file_A and file_B
    let shared_content = b"This is shared content between two files.";
    // "Unique Data" -> Will be used ONLY by file_C
    let unique_content = b"This is unique content that will become an orphan.";

    manager.write_file("file_A.txt", shared_content).expect("Write A failed");
    manager.write_file("file_B.txt", shared_content).expect("Write B failed"); // Dedup happens here
    manager.write_file("file_C.txt", unique_content).expect("Write C failed");

    // CHECKPOINT 1: Verify Disk State
    // We expect 2 chunks total:
    // 1. Hash(Shared)
    // 2. Hash(Unique)
    assert_eq!(count_chunks_on_disk(test_dir), 2, "Should have exactly 2 chunks on disk initially");

    // 2. Delete Files
    // Delete A (B still needs the shared chunk, so it's NOT an orphan)
    manager.delete_file("file_A.txt").expect("Delete A failed");
    // Delete C (The unique chunk is now an orphan)
    manager.delete_file("file_C.txt").expect("Delete C failed");

    // CHECKPOINT 2: Verify Disk State (Before GC)
    // Deleting files only removes recipes, not chunks. 
    // So disk count should STILL be 2.
    assert_eq!(count_chunks_on_disk(test_dir), 2, "Chunks should persist before GC runs");

    // 3. Run Garbage Collection
    let deleted_count = manager.run_gc().expect("GC failed");

    // 4. Verify Results
    
    // Assertion A: GC should report 1 deletion (The unique chunk)
    assert_eq!(deleted_count, 1, "GC should have deleted exactly 1 chunk");

    // Assertion B: Disk should now have 1 chunk left (The shared one)
    assert_eq!(count_chunks_on_disk(test_dir), 1, "Disk should contain 1 shared chunk after GC");

    // Assertion C: File B should still be readable (The shared chunk wasn't deleted)
    let b_content = manager.read_file("file_B.txt").expect("File B should still exist");
    assert_eq!(b_content, shared_content, "File B content corrupted!");

    // Cleanup
    fs::remove_dir_all(test_dir).unwrap();
}

#[test]
fn test_gc_does_nothing_on_clean_state() {
    let test_dir = "./test_gc_clean";
    let manager = setup_test_env(test_dir);

    // Write a file
    manager.write_file("keep_me.txt", b"Important Data").unwrap();

    // Run GC immediately
    let deleted = manager.run_gc().unwrap();

    // Should delete nothing
    assert_eq!(deleted, 0, "GC should not delete chunks that are in use");
    assert_eq!(count_chunks_on_disk(test_dir), 1, "Data should remain");

    fs::remove_dir_all(test_dir).unwrap();
}