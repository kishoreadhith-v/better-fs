// tests/backend_stress.rs

#[path = "../src/chunker.rs"]
mod chunker;
#[path = "../src/storage.rs"]
mod storage;
#[path = "../src/file_manager.rs"]
mod file_manager;

use file_manager::FileManager;
use std::fs;
use std::path::Path;

fn setup(path: &str) -> FileManager {
    if Path::new(path).exists() {
        fs::remove_dir_all(path).unwrap();
    }
    FileManager::new(path)
}

#[test]
fn test_1_empty_file() {
    let path = "./test_db_1";
    let manager = setup(path);
    // NEW API: We must provide a filename
    manager.write_file("empty.txt", &[]).expect("Write failed");

    // Read it back
    let restored = manager.read_file("empty.txt").expect("Read failed");
    assert_eq!(restored.len(), 0);
}

#[test]
fn test_2_tiny_file() {
    let path = "./test_db_2";
    let manager = setup(path);
    let data = b"Tiny";

    manager.write_file("tiny.txt", data).expect("Write failed");

    let restored = manager.read_file("tiny.txt").expect("Read failed");
    assert_eq!(restored, data);
}

#[test]
fn test_3_persistence_check() {
    let TEST_DB = "./test_db_3";
    // This replaces the old deduplication test.
    // We want to prove that data survives if we "Restart" the manager.

    // 1. Write a file
    {
        let manager = setup(TEST_DB);
        manager.write_file("resume.pdf", b"Important Data").unwrap();
    } // Manager is dropped here (Database closes)

    // 2. Re-open (Simulate Restart)
    let manager = FileManager::new(TEST_DB);

    // 3. Read it back
    let data = manager.read_file("resume.pdf").expect("File vanished after restart!");
    assert_eq!(data, b"Important Data");
}

#[test]
fn test_4_large_file_stress() {
    let TEST_DB = "./test_db_4";
    let manager = setup(TEST_DB);

    // Generate 1MB of pseudo-random data
    let data: Vec<u8> = (0u32..1024 * 1024)
        .map(|i| i.wrapping_mul(37).wrapping_add(11) as u8)
        .collect();

    let start = std::time::Instant::now();
    manager.write_file("large_video.mp4", &data).expect("Write failed");
    let duration = start.elapsed();

    println!("Processed 1MB in {:?}", duration);

    // Verify Integrity
    let restored = manager.read_file("large_video.mp4").expect("Read failed");
    assert_eq!(data, restored, "1MB file corruption detected on restore");
}

#[test]
fn test_5_missing_file() {
    let TEST_DB = "./test_db_5";
    let manager = setup(TEST_DB);
    // Try to read a file that doesn't exist
    let result = manager.read_file("ghost.txt");

    assert!(result.is_err());
}
