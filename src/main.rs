// src/main.rs
mod chunker;
mod storage;
mod file_manager;
mod fuse_handler;

use clap::{ Parser, Subcommand };
use file_manager::FileManager;
use std::path::PathBuf;
use std::fs;
use std::io::Write; // Needed for flushing output
use crate::file_manager::FileRecipe;
use fuser::{ MountOption, Session }; // Ensure you have fuser imports

// 1. Define the Command Line Interface (CLI)
#[derive(Parser)]
#[command(name = "BetterFS")]
#[command(about = "A deduplicating, content-addressable filesystem", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Save a file to BetterFS
    Write {
        /// The path to the file you want to upload
        file_path: PathBuf,
    },
    /// Read a file back from BetterFS
    Read {
        /// The name of the file inside BetterFS
        file_name: String,
    },
    /// List all files stored in BetterFS
    List,
    Mount {
        /// The folder to mount to (e.g., ./mnt)
        mount_point: String,
    },
    /// Inspect the internal database (for debugging)
    Inspect,
}

fn main() {
    let args = Cli::parse();

    // Initialize the engine in a folder named "my_storage"
    // This creates a permanent database on your disk.
    let storage_path = "./my_storage";
    let manager = FileManager::new(storage_path);
    let db_path = format!("{}/metadata_db", storage_path);

    match args.command {
        Commands::Write { file_path } => {
            // 1. Read data from your REAL hard drive
            let data = match fs::read(&file_path) {
                Ok(content) => content,
                Err(e) => {
                    eprintln!("Error: Could not read file '{:?}': {}", file_path, e);
                    return;
                }
            };

            let filename = file_path.file_name().unwrap().to_str().unwrap();

            // 2. Ingest it into BetterFS
            println!("Writing '{}' ({} bytes)...", filename, data.len());
            match manager.write_file(filename, &data) {
                Ok(_) => println!("Success! Saved as '{}'", filename),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        Commands::Read { file_name } => {
            // 1. Ask BetterFS for the bytes
            match manager.read_file(&file_name) {
                Ok(data) => {
                    // 2. Write to Standard Output (so you can pipe it)
                    std::io::stdout().write_all(&data).unwrap();
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        Commands::List => {
            let files = manager.list_files();
            if files.is_empty() {
                println!("No files found in storage.");
            } else {
                println!("Files in BetterFS:");
                for file in files {
                    println!(" - {}", file);
                }
            }
        }
        Commands::Mount { mount_point } => {
            println!("Mounting BetterFS to {}...", mount_point);
            println!("(Press Ctrl+C to unmount)");

            // Ensure the mount point exists
            fs::create_dir_all(&mount_point).unwrap();

            // Start the FUSE Driver
            let options = vec![
                fuser::MountOption::RW, // Read-Only
                fuser::MountOption::FSName("betterfs".to_string()),
                fuser::MountOption::AutoUnmount // Helps clean up on exit
            ];

            let fs_impl = fuse_handler::BetterFS::new(manager);

            fuser::mount2(fs_impl, mount_point, &options).unwrap();
        }
        Commands::Inspect => {
            println!("--- INSPECTING DATABASE ---");
            // Open the DB directly for reading
            let db = sled::open(&db_path).expect("Failed to open DB");

            for item in db.iter() {
                if let Ok((key, value)) = item {
                    let key_str = String::from_utf8_lossy(&key);

                    // Try to decode as a FileRecipe
                    // FIX 4: Use 'crate::file_manager' instead of 'better_fs::...'
                    match bincode::deserialize::<FileRecipe>(&value) {
                        Ok(recipe) => {
                            let kind_str = match recipe.kind {
                                file_manager::FileKind::Directory => "DIR",
                                file_manager::FileKind::File => "FILE",
                            };
                            println!(
                                "[{}] {} \t(Size: {} bytes, Chunks: {})",
                                kind_str,
                                key_str,
                                recipe.file_size,
                                recipe.chunks.len()
                            );
                        }
                        Err(_) => {
                            // If it fails, it might be raw data or something else
                            println!("[???] {} \t(Raw Data)", key_str);
                        }
                    }
                }
            }
            println!("---------------------------");
        }
    }
}
