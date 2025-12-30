// src/main.rs
mod chunker;
mod storage;
mod file_manager;

use clap::{Parser, Subcommand};
use file_manager::FileManager;
use std::path::PathBuf;
use std::fs;
use std::io::Write; // Needed for flushing output

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
}

fn main() {
    let args = Cli::parse();
    
    // Initialize the engine in a folder named "my_storage"
    // This creates a permanent database on your disk.
    let storage_path = "./my_storage";
    let manager = FileManager::new(storage_path);

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
        },
        Commands::Read { file_name } => {
            // 1. Ask BetterFS for the bytes
            match manager.read_file(&file_name) {
                Ok(data) => {
                    // 2. Write to Standard Output (so you can pipe it)
                    std::io::stdout().write_all(&data).unwrap();
                },
                Err(e) => eprintln!("Error: {}", e),
            }
        },
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
    }
}