// src/chunker.rs

// We use a simple rolling hash constant (polynomial rolling hash)
const WINDOW_SIZE: usize = 48; // Look at 48 bytes at a time
const MODULUS: u64 = 1_000_000_007; // A large prime number to prevent overflow
const BASE: u64 = 256; // ASCII range

pub struct Chunker {
    window: Vec<u8>,
    current_hash: u64,
}

impl Chunker {
    pub fn new() -> Self {
        Chunker {
            window: Vec::new(),
            current_hash: 0,
        }
    }

    // The Rolling Hash Calculation
    // This is the "Magic" that slides the window efficiently
    pub fn feed_byte(&mut self, new_byte: u8) {
        // 1. Add new byte to window
        self.window.push(new_byte);

        // 2. If window is full, remove the oldest byte (slide right)
        if self.window.len() > WINDOW_SIZE {
            let _old_byte = self.window.remove(0);
            
            // MATH: Remove the leading term from the polynomial
            // hash = (hash - old_byte * BASE^(N-1)) % MODULUS
            // (Simplified for this demo, usually we use pre-computed powers)
            
            // For a basic demo, let's just re-calculate to be safe and clear:
            // (Production code would use the optimized sliding formula)
            self.current_hash = 0;
            for &b in &self.window {
                self.current_hash = (self.current_hash.wrapping_mul(BASE).wrapping_add(b as u64)) % MODULUS;
            }
        } else {
            // Just add the new byte
            self.current_hash = (self.current_hash.wrapping_mul(BASE).wrapping_add(new_byte as u64)) % MODULUS;
        }
    }

    // The Logic: "Should we cut here?"
    pub fn should_cut(&self) -> bool {
        if self.window.len() < WINDOW_SIZE {
            return false;
        }
        // TARGET: Cut when the hash ends in 12 zeros (binary)
        // This statistically chunks every ~4KB (2^12)
        (self.current_hash & 0xFFF) == 0
    }
}

// UNIT TEST: Run this with 'cargo test'
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunking_consistency() {
        let mut chunker = Chunker::new();
        
        // Create more random-looking synthetic data
        // Using a simple LCG (Linear Congruential Generator) pattern
        let data: Vec<u8> = (0u32..100_000)
            .map(|i| {
                let x = i.wrapping_mul(1103515245).wrapping_add(12345);
                ((x / 65536) % 256) as u8
            }) 
            .collect();
        
        let mut cut_points = Vec::new();

        for (i, &byte) in data.iter().enumerate() {
            chunker.feed_byte(byte);
            if chunker.should_cut() {
                cut_points.push(i);
            }
        }
        
        println!("Found {} chunks at positions: {:?}", cut_points.len(), &cut_points[..cut_points.len().min(10)]);
        println!("Average chunk size: ~{} bytes", if cut_points.len() > 0 { 100_000 / cut_points.len() } else { 0 });
        
        assert!(cut_points.len() > 5, "Statistically unlikely to have fewer than 5 chunks in 100KB data");
    }
}