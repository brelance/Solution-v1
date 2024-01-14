#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use super::Block;
use std::{collections::BTreeMap, io::Read};

/// Builds a block.
pub struct BlockBuilder {
    block_size: usize,
    buffer: BTreeMap<Vec<u8>, Vec<u8>>,
    rest_size: usize,
    num_of_elements: usize,
}


impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            buffer: BTreeMap::new(),
            rest_size: block_size,
            num_of_elements: 0,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let insert_size = key.len() + value.len() + 6;
        if self.rest_size < insert_size {
            return false;
        }

        self.buffer.insert(key.to_vec(), value.to_vec());
        self.rest_size -= insert_size;
        self.num_of_elements += 1;
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.num_of_elements == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        let mut data: Vec<u8> = Vec::new();
        let mut offsets: Vec<u16> = Vec::new();
        let mut offset: u16 = 0;

        for (key, value) in self.buffer.iter() {
                offsets.push(offset);

                let key_len = key.len() as u16;
                let value_len = value.len() as u16;

                data.put_u16(key_len);
                data.extend_from_slice(key.as_slice());
                data.put_u16(value_len);
                data.extend_from_slice(value.as_slice());

                offset = offset + 2 + key_len + 2 + value_len;
        }
        // self.buffer
        //     .iter()
        //     .map(|(key, value)| {
        //         offsets.push(offset);

        //         let key_len = key.len() as u16;
        //         let value_len = value.len() as u16;

        //         data.extend_from_slice(&key_len.to_ne_bytes());
        //         data.extend_from_slice(key.as_slice());
        //         data.extend_from_slice(&value_len.to_ne_bytes());
        //         data.extend_from_slice(value.as_slice());

        //         offset = offset + 2 + key_len + 2 + value_len;
        //     });
        
            
        Block {data, offsets}    
    }
}


#[cfg(test)]
mod user_tests {
    use super::BlockBuilder;
    #[test]
    fn build_test() {
        let mut block_builder = BlockBuilder::new(4096);
        block_builder.add(b"1", b"432");
        block_builder.add(b"3", b"432");
        block_builder.add(b"2", b"233333");

        let block = block_builder.build();
        
    }
}