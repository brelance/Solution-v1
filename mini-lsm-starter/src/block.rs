#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::{io::Read, iter, ops::Index};

pub use builder::BlockBuilder;
/// You may want to check `bytes::BufMut` out when manipulating continuous chunks of memory
use bytes::{Bytes, Buf, BufMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree.
/// It is a collection of sorted key-value pairs.
/// The `actual` storage format is as below (After `Block::encode`):
///
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------

const BLOCK_SIZE: usize =  4096;

pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut encoded: Vec<u8> = self.data.clone();
        for &offset in self.offsets.iter() {
            encoded.put_u16(offset);
        }

        // Add nums of elements
        encoded.put_u16(self.offsets.len() as u16);
        Bytes::from(encoded)
    }
        
    pub fn decode(data: &[u8]) -> Self {
        let src_len = data.len();
        let num_of_elements = (data[src_len - 2] as u16 ) << 8 | data[src_len - 1] as u16;
        let offset_start_pos = src_len - 2 - num_of_elements as usize * 2;

        let kv_data = data[0..offset_start_pos].to_vec();
        let offset_slice = &data[offset_start_pos..src_len - 2];
        // let offsets: Vec<u16> = offset_slice
        //     .chunks(2)
        //     .into_iter()
        //     .map(|high, low| (high as u16) << 8 | low as u16)
        let mut offsets = Vec::new();
        for pair in offset_slice.chunks(2) {
            match pair {
                &[high, low] => {
                    let offset = (high as u16) << 8 | low as u16;
                    offsets.push(offset);
                }
                _ => {}
            }
        }
        
        Block {data: kv_data, offsets} 
        // let block_builder: BlockBuilder = BlockBuilder::new(BLOCK_SIZE);
    }

    pub fn size(&self) -> usize {
        // length of data + length of keys + length of nums
        return self.data.len() + self.offsets.len() * 2 + 2;
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test {
    use crate::block::{BlockBuilder, Block};
    #[test]
    fn test_chunk() {
        let numbers: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        // let offsets: Vec<u16> = numbers
        //     .chunks(2)
        //     .into_iter()
        //     .map(|&[high, low]| (high as u16) << 8 | low as u16 )
        //     .collect();
       
        for pair in numbers.chunks(2) {
            match pair {
                &[a, b] => println!("Pair: {}, {}", a, b),
                &[a] => println!("Single element: {}", a),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_encode_decode() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"233", b"233333");
        builder.add(b"122", b"122222");
        let mut block =  builder.build();
        let data = block.encode();
        Block::decode(&data);
    }
}


