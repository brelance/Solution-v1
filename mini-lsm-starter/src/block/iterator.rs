#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{sync::Arc, borrow::Borrow, ops::Index};

use crate::iterators::StorageIterator;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: Vec<u8>,
    /// The corresponding value, can be empty
    value: Vec<u8>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let (key, value) = BlockIterator::seek_key_value_within_offset(block.clone(), 0);
        BlockIterator { 
            block, 
            key, 
            value,
            idx: 0, 
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let (key, value, _) = BlockIterator::seek_key(block.clone(), key);
        
        BlockIterator { 
            block, 
            key,
            value,
            idx: 0, 
        }
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        unimplemented!()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let (key, value) =
            BlockIterator::seek_key_value_within_offset(self.block.clone(), 0);

        self.key = key;
        self.value = value;
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        let (key, value) =
            BlockIterator::seek_key_value_within_offset(self.block.clone(), self.idx + 1);
        self.key = key;
        self.value = value;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let (key, value, offset) = BlockIterator::seek_key(self.block.clone(), key);
        self.key = key;
        self.value = value;
        self.idx = offset;
    }

    fn seek_key(block: Arc<Block>, key: &[u8]) -> (Vec<u8>, Vec<u8>, usize) {
        let mut left = 0;
        let mut right = block.offsets.len() - 1;
        let mut mid = 0;

        while left <= right {
            mid = (left + right) / 2;
            let cur_key: Vec<u8> = BlockIterator::seek_key_within_offset(block.clone(), mid);

            match cur_key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    right = mid - 1;
                }
                std::cmp::Ordering::Equal => {
                    break;
                }
            }
        }
        let (key, value) = BlockIterator::seek_key_value_within_offset(block.clone(), mid);

        (key, value, mid)
    }

    fn seek_key_within_offset(block: Arc<Block>, offset: usize) -> Vec<u8> {
        let pos = u16::from_be_bytes([block.data[offset], block.data[offset + 1]]) as usize;
        let key_len = u16::from_be_bytes([block.data[pos], block.data[pos + 1]]) as usize;

        block.data[pos + 2..pos + 2 + key_len].to_vec()
    }

    fn seek_key_value_within_offset(block: Arc<Block>, offset: usize) -> (Vec<u8>, Vec<u8>) {
        let pos = u16::from_be_bytes([block.data[offset], block.data[offset + 1]]) as usize;
        let key_len = u16::from_be_bytes([block.data[pos], block.data[pos + 1]]) as usize;

        let value_len_pos = pos + key_len + 2;
        let value_len = u16::from_be_bytes([block.data[value_len_pos], block.data[value_len_pos + 1]]) as usize;
        
        let key = block.data[pos + 2..pos + 2 + key_len].to_vec();
        let value_pos = pos + key_len + 4;
        let value = block.data[value_pos..(value_pos + value_len)].to_vec();
        (key, value)
    }


}
