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
        let (key, value) = BlockIterator::seek_key_within_within_index(block.clone(), 0);
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
            BlockIterator::seek_key_within_within_index(self.block.clone(), 0);

        self.key = key;
        self.value = value;
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 >= self.block.offsets.len() {
            return;
        }

        let (key, value) =
            BlockIterator::seek_key_within_within_index(self.block.clone(), self.idx + 1);
        self.key = key;
        self.value = value;
        self.idx += 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let (key, value, offset) = BlockIterator::seek_key(self.block.clone(), key);
        self.key = key;
        self.value = value;
        self.idx = offset;
    }


    ///Note: This implement may cause bug. eg: "11".compare("2") == Less
    fn seek_key(block: Arc<Block>, key: &[u8]) -> (Vec<u8>, Vec<u8>, usize) {
        let mut left = 0;
        let mut right = block.offsets.len();
        let (mut result_key, mut value) :(Vec<u8>, Vec<u8>);

        while left < right {
            let mid = left + (right - left) / 2;
            let cur_key: Vec<u8> = BlockIterator::seek_key_within_index(block.clone(), mid);

            match cur_key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    right = mid;
                }
                std::cmp::Ordering::Equal => {
                    (result_key, value) = BlockIterator::seek_key_within_within_index(block.clone(), mid);
                    return (result_key, value, mid);
                }
            }
        }

        // if (left >= block.offsets.len()) {
        //     left = block.offsets.len() - 1;
        // }

        (result_key, value) = BlockIterator::seek_key_within_within_index(block.clone(), left);
        (result_key, value, left)
    }

    fn seek_key_within_index(block: Arc<Block>, index: usize) -> Vec<u8> {
        let offset = block.offsets[index] as usize;

        let key_len = u16::from_be_bytes([block.data[offset], block.data[offset + 1]]) as usize;

        block.data[offset + 2..offset + key_len + 2].to_vec()
    }

    fn seek_key_within_within_index(block: Arc<Block>, index: usize) -> (Vec<u8>, Vec<u8>) {
        let offset = block.offsets[index] as usize;
        let key_len = u16::from_be_bytes([block.data[offset], block.data[offset + 1]]) as usize;

        let value_len_pos = offset + key_len + 2;
        let value_len = u16::from_be_bytes([block.data[value_len_pos], block.data[value_len_pos + 1]]) as usize;
        
        let key = block.data[offset + 2..offset + 2 + key_len].to_vec();
        let value_pos = offset + key_len + 4;
        let value = block.data[value_pos..(value_pos + value_len)].to_vec();
        (key, value)
    }


}

#[cfg(test)]
mod test {
    use crate::{block::{BlockIterator, Block, BlockBuilder, builder}, iterators};
    use std::sync::Arc;
    fn binary_search(nums: &[i32], target: i32) -> usize {
        let mut left = 0;
        let mut right = nums.len();
    
        while left < right {
            let mid = left + (right - left) / 2;
    
            if nums[mid] <= target {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
    
        left
    }

    #[test]
    fn binary_search_test() {
        let nums = vec![1, 3, 5, 7, 9];
        let target = 9;
    
        let result = binary_search(&nums, target);
        println!("Position: {}", result);
    }

    

    #[test]
    fn iterator_test() {
        let mut builder = BlockBuilder::new(4096);
        builder.add(b"233", b"233333");
        builder.add(b"122", b"122222");
        
        let mut block =  builder.build();
        let mut iterator = BlockIterator::create_and_seek_to_first(Arc::new(block));
        
        assert_eq!(b"122", iterator.key());
        assert_eq!(b"122222", iterator.value());
   
        iterator.next();

        assert_eq!(b"233", iterator.key());
        assert_eq!(b"233333", iterator.value());
    }

    #[test]
    fn iterator_seek_key_test() {
        let mut builder = BlockBuilder::new(4096);
        builder.add(b"1", b"1");
        builder.add(b"2", b"1");
        builder.add(b"4", b"1");
        builder.add(b"5", b"1");
        builder.add(b"8", b"1");
        let mut block =  builder.build();
        let mut iterator = BlockIterator::create_and_seek_to_first(Arc::new(block));
        
        iterator.seek_to_key(b"3");
        assert_eq!(b"4", iterator.key())
        
    }
}