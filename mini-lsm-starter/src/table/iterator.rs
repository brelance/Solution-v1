#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Result, Ok};

use super::SsTable;
use crate::iterators::{StorageIterator, self};
use crate::block::BlockIterator;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iterator: BlockIterator,
    block_idx: usize,
    // is_valid: bool,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block_iterator = Self::create_block_iterator(table.clone(), 0)?;
        Ok(Self {
            table,
            block_iterator,
            block_idx: 0,
            // is_valid: true,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let new_block_iterator= Self::create_block_iterator(self.table.clone(), 0)?;
        self.block_iterator = new_block_iterator;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let (mut block_iterator, block_idx) = Self::create_block_iterator_with_key(table.clone(), key)?;

        // let is_valid = block_iterator.is_valid();
        block_iterator.seek_to_key(key);

        Ok(Self {
            table,
            block_iterator,
            block_idx,
         // is_valid,
        })
        
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing this function.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (mut block_iterator, block_idx) = Self::create_block_iterator_with_key(self.table.clone(), key)?;

        block_iterator.seek_to_key(key);
        self.block_iterator = block_iterator;
        Ok(())
    }

    fn create_block_iterator(table: Arc<SsTable>, index: usize) -> Result<BlockIterator> {
        let block: Arc<crate::block::Block> = table.read_block(0)?;
        Ok(BlockIterator::new(block))
    }

    fn create_block_iterator_with_key(table: Arc<SsTable>, key: &[u8]) -> Result<(BlockIterator, usize)> {
        let block_index = table.find_block_idx(key);
        let target_block = table.read_block(block_index)?;

        let target_iterator = BlockIterator::new(target_block);
        Ok((target_iterator, block_index))
    }
}

impl StorageIterator for SsTableIterator {
    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        self.block_iterator.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.block_iterator.key()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.block_iterator.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            println!("Iterator outs bound of the block");
            return Ok(());
        }

        self.block_iterator.next_without_check();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    
    #[test]
    fn test1() {
        
    }
}
