#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::{BlockMeta, SsTable};
use crate::lsm_storage::BlockCache;
use crate::block::{BlockBuilder, Block};
use bytes::{Bytes, BufMut};
use std::collections::BTreeSet;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    blockbuilder: BlockBuilder,
    blocks: BTreeSet<Block>,
    block_size: usize,
    offset: u32,
    // Add other fields you need.
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        let blockbuilder = BlockBuilder::new(block_size);
        let meta = Vec::new();
        let blocks = BTreeSet::new();

        Self {
            meta, 
            blockbuilder, 
            blocks,
            block_size, 
            offset: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.blockbuilder.add(key, value) {
            return;

        } else {
            // block don't contain enough capacity and we need split it.
            let block = self.blockbuilder.build_ref(); 
            
            self.offset += self.block_size as u32;
            self.blocks.insert(block);

            let new_block_builder = BlockBuilder::new(self.block_size);
            
            self.blockbuilder = new_block_builder;
            self.blockbuilder.add(key, value);
        }
    }

    /// Get the estimated size of the SSTable.
    /// Since the data blocks contain much more data than meta blocks, just return the size of data blocks here.
    pub fn estimated_size(&self) -> usize {
        return self.blocks.iter().fold(0, |acc, block| acc + block.size() );   
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // push last block
        if self.blockbuilder.num_of_elements != 0 {
            let last_block = self.blockbuilder.build_ref();
            self.blocks.insert(last_block);
        }

        let block_meta_offset = self.total_block_size();
        let mut offset: u32 = 0;

        let blocks = self.blocks.into_iter().map(|block| {
            self.meta.push(BlockMeta {
                offset,
                first_key: Bytes::from(block.first_key()),

            });
            offset += self.block_size as u32;
            block.encode()
        });
        let mut file = Vec::new();

        for block in blocks {
            file.extend_from_slice(&block);
        }

        BlockMeta::encode_block_meta(&self.meta, &mut file);
        
        //put block_meta_offset
        file.put_u32(block_meta_offset as u32);
        
        Ok(SsTable {
            file: super::FileObject(Bytes::from(file)),
            block_metas: self.meta,
            block_meta_offset,
        })
    }

    fn total_block_size(&self) -> usize {
        self.blocks.len() * self.block_size
    }

    fn total_meta_size(&self)-> usize {
        self.meta.iter().fold(0, |acc, meta| acc + meta.size())
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}


#[cfg(test)]
mod test {
    use std::path::PathBuf;


    use super::SsTableBuilder;
    use super::SsTable;

  #[test]
  fn builder_test1() {
    let mut builder: SsTableBuilder = SsTableBuilder::new(16);
    builder.add(b"1", b"1111");
    builder.add(b"3", b"3333");
    builder.add(b"2", b"2222");

    let path = PathBuf::from(".\test").join("my_test");
    let mut table: SsTable = builder.build(1, None, path).unwrap();
    let meta = table.block_metas;
    let file = SsTable::open_for_test(table.file).unwrap();
    assert_eq!(meta, file.block_metas)
    
  } 

  #[test]
  fn builder_test2() {
    let mut builder: SsTableBuilder = SsTableBuilder::new(128);
    builder.add(b"key_1", b"value_1");
    builder.add(b"key_2", b"value_2");
    builder.add(b"key_3", b"value_3");

    let path = PathBuf::from(".\test").join("my_test");
    let mut table: SsTable = builder.build(1, None, path).unwrap();
    let meta = table.block_metas;
    let file = SsTable::open_for_test(table.file).unwrap();
    assert_eq!(meta, file.block_metas)
    
  } 

} 