#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::borrow::BorrowMut;
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use anyhow::Result;

use super::{BlockMeta, SsTable};
use crate::lsm_storage::BlockCache;
use crate::block::{BlockBuilder, Block, self};
use bytes::{Bytes, BytesMut, BufMut};
use std::collections::BTreeSet;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    blockbuilder: BlockBuilder,
    blocks: BTreeSet<Block>,
    block_size: usize,
    // is_first_key: bool,
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
            // is_first_key: true,
            offset: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if !self.blockbuilder.add(key, value) {
            //block is full
            let block = self.blockbuilder.build_ref();
            
            self.offset += self.block_size as u32;
            self.blocks.insert(block);
            let new_block = BlockBuilder::new(self.block_size);
            std::mem::replace(&mut self.blockbuilder, new_block);

            // self.is_first_key = true;
        }

        // if self.is_first_key {
        //     let meta: BlockMeta = BlockMeta {
        //         offset: self.offset,
        //         first_key: Bytes::copy_from_slice(key),
        //     };
        //     self.meta.push(meta);
        //     self.is_first_key = false;
        // }

        self.blockbuilder.add(key, value);
        
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

        let mut offset = 0;
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

    use bytes::Buf;
    use bytes::{BufMut, Bytes};

    use super::SsTableBuilder;
    use super::SsTable;

  #[test]
  fn builder_test() {
    let mut builder: SsTableBuilder = SsTableBuilder::new(16);
    builder.add(b"1", b"1111");
    builder.add(b"3", b"3333");
    builder.add(b"2", b"2222");

    // builder.add(b"4", b"4444");
    // builder.add(b"5", b"5555");

    // builder.add(b"6", b"6666");

    let path = PathBuf::from(".\test").join("my_test");
    let mut table: SsTable = builder.build(1, None, path).unwrap();
    let meta = table.block_metas;
    let file = SsTable::open_for_test(table.file).unwrap();
    assert_eq!(meta, file.block_metas)
    
  } 

  #[test]
  fn test_get_u32() {
    let mut v = Vec::new();
    
    v.put_u32(32);
    v.put_u32(48);

    let v_size = v.len();
    let mut bytes = Bytes::from(v);
    assert_eq!(bytes.get_u32(),32);
    assert_eq!(bytes.get_u32(),48); 
       
    assert_eq!(v_size, bytes.len());
  }

} 