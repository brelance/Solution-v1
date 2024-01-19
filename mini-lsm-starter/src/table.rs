#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, Ok};
pub use builder::SsTableBuilder;
use bytes::{Buf, Bytes, BufMut};
pub use iterator::SsTableIterator;

use crate::block::{Block, BLOCK_SIZE};
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: u32,
    /// The first key of the data block, mainly used for index purpose.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    // pub fn new(offset, first_key: Bytes) -> Self {
    //     Self { offset: (), first_key: () }
    // }
    
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            buf.put_u32(meta.offset);
            buf.put_u16(meta.first_key.len() as u16);
            buf.extend_from_slice(&meta.first_key);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut metas = Vec::new();
        
        while buf.has_remaining() {
            let offset = buf.get_u32();
            let key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(key_len);

            metas.push(BlockMeta {
                offset,
                first_key,
            })
        } 

        metas
    }

    fn size(&self) -> usize {
        self.first_key.len() + 4 + 2
    }
}

/// A file object.
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn read_to_buf(&self, offset: u64, buf: &mut [u8]) {
        let src = &self.0[offset as usize..(offset as usize + buf.len()) as usize];

        buf.copy_from_slice(src);
    } 

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        unimplemented!()
    }

    pub fn open(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

/// -------------------------------------------------------------------------------------------------------
/// |              Data Block             |             Meta Block              |          Extra          |
/// -------------------------------------------------------------------------------------------------------
/// | Data Block #1 | ... | Data Block #N | Meta Block #1 | ... | Meta Block #N | Meta Block Offset (u32) |
/// -------------------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    file: FileObject,
    /// The meta blocks that hold info for data blocks.
    block_metas: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let offset = file.size() - 4;
        let mut buf = [0u8; 4];

        file.read_to_buf(offset, &mut buf);
        
        let block_meta_offset = u32::from_be_bytes(buf) as usize;
        let upbound = offset;
        
        let mut key_len_buf = [0u8; 2];
        let mut block_metas = Vec::new();
        let mut meta_offset = block_meta_offset as u64;

        while meta_offset < upbound {
            file.read_to_buf(meta_offset, &mut buf);
            let block_offset: u32 = u32::from_be_bytes(buf);
            meta_offset += 4;

            file.read_to_buf(meta_offset, &mut key_len_buf);
            let key_len = u16::from_be_bytes(key_len_buf) as u64;
            meta_offset += 2;

            let first_key = file.read(meta_offset, key_len)?;
            block_metas.push(
                BlockMeta {
                    offset: block_offset,
                    first_key: first_key.into()
                }
            );
            
            meta_offset += key_len;
        }

        Ok(SsTable { file, block_metas, block_meta_offset, })
        
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta = &self.block_metas[block_idx];
        
        let block_slice = self.file.read(meta.offset as u64, BLOCK_SIZE as u64)?;
        let block = Block::decode(&block_slice);

        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let mut left = 0;
        let mut right = self.block_metas.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let cur_key: Vec<u8> = self.block_metas[mid].first_key.to_vec();

            match cur_key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    right = mid;
                }
                std::cmp::Ordering::Equal => {
                    break;
                }
            }
        }

        left
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
