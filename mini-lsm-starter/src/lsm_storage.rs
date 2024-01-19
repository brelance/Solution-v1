#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::borrow::BorrowMut;
use std::mem;
use std::ops::{Bound, DerefMut};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::RwLock;

use crate::block::Block;
use crate::iterators::StorageIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::{MemTable, MemTableIterator};
use crate::table::{SsTable, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SsTables, from earliest to latest.
    l0_sstables: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    #[allow(dead_code)]
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SSTable ID.
    next_sst_id: usize,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: 1,
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let inner = self.inner.read();
        let mem_iter  = inner.memtable.scan(Bound::Included(key), Bound::Included(key));
        let imem_iter = inner.imm_memtables.iter();
        let l0_iter = inner.l0_sstables.iter();
        
        let mut value = None;
        if mem_iter.key() == key {
            value = Some(Bytes::copy_from_slice(mem_iter.value()));
            return Ok(value);
        }
        // imem_iter.map(|memtable| {
        //     if let Some(v) = memtable.get(key) { value = Some(v) }
        // });
        for imem in imem_iter {
            if let Some(v) = imem.get(key) { value = Some(v) }
        }
        if value.is_some() { return Ok(value); }


        for sst in l0_iter {
            let sst_iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key)?;
            if key == sst_iter.key() {
                value = Some(Bytes::copy_from_slice(sst_iter.value()));
            }
        }
        // l0_iter.map(|sst| {
        //     let mut sst_iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key)
        //         .expect("Error: from Level 0 Sstable");
        //     if key == sst_iter.key() { value = Some(Bytes::copy_from_slice(sst_iter.value())); }
        // });

        Ok(value)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");

        let inner = self.inner.write();
        inner.memtable.put(key, value);
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let inner = self.inner.write();
        inner.memtable.put(_key, &[]);
        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        unimplemented!()
        // let mut inner = self.inner.write();
        // let memtable = inner.memtable.clone();

        // Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        unimplemented!()
    }
}
