#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::{Bound};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{RwLock, Mutex};
use crate::debug::as_bytes;

use crate::block::Block;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::{map_bound, MemTable, MemTableIterator};
use crate::table::{SsTable, SsTableIterator, SsTableBuilder};

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
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    flush_lock: Mutex<()>,
    block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    path: PathBuf,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            flush_lock: Mutex::new(()),
            block_cache: Arc::new(BlockCache::new(1 << 20)),
            next_sst_id: AtomicUsize::new(1),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let mut value = None;

        let inner = {
            let lock = self.inner.read();
            Arc::clone(&lock)
        };

        value = inner.memtable.get(key);
        if value.as_ref().is_some_and(|v| v.is_empty()) { return Ok(None); }

        if value.is_none() {
            let imem_iter = inner.imm_memtables.iter().rev();

            for imem in imem_iter {
                if let Some(v) = imem.get(key) {
                    if !v.is_empty() { value = Some(v); }
                }
                break;
            }

            if value.is_none() {
                let l0_iter = inner.l0_sstables.iter().rev();

                for sst in l0_iter {
                    let sst_iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key)?;
                    if key == sst_iter.key() {
                        if !sst_iter.value().is_empty() {
                            value = Some(Bytes::copy_from_slice(sst_iter.value()));
                        }
                        break;
                    }
                }
            }
            
        }

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
    /// In day 3: flush the (current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        let flush_lock = self.flush_lock.lock();
        let mut flush_table;
        let sst_id;

        
        // Insert the active memtable into imm memtable
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            let memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::create()));
            flush_table = memtable.clone();
            sst_id = self.next_sst_id();

            snapshot.imm_memtables.push(memtable);
            *guard = Arc::new(snapshot);
        }

        // Flush the active memtable into sstable
        let mut sst_builder = SsTableBuilder::new(4096);
        flush_table.flush(&mut sst_builder);
        let sst = Arc::new(sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()), 
            self.path_of_sst(sst_id)
        )?);

        // Remove the active table from imm table
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();   
            snapshot.imm_memtables.pop();
            snapshot.l0_sstables.push(sst);
            
            *guard = Arc::new(snapshot);
        }


        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.inner.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);

        //Debug
        // let mut active_memiter = Box::new(snapshot.memtable.scan(lower, upper));
        // active_memiter.debug();

        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        
        for memtable in snapshot.imm_memtables.iter().rev() {
            let mut iter: MemTableIterator = memtable.scan(lower, upper);
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut table_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table in snapshot.l0_sstables.iter().rev() {
            let mut iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(table.clone(), key)?
                }
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(table.clone(), key)?;
                    println!("[fun scan Debug]: key {:?} : value {:?}", as_bytes(iter.key()), as_bytes(iter.value()));
                    println!("[fun scan Debug]: is_valid {:?}", iter.is_valid());
                    
                    if iter.is_valid() && iter.key() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table.clone())?,
            };
            // Debug
            // iter.debug();
            
            table_iters.push(Box::new(iter));
        }
        let table_iter = MergeIterator::create(table_iters);

        let iter = TwoMergeIterator::create(memtable_iter, table_iter)?;

        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
        )?))

    }

    fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn path_of_sst(&self, id: usize) -> PathBuf {
        self.path.join(format!("{:05}.sst", id))
    }
}
