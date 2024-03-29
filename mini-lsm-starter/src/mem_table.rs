
use std::ops::Bound;
use std::sync::Arc;

use anyhow::{Result, Ok};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::table::SsTableBuilder;
use crate::debug::as_bytes;
/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create() -> Self {
        Self { map: Arc::new(SkipMap::new()) }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        //Debug
        // println!("[MemIetrator Debug]: Get value from key {:?}", as_bytes(key));
        self.map.get(key).map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        // Debug
        // if value.is_empty() {
        //     println!("[MemIetrator Debug]: Delete key {:?} : value {:?}", as_bytes(key), as_bytes(value));
        // } else {
        //     println!("[MemIetrator Debug]: Put key {:?} : value {:?}", as_bytes(key), as_bytes(value));
        // }

        self.map.insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));

    }

    // pub fn delete(&self, key: &[u8]) {
    //     println!("[MemIetrator Debug]: Delete key {:?}", as_bytes(key));
    //     self.put(key, &[]);
    // }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let (low, high) = (map_bound(lower), map_bound(upper));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((low, high)),
            item: (Bytes::from_static(&[]), Bytes::from_static(&[])),
        }.build();

        let entry = iter.with_iter_mut(|iter| MemTableIterator::entry_to_kv(iter.next()));
        iter.with_mut(|x| *x.item = entry);
        iter
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            //Debug
            println!("[Memtable Debug]: Flush key {:?} : value: {:?}", entry.key(), entry.value());
            builder.add(&entry.key(), &entry.value());
        }
        Ok(())
    }
}

pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(key ) => Bound::Included(Bytes::copy_from_slice(key)),
        
        Bound::Excluded(key) => Bound::Excluded(Bytes::copy_from_slice(key)),

        Bound::Unbounded => Bound::Unbounded,
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`.
#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_kv(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}


impl MemTableIterator {
    pub fn debug(&mut self) -> Result<()> {
        while self.is_valid() {
            let key = self.key();
            let value = self.value();
            println!("[MemIetrator Debug]: Contain key {:?} : value {:?}", as_bytes(key), as_bytes(value));
            self.next();
        }

        Ok(())
    }

}

impl StorageIterator for MemTableIterator {
    fn key(&self) -> &[u8] {
        &self.borrow_item().0
    }

    fn value(&self) -> &[u8] {
        &self.borrow_item().1   
    }


    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_kv(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
