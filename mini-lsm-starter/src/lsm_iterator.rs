#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use crate::iterators::{self, StorageIterator};
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::mem_table::MemTableIterator;
use crate::table::SsTableIterator;
pub struct LsmIterator {
    iterator: TwoMergeIterator<MemTableIterator, SsTableIterator>,
}

impl LsmIterator {
    pub fn new(mem_iter: MemTableIterator, sst_iter: SsTableIterator) -> Self {
        let iterator= TwoMergeIterator::create(mem_iter, sst_iter).expect("Error: from ism_iterator");
        Self { iterator }
    }
}

impl StorageIterator for LsmIterator {
    fn is_valid(&self) -> bool {
        self.iterator.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.iterator.key()
    }

    fn value(&self) -> &[u8] {
        self.iterator.value()
    }

    fn next(&mut self) -> Result<()> {
        self.iterator.next();

        while self.is_valid() && self.value().is_empty() {
            self.iterator.next();
        }
        
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn is_valid(&self) -> bool {
        unimplemented!()
    }

    fn key(&self) -> &[u8] {
        unimplemented!()
    }

    fn value(&self) -> &[u8] {
        unimplemented!()
    }

    fn next(&mut self) -> Result<()> {
        unimplemented!()
    }
}
