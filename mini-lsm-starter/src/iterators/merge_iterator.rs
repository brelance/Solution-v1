use std::borrow::BorrowMut;
use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::ops::Deref;
use std::{iter, clone};

use anyhow::{Ok, Result};

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    // current: HeapWrapper<I>,
    current: Option<HeapWrapper<I>>
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap: BinaryHeap<HeapWrapper<I>> = BinaryHeap::new();

        if iters.is_empty() {
            return Self {
                iters: heap,
                current: None,
            }
        }


        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                let heapwrapper = HeapWrapper(index, iter);
                heap.push(heapwrapper);
            }
        }

        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current)
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        self.current
            .as_ref()
            .map_or(&[], |iter| iter.1.key())
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map_or(&[], |iter| iter.1.value())
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map_or(false, |iter| iter.1.is_valid())
    }

    
    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        let current = self.current.as_mut().unwrap();
        // let mut current = unsafe {self.current.as_mut().unwrap_unchecked() };
        
        while let Some(mut next_iter) = self.iters.peek_mut() {
            if current.1.key() == next_iter.1.key() {
                if let error @ Err(_) = next_iter.1.next() {
                    PeekMut::pop(next_iter);
                    return error;
                }

                if !next_iter.1.is_valid() {
                    PeekMut::pop(next_iter);
                }
            } else {
                break;
            }

        }   

        current.1.next()?;
        
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        if let Some(mut next_iter) = self.iters.peek_mut() {
            if current < &mut *next_iter {
                std::mem::swap(current, &mut *next_iter);
            }
        }

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;

    use std::cmp;

    #[derive(Debug)]
    struct ReverseOrder (pub usize);
    impl PartialEq for ReverseOrder {
        fn eq(&self, other: &Self) -> bool {
            self.0 == self.0
        }
    }

    impl Eq for ReverseOrder {}

    impl PartialOrd for ReverseOrder {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            match self.0.cmp(&other.0) {
                cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
                cmp::Ordering::Less => Some(cmp::Ordering::Less),
                cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
            }
            .map(|x| x.reverse())
        }
    }
    

    impl Ord for ReverseOrder {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.partial_cmp(other).unwrap()
        }
    }

    #[test]
    fn test_heap() {
        let test = ReverseOrder(64);
        let test1 = ReverseOrder(32);
        let test2 = ReverseOrder(16);
        let test3 = ReverseOrder(8);
        let mut heap = BinaryHeap::new();
        
        heap.push(test3);
        heap.push(test2);
        heap.push(test);
        heap.push(test1);

        assert_eq!(heap.peek().unwrap().0, 8);

    }

    #[test]
    fn test_heap2() {
        let test = 64;
        let test1 = 32;
        let test2 = 16;
        let test3 = 8;
        let mut heap = BinaryHeap::new();
        
        heap.push(test3);
        heap.push(test2);
        heap.push(test);
        heap.push(test1);

        assert_eq!(*heap.peek().unwrap(), 64);
    }
}