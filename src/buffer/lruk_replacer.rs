/*
LRU-K algorithm is described in https://www.cs.cmu.edu/~christos/courses/721-resources/p297-o_neil.pdf

- If page p is already in buffer:
    When we access a page p at time t:
    - Update history information
    - Check if it's a correlated or uncorrelated reference
    - Update LAST(p) and HIST(p) accordingly
        1. First, check correlation:
           - Calculate time_gap = t - LAST(p)
           - Compare with Correlated_Reference_Period
        2. If time_gap ≤ Correlated_Reference_Period (CORRELATED REF):
           - This is a correlated reference, like quick repeat visits to same page
           - Only update LAST(p) = t
           - Leave HIST(p) unchanged
        3. If time_gap > Correlated_Reference_Period (UNCORRELATED REF):
           - This is an uncorrelated reference; significant time has passed since last access
           - Update both LAST(p) and HIST(p)
           - Steps:
             a. Calculate correlation period = LAST(p) - HIST(p,1)
#[derive(PartialEq)]
             b. Shift all HIST values right
             c. Set HIST(p,1) = t
             d. Set LAST(p) = t
- Otherwise, if page p is not in buffer:
    - Find a victim page to replace
    - Write back victim if dirty
    - Fetch new page p
    - Initialize/update its history

    1. Check if buffer is full:
       If full, need to find victim to evict:
       - Initialize min_hist_k = MAX_VALUE
       - For each page q in buffer:
         a. Check if eligible for eviction:
            - If t - LAST(q) > Correlated_Reference_Period
            - Means page q is not in active use
         b. If eligible and HIST(q,K) < min_hist_k:
            - This page has oldest Kth reference
            - Mark as potential victim
            - Update min_hist_k = HIST(q,K)
    2. If victim found and is dirty:
       - Write victim's data back to disk
       - Remove victim from buffer
    3. Fetch new page p:
       - Read page p from disk
       - Allocate buffer frame
    4. Initialize history for new page:
       - Create new HIST block
       - Set HIST(p,1) = t
       - Set LAST(p) = t
       - All other HIST entries = 0

Notes:
- References to the same page can be either "correlated" or "uncorrelated".
- "Correlated references" are the ones that refer to the same page very close in time.
For ex, while a query scans a table, it might access the same page multiple times
quickly. So, the references to those pages in this case can be called as 'correlated'.
- "Uncorrelated references" are the ones to the same page that are separated by a significant time gap.

*/
use crate::buffer::types::FrameId;
use crate::common::time::Timestamp;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug)]
pub struct LRUKNode {
    pin_count: AtomicUsize,
    // hist is the array of uncorrelated references.
    // So, it contains K most recent references, excluding correlated ones
    // hist[0] is most recent, hist[K-1] is oldest
    hist: VecDeque<Timestamp>,
    // last is the most recent reference time (including correlated ones).
    // Updated on EVERY reference, whether correlated or not.
    last: Timestamp,
    // dirty indicates if page needs to be written back to disk
    dirty: bool,
    evictable: bool,
}

impl LRUKNode {
    fn new(k: usize, ts: Timestamp) -> Self {
        let mut hist = VecDeque::with_capacity(k);
        hist.push_back(ts);
        LRUKNode {
            pin_count: AtomicUsize::new(0),
            hist,
            last: ts,
            dirty: false,
            evictable: false,
        }
    }

    pub fn is_evictable(&self) -> bool {
        // self.get_pin_count() == 0
        self.evictable
    }

    fn increment_pin_count(&self) {
        self.pin_count.fetch_add(1, Ordering::SeqCst);
    }

    fn decrement_pin_count(&self) -> usize {
        self.pin_count.fetch_sub(1, Ordering::SeqCst)
    }

    fn get_pin_count(&self) -> usize {
        self.pin_count.load(Ordering::SeqCst)
    }

    fn get_backward_k_distance(&self, k: usize, current_time: Timestamp) -> Timestamp {
        if self.hist.len() <= k - 1 {
            return Timestamp::MAX;
        }

        current_time - self.hist[0]
    }
}

pub struct LRUKReplacer {
    // k is the number of history references to track (the 'K' in LRU-K)
    k: usize,
    // Maximum number of pages in buffer
    buffer_size: usize,
    // Main page table: maps page IDs to their history blocks
    buffer: HashMap<FrameId, LRUKNode>,
    // size represents the number of evictable frames
    size: usize,
    current_timestamp_: Timestamp,
}


impl LRUKReplacer {
    fn new(k: usize, buffer_size: usize) -> Self {
        LRUKReplacer {
            k,
            buffer_size,
            buffer: HashMap::new(),
            size: 0,
            current_timestamp_: Timestamp::MIN,
        }
    }

    pub fn evict(&mut self) -> Option<FrameId> {
        if self.size == 0 {
            return None;
        }

        let mut victim_id: Option<FrameId> = None;
        let mut victim_last = Timestamp::MIN;
        let mut max_backward_k_dist = Timestamp::MIN;

        for (page_id, frame) in self.buffer.iter() {
            if !frame.is_evictable() {
                continue;
            }

            let k_dist = frame.get_backward_k_distance(self.k, self.current_timestamp_);
            // whenever we have multiple +inf, need to evict the oldest one.
            if k_dist > max_backward_k_dist {
                victim_id = Some(*page_id);
                victim_last = frame.last;
                max_backward_k_dist = k_dist;
            } else if k_dist == Timestamp::MAX && frame.last < victim_last {
                victim_id = Some(*page_id);
                victim_last = frame.last;
            }
        }

        if let Some(v_id) = victim_id {
            let removed = self.buffer.remove(&v_id);
            if removed.is_some() {
                self.size -= 1;
            }

            return Some(v_id);
        }

        None
    }

    pub fn record_access(&mut self, page_id: FrameId) {
        match self.buffer.get_mut(&page_id) {
            None => {
                self.buffer.insert(page_id, LRUKNode::new(self.k, self.current_timestamp_));
            }
            Some(frame) => {
                if frame.hist.len() == self.k {
                    frame.hist.pop_front();
                }

                frame.hist.push_back(self.current_timestamp_);
                frame.last = self.current_timestamp_;
            }
        }

        self.current_timestamp_ += 1;
    }

    pub fn remove(&mut self, page_id: FrameId) -> Result<Option<LRUKNode>, String> {
        if let Some(page_to_delete) = self.buffer.get(&page_id) {
            if !page_to_delete.is_evictable() {
                return Err(String::from("the page is not evictable"));
            }
        }


        self.buffer.remove(&page_id).map_or(Ok(None), |frame| {
            Ok(Some(frame))
        })
    }

    pub fn set_evictable(&mut self, page_id: FrameId, evictable: bool) {
        if let Some(frame) = self.buffer.get_mut(&page_id) {
            if frame.is_evictable() && !evictable {
                frame.evictable = evictable;
                self.size -= 1;
            } else if !frame.is_evictable() && evictable {
                frame.evictable = evictable;
                self.size += 1;
            }
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

fn seconds_to_nanos(seconds: u64) -> Option<u128> {
    let duration = Duration::from_secs(seconds);
    Some(duration.as_nanos())
}

#[cfg(test)]
mod tests {
    use crate::buffer::lruk_replacer::LRUKReplacer;

    #[test]
    fn test_lruk_simple() {
        let mut replacer = LRUKReplacer::new(2, 7);
        replacer.record_access(1);
        replacer.record_access(2);
        replacer.record_access(3);
        replacer.record_access(4);
        replacer.record_access(5);
        replacer.record_access(6);
        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);
        replacer.set_evictable(3, true);
        replacer.set_evictable(4, true);
        replacer.set_evictable(5, true);
        replacer.set_evictable(6, false);

        assert_eq!(replacer.size(), 5);

        replacer.record_access(1);
        assert_eq!(2, replacer.evict().unwrap());
        assert_eq!(3, replacer.evict().unwrap());
        assert_eq!(4, replacer.evict().unwrap());
        assert_eq!(2, replacer.size());

        // Insert new frames [3, 4], and update the access history for 5. Now, the ordering is [3, 1, 5, 4].
        replacer.record_access(3);
        replacer.record_access(4);
        replacer.record_access(5);
        replacer.record_access(4);
        replacer.set_evictable(3, true);
        replacer.set_evictable(4, true);
        assert_eq!(4, replacer.size());

        // Look for a frame to evict. We expect frame 3 to be evicted next.
        assert_eq!(3, replacer.evict().unwrap());
        assert_eq!(3, replacer.size());

        // Set 6 to be evictable. 6 Should be evicted next since it has the maximum backward k-distance.
        replacer.set_evictable(6, true);
        assert_eq!(4, replacer.size());
        assert_eq!(6, replacer.evict().unwrap());
        assert_eq!(3, replacer.size());

        // Mark frame 1 as non-evictable. We now have [5, 4].
        replacer.set_evictable(1, false);

        // We expect frame 5 to be evicted next.
        assert_eq!(2, replacer.size());
        assert_eq!(5, replacer.evict().unwrap());
        assert_eq!(1, replacer.size());

        // Update the access history for frame 1 and make it evictable. Now we have [4, 1].
        replacer.record_access(1);
        replacer.record_access(1);
        replacer.set_evictable(1, true);
        assert_eq!(2, replacer.size());

        // Evict the last two frames.
        assert_eq!(4, replacer.evict().unwrap());
        assert_eq!(1, replacer.size());
        assert_eq!(1, replacer.evict().unwrap());
        assert_eq!(0, replacer.size());

        // Insert frame 1 again and mark it as non-evictable.
        replacer.record_access(1);
        replacer.set_evictable(1, false);
        assert_eq!(0, replacer.size());

        // A failed eviction should not change the size of the replacer.
        let frame = replacer.evict();
        assert_eq!(None, frame);

        // Mark frame 1 as evictable again and evict it.
        replacer.set_evictable(1, true);
        assert_eq!(1, replacer.size());
        assert_eq!(1, replacer.evict().unwrap());
        assert_eq!(0, replacer.size());

        // There is nothing left in the replacer, so make sure this doesn't do something strange.
        let frame = replacer.evict();
        assert_eq!(None, frame);
        assert_eq!(0, replacer.size());

        // Make sure that setting a non-existent frame as evictable or non-evictable doesn't do something strange.
        replacer.set_evictable(6, false);
        replacer.set_evictable(6, true);
        assert_eq!(0, replacer.size());
    }
}