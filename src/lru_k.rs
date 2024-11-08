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
use crate::error;
use crate::error::BufferError;
use std::collections::HashMap;
use std::time::Duration;

// Timestamp corresponds to nanoseconds
type Timestamp = u128;
type PageId = u32;


/// HistoryBlock maintains access history (HIST and LAST) for specific page data.
/// - Maintains K most recent uncorrelated references in hist
/// - Tracks most recent reference (correlated or not) in last
#[derive(Debug, Clone)]
struct HistoryBlock {
    // hist is the array of uncorrelated references.
    // So, it contains K most recent references, excluding correlated ones
    // hist[0] is most recent, hist[K-1] is oldest
    hist: Vec<Timestamp>,
    // last is the most recent reference time (including correlated ones).
    // Updated on EVERY reference, whether correlated or not.
    last: Timestamp,
    // dirty indicates if page needs to be written back to disk
    dirty: bool,
    // Actual page data.
    data: Vec<u8>,
}

struct LRUKBuffer {
    _c: usize,
    // k is the number of history references to track (the 'K' in LRU-K)
    k: usize,
    // correlated_reference_period corresponds to the time window to determine
    // if references are correlated.
    // References within this period are considered part of the same access pattern
    correlated_reference_period: Timestamp,
    // Maximum number of pages in buffer
    buffer_size: usize,
    // Main page table: maps page IDs to their history blocks
    buffer: HashMap<PageId, HistoryBlock>,
    // Current number of pages in buffer
    current_size: usize,
}


impl HistoryBlock {
    /// Creates a new history block for a page
    /// Initializes first reference as both HIST[0] and LAST
    fn new(k: usize, current_time: Timestamp, page_data: Vec<u8>) -> Self {
        let mut hist = vec![0; k];
        hist[0] = current_time;  // First reference becomes history

        HistoryBlock {
            hist,
            last: current_time,
            dirty: false,
            data: page_data,
        }
    }

    fn get_backward_k_distance(&self, k: usize, current_time: Timestamp) -> error::Result<Timestamp> {
        if self.hist.len() <= k - 1 {
            return Err(BufferError {
                detail: String::from("HIST is not populated"),
            });
        }

        if self.hist.iter().any(|&ts| ts == 0) {
            return Ok(Timestamp::MIN);
        }

        Ok(current_time - self.hist[k - 1])
    }

    /// Updates history for a correlated reference
    /// Only updates LAST timestamp, preserving HIST values
    /// Called when reference is within correlated_reference_period of last reference
    fn update_correlated(&mut self, current_time: Timestamp) {
        // For correlated references, we only update LAST
        // This prevents sequential scans from polluting history
        self.last = current_time;
    }

    /// Updates history for an uncorrelated reference
    /// Updates both HIST and LAST values
    /// Called when reference is outside correlated_reference_period of last reference
    fn update_uncorrelated(&mut self, current_time: Timestamp, k: usize) {
        // Calculate the duration of the previous correlated reference period
        let correlation_period = self.last - self.hist[0];

        // Shift historical references and adjust timestamps
        for i in (1..k).rev() {
            // Each previous reference is pushed back by the correlation period
            self.hist[i] = self.hist[i - 1] + correlation_period;
        }

        // New uncorrelated reference becomes most recent history
        self.hist[0] = current_time;
        self.last = current_time;
    }
}

fn current_time(add_seconds: Option<u64>) -> Timestamp {
    let current = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();

    add_seconds
        .map_or(current, |secs| {
            current.checked_add(std::time::Duration::from_secs(secs))
                .unwrap_or(current)
        })
        .as_nanos()
}

impl LRUKBuffer {
    fn new(k: usize, buffer_size: usize, correlated_ref_period_secs: Timestamp) -> Self {
        let seconds = seconds_to_nanos(correlated_ref_period_secs as u64);
        if seconds.is_none() {
            panic!("invalid second provided to LRU-K Buffer")
        }

        LRUKBuffer {
            _c: 0,
            k,
            correlated_reference_period: seconds.unwrap(),
            buffer_size,
            buffer: HashMap::new(),
            current_size: 0,
        }
    }

    pub fn ref_page(&mut self, page_id: PageId, page_data: Option<Vec<u8>>) -> Result<(), String> {
        self._ref_page(page_id, page_data, current_time(None))
    }

    fn _ref_page(&mut self, page_id: PageId, page_data: Option<Vec<u8>>, current_time: Timestamp) -> Result<(), String> {
        if let Some(block) = self.buffer.get_mut(&page_id) {
            // 1) Page is already in buffer
            // Determine if this is a correlated reference by checking time since last reference.
            if current_time - block.last > self.correlated_reference_period {
                // UNCORRELATED REF: significant time has passed
                // Update both HIST and LAST
                block.update_uncorrelated(current_time, self.k);
            } else {
                // CORRELATED REF: part of same access pattern
                // Update only LAST, no need to update HIST as it only contains
                // uncorrelated reference history.
                block.update_correlated(current_time);
            }

            let eq = match page_data {
                Some(v) => v == block.data,
                None => false
            };
            if !eq {
                block.dirty = true;
            }

            Ok(())
        } else {
            // 2) Page is not in the buffer
            // Check if we need to evict a page
            if self.current_size >= self.buffer_size {
                if self.evict(current_time).is_none() {
                    return Err(String::from("failed to evict a page"));
                }
            }

            // Initialize new page's history
            let page_data = page_data.ok_or("Page data required for new pages")?;
            let new_block = HistoryBlock::new(self.k, current_time, page_data);
            self.buffer.insert(page_id, new_block);
            self.current_size += 1;

            Ok(())
        }
    }

    fn evict(&mut self, current_time: Timestamp) -> Option<HistoryBlock> {
        let mut victim_id = None;
        let mut max_backward_k_dist = Timestamp::MIN;

        for (page_id, block) in self.buffer.iter() {
            if current_time < block.last {
                continue;
            }

            let dist = current_time - block.last;

            if dist > self.correlated_reference_period {
                if let Ok(backward_k_dist) = block.get_backward_k_distance(self.k, current_time) {
                    if backward_k_dist >= max_backward_k_dist {
                        max_backward_k_dist = backward_k_dist;
                        victim_id = Some(*page_id);
                    }
                }
            }
        }

        if let Some(victim) = victim_id {
            // Write back dirty page if needed
            if let Some(block) = self.buffer.get_mut(&victim) {
                if block.dirty {
                    // TODO: write to disk
                    println!("Writing back dirty page {}", victim);
                    self._c += 1;
                    block.dirty = false;
                }
            }

            // Remove victim from buffer
            let block = self.buffer.remove(&victim);
            self.current_size -= 1;

            return block;
        }

        None
    }
}

fn seconds_to_nanos(seconds: u64) -> Option<u128> {
    let duration = Duration::from_secs(seconds);
    Some(duration.as_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    fn create_page_data(value: u8) -> Vec<u8> {
        vec![value; 4096]  // 4KB page
    }

    #[test]
    fn test_backward_k_distance_calculation() {
        let k = 2;
        let current_time = 6;

        let page1 = HistoryBlock {
            hist: vec![4, 1],
            last: 4,
            dirty: false,
            data: vec![1],
        };

        let page2 = HistoryBlock {
            hist: vec![5, 2],
            last: 5,
            dirty: false,
            data: vec![2],
        };

        let page3 = HistoryBlock {
            hist: vec![3, 0],
            last: 3,
            dirty: false,
            data: vec![3],
        };

        let k_dist = page1.get_backward_k_distance(k, current_time);
        assert!(k_dist.is_ok());
        assert_eq!(k_dist.unwrap(), 5);

        let k_dist = page2.get_backward_k_distance(k, current_time);
        assert!(k_dist.is_ok());
        assert_eq!(k_dist.unwrap(), 4);

        let k_dist = page3.get_backward_k_distance(k, current_time);
        assert!(k_dist.is_ok());
        assert_eq!(k_dist.unwrap(), 0);
    }

    #[test]
    fn simple_eviction() {
        let mut buffer = LRUKBuffer::new(2, 3, 1);

        buffer._ref_page(1, Some(vec![1]), current_time(None)).unwrap();
        buffer._ref_page(2, Some(vec![2]), current_time(None)).unwrap();
        buffer._ref_page(1, Some(vec![1]), current_time(Some(3))).unwrap();

        let res = buffer.evict(current_time(Some(5))).unwrap();
        assert_eq!(res.data, vec![1]);
    }

    #[test]
    fn multi_candidate_eviction() {
        let mut buffer = LRUKBuffer::new(2, 3, 1);

        buffer._ref_page(1, Some(vec![1]), current_time(None)).unwrap();
        buffer._ref_page(2, Some(vec![2]), current_time(None)).unwrap();
        buffer._ref_page(1, Some(vec![1]), current_time(Some(2))).unwrap();
        buffer._ref_page(2, Some(vec![2]), current_time(Some(4))).unwrap();

        let res = buffer.evict(current_time(Some(10))).unwrap();
        assert_eq!(res.data, vec![1]);
    }

    #[test]
    fn test_dirty_page() {
        let mut buffer = LRUKBuffer::new(
            2, 3, 20,
        );

        let page_data = create_page_data(1);
        buffer.ref_page(1, Some(page_data)).unwrap();
        buffer.ref_page(1, Some(create_page_data(2))).unwrap();
        let latest_history = buffer.buffer.get(&1).unwrap();
        assert_eq!(latest_history.dirty, true, "HIST should not change for correlated reference");
    }

    #[test]
    fn test_correlated_references() {
        let mut buffer = LRUKBuffer::new(
            2, 3, 20,
        );

        // First reference to page 1
        let page_data = create_page_data(1);
        buffer.ref_page(1, Some(page_data)).unwrap();

        let initial_hist = buffer.buffer.get(&1).unwrap().hist[0];
        let initial_last = buffer.buffer.get(&1).unwrap().last;
        assert_eq!(initial_hist, initial_last, "Initial HIST and LAST should be equal");

        buffer.ref_page(1, None).unwrap();

        let latest_history = buffer.buffer.get(&1).unwrap();
        assert_eq!(latest_history.hist[0], initial_hist, "HIST should not change for correlated reference");
        assert!(latest_history.last > initial_last, "LAST should update for correlated reference");
    }

    #[test]
    fn test_uncorrelated_references() {
        let mut buffer = LRUKBuffer::new(2, 3, 1);

        buffer.ref_page(1, Some(create_page_data(1))).unwrap();
        let initial_hist = buffer.buffer.get(&1).unwrap().hist[0];

        sleep(Duration::from_secs(2));

        buffer.ref_page(1, None).unwrap();

        let block = buffer.buffer.get(&1).unwrap();
        assert!(block.hist[0] > initial_hist, "HIST should update for uncorrelated reference");
        assert_eq!(block.hist[0], block.last, "HIST[0] and LAST should be equal for new uncorrelated reference");
    }

    #[test]
    fn test_buffer_eviction() {
        let mut buffer = LRUKBuffer::new(2, 2, 1);

        buffer.ref_page(1, Some(create_page_data(1))).unwrap();
        buffer.ref_page(2, Some(create_page_data(2))).unwrap();

        assert_eq!(buffer.current_size, 2, "Buffer should be full");

        // Wait to ensure pages are eligible for eviction
        sleep(Duration::from_secs(2));

        // Add new page, forcing eviction
        buffer.ref_page(3, Some(vec![3])).unwrap();

        assert_eq!(buffer.current_size, 2, "Buffer size should remain at capacity");
        assert!(buffer.buffer.contains_key(&3), "New page should be in buffer");
        assert!(!buffer.buffer.contains_key(&1) || !buffer.buffer.contains_key(&2),
                "One of the original pages should be evicted");
    }

    #[test]
    fn test_dirty_page_eviction() {
        let mut buffer = LRUKBuffer::new(2, 2, 5);

        buffer.ref_page(1, Some(create_page_data(1))).unwrap();
        if let Some(block) = buffer.buffer.get_mut(&1) {
            block.dirty = true;
        }

        buffer.ref_page(2, Some(create_page_data(2))).unwrap();

        sleep(Duration::from_secs(6));

        buffer.ref_page(3, Some(create_page_data(3))).unwrap();

        assert_eq!(buffer.current_size, 2, "Buffer size should remain at capacity");
    }

    #[test]
    fn test_mixed_reference_patterns() {
        let mut buffer = LRUKBuffer::new(2, 3, 1);

        buffer.ref_page(1, Some(create_page_data(1))).unwrap();
        buffer.ref_page(2, Some(create_page_data(2))).unwrap();
        buffer.ref_page(1, None).unwrap();
        sleep(Duration::from_secs(2));

        buffer.ref_page(1, None).unwrap();

        let block = buffer.buffer.get(&1).unwrap();
        assert_eq!(block.hist.len(), 2);
        assert!(block.hist[1] > 0, "Should have historical reference");
        assert!(block.hist[0] > block.hist[1], "First reference must be the recent one");
        assert_eq!(block.last, block.hist[0]);
        assert_eq!(block.dirty, true);
    }
}