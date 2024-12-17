use super::error::{BPlusTreeError, BPlusTreeResult};
use super::serde::Serde;
use crate::buffer::types::{PageId, INVALID_PAGE_ID};
use crate::storage::common::BoundedReader;
use crate::storage::disk::PAGE_SIZE;
use bytes::BytesMut;
use std::cmp::Ordering;

pub const INTERNAL_PAGE_HEADER_SIZE: usize = 9;
pub const LEAF_PAGE_HEADER_SIZE: usize = 13;
pub const RID_SIZE: usize = 8;
pub const PAGE_ID_SIZE: usize = std::mem::size_of::<PageId>();
pub const ENUM_IDX_LEAF_PAGE: u8 = 1;
pub const ENUM_IDX_INTERNAL_PAGE: u8 = 2;

#[derive(Copy, Clone, Debug)]
pub enum IndexPageType {
    LeafPage = ENUM_IDX_LEAF_PAGE as isize,
    InternalPage = ENUM_IDX_INTERNAL_PAGE as isize,
}

pub type IndexPageKey = u32;
pub const INVALID_INDEX_PAGE_KEY: IndexPageKey = 0;

impl Serde for IndexPageKey {
    fn serialize_into(&self, buffer: &mut BytesMut) {
        buffer.write_u32(*self as u32);
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RID {
    pub page_id: PageId,
    pub slot_num: u32,
}

impl RID {
    pub fn new(page_id: PageId, slot_num: u32) -> Self {
        Self { page_id, slot_num }
    }

    pub fn serialize_into(&self, buffer: &mut BytesMut) {
        buffer.write_u32(self.page_id.0 as u32);
        buffer.write_u32(self.slot_num);
    }

    pub fn from(start: usize, buffer: &BytesMut) -> BPlusTreeResult<RID> {
        let mut start = start;
        let page_id = buffer.read_u32(start)?;
        start += 4;
        let slot_num = buffer.read_u32(start)?;

        Ok(RID {
            page_id: PageId(page_id as usize),
            slot_num,
        })
    }
}

/// BPlusTreePage represents an enum type for pages in B+Tree index.
pub enum BPlusTreePage {
    Internal(BPlusTreeInternalPage),
    Leaf(BPlusTreeLeafPage),
}

impl BPlusTreePage {
    pub fn page_type(&self) -> IndexPageType {
        match self {
            BPlusTreePage::Internal(_) => IndexPageType::InternalPage,
            BPlusTreePage::Leaf(_) => IndexPageType::LeafPage,
        }
    }

    pub fn serialize(&self) -> BytesMut {
        match self {
            BPlusTreePage::Internal(p) => p.serialize(),
            BPlusTreePage::Leaf(p) => p.serialize(),
        }
    }

    pub fn serialize_into(&self, buf: &mut BytesMut) {
        match self {
            BPlusTreePage::Internal(internal_page) => internal_page.serialize_into(buf),
            BPlusTreePage::Leaf(leaf_page) => leaf_page.serialize_into(buf),
        }
    }

    pub(crate) fn insert_internal(&mut self, key: IndexPageKey, page_id: PageId) {
        match self {
            BPlusTreePage::Internal(p) => p.insert(key, page_id),
            BPlusTreePage::Leaf(_) => todo!(),
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        match self {
            BPlusTreePage::Internal(p) => p.is_full(),
            BPlusTreePage::Leaf(p) => p.is_full(),
        }
    }

    pub(crate) fn insert(&mut self, key: IndexPageKey, rid: RID) {
        match self {
            BPlusTreePage::Internal(ip) => ip.insert(key, rid.page_id),
            BPlusTreePage::Leaf(lp) => lp.insert(key, rid),
        }
    }
}

/// BaseHeader includes common metadata used page headers.
pub struct BaseHeader {
    // page_type represents the type of current page in B+Tree index.
    pub page_type: IndexPageType,
    // current_size indicates the current number of keys in the page.
    pub size: usize,
    // max_size represents maximum number of keys in the page.
    pub max_size: usize,
}

impl BaseHeader {
    pub fn serialize_into(&self, buf: &mut BytesMut) {
        buf.write_u8(self.page_type as u8);
        buf.write_u32(self.size as u32);
        buf.write_u32(self.max_size as u32);
    }
}

/// PageHeader represents the header in B+Tree index pages.
/// The difference between Internal and Leaf pages headers is the
/// next_page_id used in leaf pages as the nodes in leaf pages need to
/// be a linked list.
pub enum PageHeader {
    Internal(InternalPageHeader),
    Leaf(LeafPageHeader),
}

pub struct InternalPageHeader {
    pub(crate) base: BaseHeader,
}

impl InternalPageHeader {
    pub fn new(page_type: IndexPageType, size: usize, max_size: usize) -> InternalPageHeader {
        InternalPageHeader {
            base: BaseHeader {
                page_type,
                size,
                max_size,
            },
        }
    }
}

impl InternalPageHeader {
    pub fn serialize_into(&self, buf: &mut BytesMut) {
        self.base.serialize_into(buf);
    }
}

pub struct LeafPageHeader {
    pub base: BaseHeader,
    pub next_page_id: PageId,
}

impl LeafPageHeader {
    pub fn serialize_into(&self, buf: &mut BytesMut) {
        self.base.serialize_into(buf);
        buf.write_u32(self.next_page_id.0 as u32);
    }
}

/*
Store `n` indexed keys and `n + 1` child pointers (page_id) within internal page.
Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
K(i) <= K < K(i+1).
NOTE: Since the number of keys does not equal to number of child pointers,
the first key in key_array_ always remains invalid. That is to say, any search / lookup
should ignore the first key.

Internal page format (keys are stored in increasing order):
 ---------
| HEADER |
 ---------
 ------------------------------------------
| KEY(1)(INVALID) | KEY(2) | ... | KEY(n) |
 ------------------------------------------
 ---------------------------------------------
| PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(n) |
 ---------------------------------------------

   Keys:     [_, K1, K2, K3]     // First key (_) is invalid/unused
   PageIDs:  [P0, P1, P2, P3]    // Child pointers

   For a search value X:
   - If X < K1, follow P0
   - If K1 ≤ X < K2, follow P1
   - If K2 ≤ X < K3, follow P2
   - If X ≥ K3, follow P3

   Note: First key is kept invalid because B+ tree internal nodes don't need a "left boundary" key
   for the K1. It only needs to know the boundary.
   Any value less than K1 automatically goes to P0. This design allows for direct index matching
   between keys and their corresponding right child pointers.
*/
pub struct BPlusTreeInternalPage {
    pub header: InternalPageHeader,
    pub keys: Vec<IndexPageKey>,
    pub page_ids: Vec<PageId>,
}

impl BPlusTreeInternalPage {
    pub(crate) fn copy_half_from(&mut self, tree_page: &mut BPlusTreeInternalPage) {
        let idx = tree_page.header.base.size / 2;
        let keys = tree_page.keys.split_off(idx);
        let pids = tree_page.page_ids.split_off(idx);

        self.header.base.size -= keys.len();
        self.keys = keys;
        self.page_ids = pids;
    }

    /// Sorts the keys and page_ids while maintaining their relationships.
    /// Note: The first key (index 0) is kept invalid as per B+ tree internal node structure.
    pub fn sort(&mut self) {
        if self.keys.len() <= 2 {
            return;
        }

        // let invalid_key: IndexPageKey = self.keys.remove(0);

        let mut indices: Vec<usize> = (0..self.keys.len()).collect();

        indices.sort_by_key(|&i| self.keys[i]);

        let mut sorted_keys = Vec::with_capacity(indices.len());
        let mut sorted_page_ids = Vec::with_capacity(self.page_ids.len());

        // Append the sorted elements
        for &idx in &indices {
            sorted_keys.push(self.keys[idx]);
            sorted_page_ids.push(self.page_ids[idx]);
        }
        for i in 0..=indices.len() {}

        // Replace the original vectors
        self.keys = sorted_keys;
        self.page_ids = sorted_page_ids;
    }

    pub fn serialize(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        self.header.serialize_into(&mut buf);
        for i in 0..self.keys.len() {
            self.keys[i].serialize_into(&mut buf);
        }

        for page_id in &self.page_ids {
            page_id.serialize_into(&mut buf);
        }

        buf.resize(PAGE_SIZE, 0);
        buf
    }

    pub fn serialize_into(&self, buf: &mut BytesMut) {
        for i in 0..self.keys.len() {
            self.keys[i].serialize_into(buf);
        }

        for page_id in &self.page_ids {
            page_id.serialize_into(buf);
        }

        buf.resize(PAGE_SIZE, 0);
    }

    /// Finds the PageId of the child node that should contain the given key
    pub fn find_child(&self, key: IndexPageKey) -> BPlusTreeResult<PageId> {
        let child_index = self.find_child_index(key);
        self.value_at(child_index)
            .ok_or(BPlusTreeError::InvalidOperation)
    }

    /// Finds the index of the child pointer that should be followed to find the given key.
    /// Returns the index i, where key_array_[i-1] <= key < key_array_[i]
    pub fn find_child_index(&self, key: IndexPageKey) -> usize {
        let mut left = 0;
        let mut right = self.header.base.size;

        while left <= right {
            let mid = left + (right - left) / 2;

            if mid >= self.keys.len() {
                break;
            }

            let mid_key = self.keys[mid];
            if mid_key <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // The pointer index is one less than where we ended up
        // This is because in a B+ tree internal node:
        // P1 K1 P2 K2 P3 ...
        // If we want a key less than K1, we follow P1
        // If we want a key between K1 and K2, we follow P2
        // Etc.
        left - 1
    }

    pub fn insert(&mut self, key: IndexPageKey, page_id: PageId) {
        self.keys.push(key);
        self.page_ids.push(page_id);
        self.sort();
        self.header.base.size += 1;
    }

    pub fn insert_pid_at(&mut self, idx: usize, page_id: PageId) {
        let prev_pids_len = self.page_ids.len();
        self.page_ids.insert(idx, page_id);
    }

    /// Creates a new internal page with specified maximum size
    pub fn new(max_size: usize) -> Self {
        let header = InternalPageHeader {
            base: BaseHeader {
                page_type: IndexPageType::InternalPage,
                size: 0,
                max_size,
            },
        };

        // Initialize with capacity. First key is invalid.
        // We add 1 to max_size for page_ids as we need one more pointer than keys
        let mut keys = Vec::with_capacity(max_size);
        let mut page_ids = Vec::with_capacity(max_size + 1);

        // Initialize first key as invalid (we'll use 0 as invalid key)
        // maybe we do not need this as we are initializing the vector of page_ids as max_size + 1
        keys.push(INVALID_INDEX_PAGE_KEY);

        Self {
            header,
            keys,
            page_ids,
        }
    }

    /// Returns the key at the given index. Index must be > 0 as first key is invalid
    pub fn key_at(&self, index: usize) -> Option<IndexPageKey> {
        if index == 0 || index >= self.header.base.size {
            None
        } else {
            Some(self.keys[index])
        }
    }

    /// Sets the key at the given index. Index must be > 0 as first key is invalid
    pub fn set_key_at(&mut self, index: usize, key: IndexPageKey) -> BPlusTreeResult<()> {
        if index == 0 {
            return Err(BPlusTreeError::Generic(
                "Cannot set key at index 0 (reserved for invalid key)".to_string(),
            ));
        }
        if index >= self.header.base.max_size {
            return Err(BPlusTreeError::IndexOutOfBounds);
        }

        if index >= self.keys.len() {
            self.keys.push(key);
        } else {
            self.keys[index] = key;
        }
        Ok(())
    }

    /// Returns the page_id at given index
    pub fn value_at(&self, index: usize) -> Option<PageId> {
        self.page_ids.get(index).copied()
    }

    /// Sets the page_id at given index
    pub fn set_value_at(&mut self, index: usize, page_id: PageId) -> BPlusTreeResult<()> {
        if index >= self.header.base.max_size + 1 {
            return Err(BPlusTreeError::IndexOutOfBounds);
        }

        if index >= self.page_ids.len() {
            self.page_ids.push(page_id);
        } else {
            self.page_ids[index] = page_id;
        }

        Ok(())
    }

    /// Returns current size (number of keys)
    pub fn size(&self) -> usize {
        self.header.base.size
    }

    /// Returns maximum size
    pub fn max_size(&self) -> usize {
        self.header.base.max_size
    }

    /// Checks if the page is full
    pub fn is_full(&self) -> bool {
        self.header.base.size > self.header.base.max_size
    }

    /// Checks if the page is half full
    pub fn is_half_full(&self) -> bool {
        self.header.base.size >= self.header.base.max_size / 2
    }
}

/*
 * Store indexed key and record id (record id = page id combined with slot id,
 * see `include/common/rid.h` for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 */
pub struct BPlusTreeLeafPage {
    pub header: LeafPageHeader,
    pub keys: Vec<IndexPageKey>,
    pub values: Vec<RID>,
}

impl BPlusTreeLeafPage {
    /// Creates a new leaf page with specified maximum size
    pub fn new(max_size: usize) -> Self {
        let header = LeafPageHeader {
            base: BaseHeader {
                page_type: IndexPageType::LeafPage,
                size: 0,
                max_size,
            },
            next_page_id: INVALID_PAGE_ID,
        };

        Self {
            header,
            keys: Vec::with_capacity(max_size),
            values: Vec::with_capacity(max_size),
        }
    }

    pub fn sort(&mut self) {
        let mut indices: Vec<usize> = (0..self.keys.len()).collect();

        indices.sort_by_key(|&i| self.keys[i]);

        let sorted_keys: Vec<_> = indices.iter().map(|&i| self.keys[i]).collect();
        let sorted_values: Vec<_> = indices.iter().map(|&i| self.values[i]).collect();

        self.keys = sorted_keys;
        self.values = sorted_values;
    }

    pub fn serialize(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(PAGE_SIZE);
        self.header.serialize_into(&mut buf);

        for i in 0..self.keys.len() {
            self.keys[i].serialize_into(&mut buf);
        }

        for value in &self.values {
            value.serialize_into(&mut buf);
        }

        buf.resize(PAGE_SIZE, 0);
        buf
    }

    // TODO: ensure that buf does not exceed page size.
    pub fn serialize_into(&self, buf: &mut BytesMut) {
        self.header.serialize_into(buf);

        for i in 0..self.keys.len() {
            self.keys[i].serialize_into(buf);
        }

        for value in &self.values {
            value.serialize_into(buf);
        }

        buf.resize(PAGE_SIZE, 0);
    }

    /// Returns the key at the given index
    pub fn key_at(&self, index: usize) -> Option<IndexPageKey> {
        if index >= self.header.base.size {
            None
        } else {
            Some(self.keys[index])
        }
    }

    /// Sets the key at the given index
    pub fn set_key_at(&mut self, index: usize, key: IndexPageKey) -> BPlusTreeResult<()> {
        if index >= self.header.base.max_size {
            return Err(BPlusTreeError::IndexOutOfBounds);
        }

        if index >= self.keys.len() {
            self.keys.push(key);
        } else {
            self.keys[index] = key;
        }
        Ok(())
    }

    /// Gets the next page id (for leaf node linking)
    pub fn next_page_id(&self) -> PageId {
        self.header.next_page_id.clone()
    }

    /// Sets the next page id
    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.header.next_page_id = page_id;
    }

    /// Checks if the page is full
    pub fn is_full(&self) -> bool {
        self.header.base.size > self.header.base.max_size
    }

    pub fn insert(&mut self, key: IndexPageKey, rid: RID) {
        self.keys.push(key);
        self.values.push(rid);
        self.sort();

        self.header.base.size += 1;
    }

    /// Finds the position of a key in the leaf page
    pub fn find_key(&self, key: IndexPageKey) -> Option<usize> {
        for i in 0..self.header.base.size {
            match key.cmp(&self.keys[i]) {
                Ordering::Equal => return Some(i),
                Ordering::Less => break,
                Ordering::Greater => continue,
            }
        }
        None
    }

    /// Removes a key and its associated value from the leaf page
    pub fn remove(&mut self, key: IndexPageKey) -> BPlusTreeResult<()> {
        if let Some(pos) = self.find_key(key) {
            self.keys.remove(pos);
            self.values.remove(pos);
            self.header.base.size -= 1;
            return Ok(());
        }

        Err(BPlusTreeError::KeyNotFound(key as usize))
    }

    pub(crate) fn copy_half_from(&mut self, page_frame: &mut BPlusTreeLeafPage) {
        let idx = page_frame.header.base.size / 2;
        let keys = page_frame.keys.split_off(idx);
        let vals = page_frame.values.split_off(idx);
        if keys.len() != vals.len() {
            panic!("invalid keys and vals on leaf page");
        }
        page_frame.header.base.size -= keys.len();
        self.header.base.size = keys.len();
        self.keys = keys;
        self.values = vals;
    }
}

// Implement Debug trait for better debugging
impl std::fmt::Debug for BPlusTreeLeafPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LeafPage {{ size: {}, keys: {:?}, values: {:?}, next_page_id: {:?} }}",
            self.header.base.size,
            &self.keys[..self.header.base.size],
            &self.values[..self.header.base.size],
            self.header.next_page_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a test header
    fn create_test_base_header(
        page_type: IndexPageType,
        size: usize,
        max_size: usize,
    ) -> BaseHeader {
        BaseHeader {
            page_type,
            size,
            max_size,
        }
    }

    #[test]
    fn test_header_creation() {
        let header = create_test_base_header(IndexPageType::LeafPage, 5, 100);
        assert!(matches!(header.page_type, IndexPageType::LeafPage));
        assert_eq!(header.size, 5);
        assert_eq!(header.max_size, 100);
    }

    #[test]
    fn test_serialize_format() {
        let size = 5;
        let max_size = 100;
        let header = create_test_base_header(IndexPageType::LeafPage, size, max_size);
        let mut buf = BytesMut::new();

        header.serialize_into(&mut buf);

        assert_eq!(buf.len(), INTERNAL_PAGE_HEADER_SIZE);

        // verify format: <page_type_1byte><size_4byte><max_size_4byte>
        assert_eq!(buf.read_u8(0).unwrap(), 1);
        assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::LeafPage as u8);
        assert_eq!(buf.read_u32(1).unwrap(), size as u32);
        assert_eq!(buf.read_u32(5).unwrap(), max_size as u32);
    }

    #[test]
    fn test_serialize_zero_values() {
        let header = create_test_base_header(IndexPageType::InternalPage, 0, 0);
        let mut buf = BytesMut::new();

        header.serialize_into(&mut buf);

        assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::InternalPage as u8);
        assert_eq!(buf.read_u32(1).unwrap(), 0);
        assert_eq!(buf.read_u32(5).unwrap(), 0);
    }

    #[test]
    fn test_serialize_max_values() {
        let header = create_test_base_header(
            IndexPageType::LeafPage,
            u32::MAX as usize,
            u32::MAX as usize,
        );
        let mut buf = BytesMut::new();

        header.serialize_into(&mut buf);

        assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::LeafPage as u8);
        assert_eq!(buf.read_u32(1).unwrap(), u32::MAX);
        assert_eq!(buf.read_u32(5).unwrap(), u32::MAX);
    }

    #[test]
    fn test_multiple_serializations() {
        let header1 = create_test_base_header(IndexPageType::LeafPage, 5, 100);
        let header2 = create_test_base_header(IndexPageType::InternalPage, 10, 200);
        let mut buf = BytesMut::new();

        // Serialize both headers consecutively
        header1.serialize_into(&mut buf);
        header2.serialize_into(&mut buf);

        // Verify first header
        assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::LeafPage as u8);
        assert_eq!(buf.read_u32(1).unwrap(), 5);
        assert_eq!(buf.read_u32(5).unwrap(), 100);

        // Verify second header
        assert_eq!(buf.read_u8(9).unwrap(), IndexPageType::InternalPage as u8);
        assert_eq!(buf.read_u32(10).unwrap(), 10);
        assert_eq!(buf.read_u32(14).unwrap(), 200);
    }

    #[test]
    fn test_size_constraints() {
        let header = create_test_base_header(IndexPageType::LeafPage, 101, 100);
        let mut buf = BytesMut::new();

        // Even though size > max_size, it should still serialize correctly
        header.serialize_into(&mut buf);

        assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::LeafPage as u8);
        assert_eq!(buf.read_u32(1).unwrap(), 101);
        assert_eq!(buf.read_u32(5).unwrap(), 100);
    }

    #[test]
    fn test_all_page_types() {
        let mut buf = BytesMut::new();

        // Test serialization for each page type
        for page_type in [IndexPageType::LeafPage, IndexPageType::InternalPage] {
            buf.clear();
            let header = create_test_base_header(page_type, 1, 100);
            header.serialize_into(&mut buf);

            assert_eq!(buf.read_u8(0).unwrap(), page_type as u8);
            assert_eq!(buf.read_u32(1).unwrap(), 1);
            assert_eq!(buf.read_u32(5).unwrap(), 100);
        }
    }

    // Helper function to create a test leaf page header
    fn create_test_leaf_header(size: usize, max_size: usize, next_page_id: u32) -> LeafPageHeader {
        LeafPageHeader {
            base: BaseHeader {
                page_type: IndexPageType::LeafPage,
                size,
                max_size,
            },
            next_page_id: PageId(next_page_id as usize),
        }
    }

    #[test]
    fn test_leaf_header_creation() {
        let header = create_test_leaf_header(5, 100, 42);

        assert!(matches!(header.base.page_type, IndexPageType::LeafPage));
        assert_eq!(header.base.size, 5);
        assert_eq!(header.base.max_size, 100);
        assert_eq!(header.next_page_id.0, 42);
    }

    #[test]
    fn test_serialize_format_leaf() {
        let header = create_test_leaf_header(5, 100, 42);
        let mut buf = BytesMut::new();

        // Serialize the header
        header.serialize_into(&mut buf);

        // Verify total length (BaseHeader 9 bytes + next_page_id 4 bytes = 13 bytes)
        assert_eq!(buf.len(), LEAF_PAGE_HEADER_SIZE);

        // Verify format: <base_header><next_page_id_4byte>
        assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::LeafPage as u8);
        assert_eq!(buf.read_u32(1).unwrap(), 5); // size
        assert_eq!(buf.read_u32(5).unwrap(), 100); // max_size

        // Verify next_page_id
        assert_eq!(buf.read_u32(9).unwrap(), 42);
    }

    #[test]
    fn test_serialize_zero_values_leaf() {
        let header = create_test_leaf_header(0, 0, 0);
        let mut buf = BytesMut::new();

        header.serialize_into(&mut buf);

        assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::LeafPage as u8);
        assert_eq!(buf.read_u32(1).unwrap(), 0); // size
        assert_eq!(buf.read_u32(5).unwrap(), 0); // max_size
        assert_eq!(buf.read_u32(9).unwrap(), 0); // next_page_id
    }

    #[test]
    fn test_serialize_max_values_leaf() {
        let header = create_test_leaf_header(u32::MAX as usize, u32::MAX as usize, u32::MAX);
        let mut buf = BytesMut::new();

        header.serialize_into(&mut buf);

        assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::LeafPage as u8);
        assert_eq!(buf.read_u32(1).unwrap(), u32::MAX); // size
        assert_eq!(buf.read_u32(5).unwrap(), u32::MAX); // max_size
        assert_eq!(buf.read_u32(9).unwrap(), u32::MAX); // next_page_id
    }

    #[test]
    fn test_multiple_serializations_leaf() {
        let header1 = create_test_leaf_header(5, 100, 42);
        let header2 = create_test_leaf_header(10, 200, 84);
        let mut buf = BytesMut::new();

        // Serialize both headers consecutively
        header1.serialize_into(&mut buf);
        header2.serialize_into(&mut buf);

        // Verify first header
        assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::LeafPage as u8);
        assert_eq!(buf.read_u32(1).unwrap(), 5);
        assert_eq!(buf.read_u32(5).unwrap(), 100);
        assert_eq!(buf.read_u32(9).unwrap(), 42);

        // Verify second header (offset by 13 bytes)
        assert_eq!(buf.read_u8(13).unwrap(), IndexPageType::LeafPage as u8);
        assert_eq!(buf.read_u32(14).unwrap(), 10);
        assert_eq!(buf.read_u32(18).unwrap(), 200);
        assert_eq!(buf.read_u32(22).unwrap(), 84);
    }

    #[test]
    fn test_sequential_page_ids() {
        let mut buf = BytesMut::new();

        // Test sequential page IDs
        for i in 0..5 {
            buf.clear();
            let header = create_test_leaf_header(1, 100, i);
            header.serialize_into(&mut buf);

            assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::LeafPage as u8);
            assert_eq!(buf.read_u32(1).unwrap(), 1);
            assert_eq!(buf.read_u32(5).unwrap(), 100);
            assert_eq!(buf.read_u32(9).unwrap(), i);
        }
    }

    #[test]
    fn test_size_larger_than_max() {
        let header = create_test_leaf_header(150, 100, 42);
        let mut buf = BytesMut::new();

        // Even though size > max_size, it should still serialize correctly
        header.serialize_into(&mut buf);

        assert_eq!(buf.read_u8(0).unwrap(), IndexPageType::LeafPage as u8);
        assert_eq!(buf.read_u32(1).unwrap(), 150);
        assert_eq!(buf.read_u32(5).unwrap(), 100);
        assert_eq!(buf.read_u32(9).unwrap(), 42);
    }

    #[test]
    fn test_buffer_capacity() {
        let header = create_test_leaf_header(5, 100, 42);
        let mut buf = BytesMut::with_capacity(20); // More than needed

        header.serialize_into(&mut buf);

        // Verify the buffer grew correctly
        assert_eq!(buf.capacity(), 20);
        assert_eq!(buf.len(), 13);
    }
}
