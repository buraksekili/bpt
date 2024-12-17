use super::{
    context::Context,
    error::{BPlusTreeError, BPlusTreeResult},
    pages::{
        BPlusTreeInternalPage, BPlusTreeLeafPage, BPlusTreePage, IndexPageKey,
        INTERNAL_PAGE_HEADER_SIZE, LEAF_PAGE_HEADER_SIZE, RID,
    },
};
use crate::storage::common::BoundedReader;
use crate::storage::index::pages::INVALID_INDEX_PAGE_KEY;
use crate::storage::index::serde::{parse_key_from, parse_page_header};
use crate::{
    buffer::manager::FrameHeader,
    storage::index::pages::{PageHeader, RID_SIZE},
};
use crate::{
    buffer::{
        manager::BufferManager,
        types::{PageId, INVALID_PAGE_ID},
    },
    storage::disk::PAGE_SIZE,
};
use bytes::BytesMut;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

pub struct BPlusTree {
    header_page_id: RwLock<PageId>,
    bpm: Arc<BufferManager>,
    leaf_max_size: usize,
    internal_max_size: usize,
}


impl BPlusTree {
    pub fn new(header_page_id: PageId, bpm: Arc<BufferManager>) -> Self {
        // TODO: fix hard-coded parameters
        Self {
            header_page_id: RwLock::new(header_page_id),
            bpm,
            leaf_max_size: 3,
            internal_max_size: 3,
        }
    }

    fn frame_header_to_page(frame_header: &FrameHeader) -> BPlusTreeResult<BPlusTreePage> {
        let bpt_page: BPlusTreePage;

        let frame = frame_header.frame.read();
        let data = &frame.data;
        let (read, header) = parse_page_header(data)?;
        match header {
            PageHeader::Leaf(leaf_header) => {
                let mut leaf_page = BPlusTreeLeafPage::new(leaf_header.base.max_size);
                leaf_page.header = leaf_header;

                // Skip header size (13 bytes)
                let mut offset = LEAF_PAGE_HEADER_SIZE;

                for _ in 0..leaf_page.header.base.size {
                    let key = parse_key_from(offset, data)?;
                    if key != INVALID_INDEX_PAGE_KEY {
                        leaf_page.keys.push(key);
                    }
                    offset += std::mem::size_of::<IndexPageKey>();
                }

                for _ in 0..leaf_page.header.base.size {
                    let rid: RID = RID::from(offset, data)?;
                    leaf_page.values.push(rid);
                    offset += RID_SIZE;
                }

                bpt_page = BPlusTreePage::Leaf(leaf_page)
            }
            PageHeader::Internal(internal_header) => {
                let mut internal_page = BPlusTreeInternalPage::new(internal_header.base.max_size);
                internal_page.header = internal_header;

                // Skip header size (9 bytes)
                let mut offset = INTERNAL_PAGE_HEADER_SIZE;

                for _ in 0..=internal_page.header.base.size {
                    let key = parse_key_from(offset, data)?;
                    if key != INVALID_INDEX_PAGE_KEY {
                        internal_page.keys.push(key);
                    }
                    offset += std::mem::size_of::<IndexPageKey>();
                }

                for _ in 0..=internal_page.header.base.size {
                    let page_id = data.read_u32(offset)?;
                    offset += 4;
                    internal_page.page_ids.push(PageId(page_id as usize));
                }

                bpt_page = BPlusTreePage::Internal(internal_page);
            }
        }

        Ok((bpt_page))
    }

    /// Insert a key-value pair into the B+ tree.
    /// If the tree has no root attached, it creates a new root.
    pub fn insert(&self, key: IndexPageKey, rid: RID) -> BPlusTreeResult<bool> {
        if *self.header_page_id.read() == INVALID_PAGE_ID {
            let (new_root, mut root_page) = self.create_leaf_page()?;
            if !matches!(root_page, BPlusTreePage::Leaf(_)) {
                return Err(BPlusTreeError::CreateLeafPage);
            }

            if let BPlusTreePage::Leaf(leaf_page) = &mut root_page {
                leaf_page.insert(key, rid);

                let mut rpid = self.header_page_id.write();
                *rpid = new_root;

                let mut buf = BytesMut::new();
                leaf_page.serialize_into(&mut buf);
                self.bpm.write_page(*rpid, &buf)?;

                return Ok(true);
            }

            return Err(BPlusTreeError::Generic(format!(
                "failed to insert key={}",
                key
            )));
        }

        let root_page_id = self.get_root_page_id();
        let (mut curr_page_frame, mut root_page) = self.fetch_page(root_page_id)?;
        let mut ctx = Context::new();

        while let BPlusTreePage::Internal(internal_page) = root_page {
            ctx.push_into_read_set(curr_page_frame.frame.read().page_id);
            let next_child_page_id = internal_page.find_child(key)?;
            let (next_page_frame, next_child_page) = self.fetch_page(next_child_page_id)?;
            curr_page_frame = next_page_frame;
            root_page = next_child_page;
        }

        let mut curr_tree_page = Self::frame_header_to_page(&curr_page_frame)?;
        curr_tree_page.insert(key, rid);

        while curr_tree_page.is_full() {
            match curr_tree_page {
                BPlusTreePage::Internal(ref mut curr_full_internal_page) => {
                    let (new_node, new_node_pid) = self.split_internal_page(curr_full_internal_page)?;
                    let curr_page_id = curr_page_frame.frame.read().page_id;
                    self.bpm.write_page(curr_page_id, &curr_full_internal_page.serialize())?;
                    let min_leaf = self.min_leaf(new_node_pid)?;

                    if let Some(parent_node_pid) = ctx.pop_back() {
                        let (parent_node_frame, mut parent_tree_page) =
                            self.fetch_page(parent_node_pid)?;
                        parent_tree_page
                            .insert_internal(*new_node.keys.first().unwrap(), new_node_pid);
                        curr_tree_page = parent_tree_page;
                        curr_page_frame = parent_node_frame;
                    } else if curr_page_id == self.get_root_page_id() {
                        let new_root_page = self.bpm.new_page()?;
                        let new_root_page_id = new_root_page.frame.read().page_id;
                        let mut new_root_internal_tree_page =
                            BPlusTreeInternalPage::new(self.internal_max_size);

                        new_root_internal_tree_page.page_ids.push(curr_page_id);
                        new_root_internal_tree_page
                            .insert(min_leaf.0, new_node_pid);
                        self.bpm.write_page(
                            new_root_page_id,
                            &new_root_internal_tree_page.serialize(),
                        )?;

                        *self.header_page_id.write() = new_root_page_id;
                        curr_page_frame = new_root_page;
                        curr_tree_page = BPlusTreePage::Internal(new_root_internal_tree_page);
                    }
                }
                BPlusTreePage::Leaf(ref mut curr_full_leaf_node) => {
                    let curr_page_id = curr_page_frame.frame.read().page_id;
                    let (new_leaf_tree_page, new_leaf_tree_pid) =
                        self.split_leaf_page(curr_full_leaf_node)?;
                    self.bpm
                        .write_page(curr_page_id, &curr_full_leaf_node.serialize())?;
                    let min_leaf = self.min_leaf(new_leaf_tree_pid)?;

                    if let Some(parent_node_pid) = ctx.pop_back() {
                        let (parent_node_frame, mut parent_tree_page) =
                            self.fetch_page(parent_node_pid)?;
                        parent_tree_page.insert_internal(
                            *new_leaf_tree_page.keys.get(0).unwrap(),
                            new_leaf_tree_pid,
                        );
                        curr_tree_page = parent_tree_page;
                        curr_page_frame = parent_node_frame;
                    } else if curr_page_id == self.get_root_page_id() {
                        let new_root_page_frame = self.bpm.new_page()?;
                        let new_root_page_id = new_root_page_frame.frame.read().page_id;
                        let mut new_root_internal_tree_page =
                            BPlusTreeInternalPage::new(self.internal_max_size);

                        new_root_internal_tree_page.page_ids.push(curr_page_id);
                        new_root_internal_tree_page
                            .insert(min_leaf.0, curr_full_leaf_node.next_page_id());
                        self.bpm.write_page(
                            new_root_page_id,
                            &new_root_internal_tree_page.serialize(),
                        )?;

                        *self.header_page_id.write() = new_root_page_id;
                        curr_page_frame = new_root_page_frame;
                        curr_tree_page = BPlusTreePage::Internal(new_root_internal_tree_page);
                    }
                }
            }
        }

        {
            let mut frame = curr_page_frame.frame.write();
            frame.data = curr_tree_page.serialize();
            frame.is_dirty = true;
        }

        Ok(true)
    }

    // Returns the root page id of the tree.
    fn get_root_page_id(&self) -> PageId {
        let root_page_id = self.header_page_id.read();
        *root_page_id
    }


    /// Create a new leaf page and return both page_id and page object,
    pub fn create_leaf_page(&self) -> BPlusTreeResult<(PageId, BPlusTreePage)> {
        // Create a new page through buffer pool
        let new_page_id = self.allocate_page()?;

        // Initialize new leaf page
        let leaf_page = BPlusTreePage::Leaf(BPlusTreeLeafPage::new(self.leaf_max_size));

        // Serialize and write the page
        let mut buffer = BytesMut::with_capacity(PAGE_SIZE);
        self.serialize_page(&leaf_page, &mut buffer)?;

        // Write to buffer pool
        self.bpm.write_page(new_page_id, &buffer)?;

        // Return both the page_id and the page object
        Ok((new_page_id, leaf_page))
    }

    fn serialize(&self, page: &BPlusTreePage) -> BPlusTreeResult<BytesMut> {
        let mut buf = BytesMut::new();
        self.serialize_page(page, &mut buf)?;
        Ok(buf)
    }

    /// Helper method to serialize a page
    fn serialize_page(&self, page: &BPlusTreePage, buffer: &mut BytesMut) -> BPlusTreeResult<()> {
        if buffer.capacity() < PAGE_SIZE {
            return Err(BPlusTreeError::BufferTooSmall);
        }

        match page {
            BPlusTreePage::Leaf(leaf_page) => leaf_page.serialize_into(buffer),
            BPlusTreePage::Internal(internal_page) => {
                internal_page.serialize_into(buffer);
            }
        };

        Ok(())
    }

    fn create_internal_page(&self) -> BPlusTreeResult<PageId> {
        let new_page_id = self.allocate_page()?;
        let page = BPlusTreePage::Internal(BPlusTreeInternalPage::new(self.internal_max_size));
        let buffer = self.serialize(&page)?;
        self.bpm.write_page(new_page_id, &buffer)?;

        Ok(new_page_id)
    }

    /// split_leaf_page splits given `page_frame`. It creates a new page on the disk and
    /// frame on memory, and populate these disk and memory structures based on given
    /// page frame.
    /// page_frame represents an in-memory B+Tree page that is assumed to be full and going to be splitted.
    /// Returns the new leaf node and its page ID.
    fn split_leaf_page(
        &self,
        full_leaf_tree_page: &mut BPlusTreeLeafPage,
    ) -> BPlusTreeResult<(BPlusTreeLeafPage, PageId)> {
        if !full_leaf_tree_page.is_full() {
            return Err(BPlusTreeError::PageFull);
        }

        let new_disk_page = self.bpm.new_page()?;
        let new_page_id = new_disk_page.frame.read().page_id;

        let mut new_leaf_node = BPlusTreeLeafPage::new(self.leaf_max_size);
        let next_page_id = full_leaf_tree_page.next_page_id();

        new_leaf_node.set_next_page_id(next_page_id);
        new_leaf_node.copy_half_from(full_leaf_tree_page);
        full_leaf_tree_page.header.next_page_id = new_page_id;

        self.bpm
            .write_page(new_page_id, &new_leaf_node.serialize())?;

        Ok((new_leaf_node, new_page_id))
    }

    fn split_internal_page(
        &self,
        internal_node: &mut BPlusTreeInternalPage,
    ) -> BPlusTreeResult<(BPlusTreeInternalPage, PageId)> {
        let new_disk_page = self.bpm.new_page()?;
        let new_page_id = new_disk_page.frame.read().page_id;

        let mut new_internal_node = BPlusTreeInternalPage::new(self.internal_max_size);

        new_internal_node.copy_half_from(internal_node);
        self.bpm
            .write_page(new_page_id, &new_internal_node.serialize())?;

        Ok((new_internal_node, new_page_id))
    }

    // The min_leaf method helps us find this boundary marker. It returns a tuple where:
    // - IndexPageKey is the smallest key in the right subtree (our boundary marker)
    // - PageId is the ID of the new page we created during the split
    // We return these as a pair because the parent node needs both pieces:
    // - The key to make routing decisions
    // - The page ID to actually follow that route
    pub fn min_leaf(&self, page_id: PageId) -> BPlusTreeResult<(IndexPageKey, PageId)> {
        let (mut frame_header, mut curr_tree_page) = self.fetch_page(page_id)?;
        loop {
            match curr_tree_page {
                BPlusTreePage::Internal(internal_page) => {
                    let next_page_id = internal_page.value_at(0).unwrap();
                    let (next_page_frame_header, next_tree_page) = self.fetch_page(next_page_id)?;
                    curr_tree_page = next_tree_page;
                    frame_header = next_page_frame_header;
                }
                BPlusTreePage::Leaf(leaf_page) => {
                    // return Ok((leaf_page.keys[0], leaf_page.values[0]));
                    let pid = frame_header.frame.read().page_id;
                    return Ok((leaf_page.keys[0], pid));
                }
            }
        }
    }

    fn fetch_page(&self, page_id: PageId) -> BPlusTreeResult<(FrameHeader, BPlusTreePage)> {
        let frame_header = self.bpm.fetch_page(page_id)?;
        let bpt_page = Self::frame_header_to_page(&frame_header)?;

        Ok((frame_header, bpt_page))
    }

    fn allocate_page(&self) -> BPlusTreeResult<PageId> {
        let frame_header = self.bpm.new_page()?;
        let page_id = frame_header.frame.read().page_id;
        Ok(page_id)
    }

    pub fn print_tree(&self) -> BPlusTreeResult<()> {
        let root_page_id = self.get_root_page_id();
        if root_page_id == INVALID_PAGE_ID {
            println!("Tree is empty.");
            return Ok(());
        }

        let mut queue: VecDeque<PageId> = VecDeque::new();
        queue.push_back(root_page_id);
        let mut seen = HashMap::new();

        while queue.len() > 0 {
            let page_id = queue.pop_front().unwrap();
            if page_id.0 == *INVALID_PAGE_ID {
                continue;
            }

            if seen.contains_key(&page_id) {
                continue;
            }

            seen.insert(page_id, true);

            let (_, page) = self.fetch_page(page_id)?;

            match &page {
                BPlusTreePage::Leaf(leaf_page) => {
                    print!("Leaf Page {} | Keys: [", page_id.0);
                    for (counter, key) in leaf_page.keys.iter().enumerate() {
                        if counter == leaf_page.keys.len() - 1 {
                            print!("{}", key);
                        } else {
                            print!("{},", key);
                        }
                    }

                    print!("] RIDS: [");
                    for (counter, key) in leaf_page.values.iter().enumerate() {
                        if counter == leaf_page.values.len() - 1 {
                            print!("page_id: {} & slot_num: {}", *key.page_id, key.slot_num);
                        } else {
                            print!(
                                "page_id: {} & slot_num: {} <-> ",
                                *key.page_id, key.slot_num
                            );
                        }
                    }

                    print!("]");
                    let next_page_id = leaf_page.next_page_id();
                    if next_page_id != INVALID_PAGE_ID {
                        queue.push_back(next_page_id);
                        println!(" ---> {}", *next_page_id);
                    } else {
                        println!("");
                    }
                }
                BPlusTreePage::Internal(internal_page) => {
                    print!("Internal Page {} | Keys: [", page_id.0);
                    for key in &internal_page.keys {
                        print!("{} ", key);
                    }

                    print!("] | Pointers: [");
                    for page_id in &internal_page.page_ids {
                        print!("{} ", page_id.0);
                        queue.push_back(*page_id);
                    }

                    println!("]");
                }
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        buffer::lruk_replacer::LRUKReplacer,
        storage::{
            disk::{disk_manager::DiskManager, disk_scheduler::DiskScheduler},
            index::pages::IndexPageType,
        },
    };
    use tempfile::TempDir;

    fn create_bpm(buffer_size: usize, temp_dir: &TempDir) -> BufferManager {
        println!("{:?}", temp_dir.path().display());
        let replacer = LRUKReplacer::new(2, buffer_size);
        BufferManager::new(
            buffer_size,
            DiskScheduler::new(Arc::new(DiskManager::new(temp_dir.path()).unwrap())),
            replacer,
        )
    }

    #[test]
    fn test_simple_get_root_page_id() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let bpm = Arc::new(create_bpm(10, &temp_dir));

        let bpt = BPlusTree::new(INVALID_PAGE_ID, bpm.clone());
        assert_eq!(bpt.get_root_page_id(), INVALID_PAGE_ID);

        let expected_key = 3;
        let expected_rid = RID::new(PageId(123), 4);
        let inserted = bpt.insert(expected_key, expected_rid).unwrap();
        assert!(inserted);

        let rpid = *bpt.get_root_page_id();
        assert!(rpid > 0);
        assert_eq!(rpid, 1);

        bpm.flush_all().unwrap();

        let (_, page) = bpt.fetch_page(PageId(rpid)).unwrap();
        assert!(matches!(page, BPlusTreePage::Leaf(_)));
        if let BPlusTreePage::Leaf(page) = page {
            assert!(matches!(
                page.header.base.page_type,
                IndexPageType::LeafPage
            ));
            assert_eq!(page.header.next_page_id, INVALID_PAGE_ID);
            assert_eq!(page.header.base.size, 1);
            assert_eq!(page.keys.len(), 1);
            assert_eq!(page.values.len(), 1);
            assert_eq!(page.keys[0], expected_key);
            assert_eq!(page.values[0], expected_rid);
        }
    }

    #[test]
    fn test_simple_insert() -> BPlusTreeResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let bpm = Arc::new(create_bpm(10, &temp_dir));

        let bpt = BPlusTree::new(INVALID_PAGE_ID, bpm.clone());
        assert_eq!(bpt.get_root_page_id(), INVALID_PAGE_ID);

        for i in 1..=11 {
            println!("i -> {}", i);
            let frame = bpm.new_page()?;
            let pid = frame.frame.read().page_id;
            bpt.insert(i, RID::new(pid, i))?;
        }

        println!("\n***** FINAL ***********");
        bpt.print_tree()?;
        Ok(())
    }
}
