use super::types::INVALID_PAGE_ID;
use crate::buffer::lruk_replacer::LRUKReplacer;
use crate::buffer::types::{BufferManagerError, BufferManagerResult, FrameId, PageId};
use crate::storage::disk::disk_scheduler::{DiskRequest, DiskResponse, DiskScheduler};
use crate::storage::disk::PAGE_SIZE;
use bytes::BytesMut;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;

pub struct FrameHeader {
    pub frame: Arc<RwLock<Frame>>,
    replacer: Arc<RwLock<LRUKReplacer>>,
}

impl Drop for FrameHeader {
    fn drop(&mut self) {
        if self.frame.read().pin_count == 0 {
            let frame_id = self.frame.read().frame_id;
            self.replacer.write().set_evictable(frame_id, true);
            return;
        }

        self.frame.write().pin_count -= 1;
        if self.frame.read().pin_count <= 0 {
            let frame_id = self.frame.read().frame_id;
            self.replacer.write().set_evictable(frame_id, true);
        }
    }
}

// FrameHeader corresponds to the frame (page) in memory.
pub struct Frame {
    frame_id: FrameId,
    pub page_id: PageId,
    pin_count: u32,
    pub is_dirty: bool,
    pub data: BytesMut,
}

impl Frame {
    fn new(frame_id: FrameId) -> Self {
        Self {
            frame_id,
            page_id: INVALID_PAGE_ID,
            data: BytesMut::zeroed(PAGE_SIZE),
            is_dirty: false,
            pin_count: 1,
        }
    }

    fn with_page_id(mut self, page_id: PageId) -> Self {
        self.page_id = page_id;
        self
    }

    fn reset(&mut self) {
        self.pin_count = 0;
        self.frame_id = 0;
        self.is_dirty = false;
        self.data = BytesMut::zeroed(PAGE_SIZE);
        self.page_id = INVALID_PAGE_ID;
    }
}

pub enum DeletePageResponse {
    Deleted,
    NotFound,
}

// BufferManager manages data between disk and memory.
// Each data contains PAGE_SIZE bytes in it (4KB by default).
pub struct BufferManager {
    // Array of buffer pool pages.
    pages: Vec<Arc<RwLock<Frame>>>,

    // page_table keeps track of the mapping between pages and buffer pool frames.
    // TODO: use DashMap for concurrency.
    page_table: Arc<DashMap<PageId, FrameId>>,
    free_list: Arc<RwLock<Vec<FrameId>>>,
    replacer: Arc<RwLock<LRUKReplacer>>,
    disk_scheduler: Arc<DiskScheduler>,
}

impl BufferManager {
    pub fn new(
        num_frames: usize,
        disk_scheduler: DiskScheduler,
        replacer: LRUKReplacer,
    ) -> BufferManager {
        // TODO: check if replacer len and num_frames match
        let mut free_list = Vec::new();
        let mut pages = Vec::new();
        let replacer = Arc::new(RwLock::new(replacer));

        for i in 0..num_frames {
            // TODO: assigning the clone of replacer during initialization
            // is costly when num_frames is high.
            pages.push(Arc::new(RwLock::new(Frame::new(i))));
            free_list.push(i);
        }

        BufferManager {
            pages,
            page_table: Arc::new(DashMap::new()),
            free_list: Arc::new(RwLock::new(free_list)),
            replacer,
            disk_scheduler: Arc::new(disk_scheduler),
        }
    }

    // Fetch the requested page from the buffer pool.
    // Return nullptr if page_id needs to be fetched from the disk but all frames are currently
    // in use and not evictable (in another word, pinned).
    pub fn fetch_page(&self, page_id: PageId) -> BufferManagerResult<FrameHeader> {
        // First search for page_id in the buffer pool. If not found, pick a replacement frame
        // from either the free list or the replacer (always find from the free list first),
        // read the page from disk by calling disk_manager_->ReadPage(), and replace the old page
        // in the frame. If the old page is dirty, you need to write it back to disk and update
        // the metadata of the new page.
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let existing_frame = self.pages[*frame_id.value()].clone();
            existing_frame
                .try_write_for(Duration::from_secs(2))
                .unwrap()
                .pin_count += 1;
            {
                let frame_id = *frame_id.value();

                let mut replacer = self.replacer.write();
                replacer.set_evictable(frame_id, false);
                replacer.record_access(frame_id);
            }

            // return Ok(existing_frame.read().data.clone());
            return Ok(FrameHeader {
                frame: existing_frame,
                replacer: self.replacer.clone(),
            });
        }

        let frame_id = self.allocate_frame()?;
        let data: BytesMut;

        {
            let rx = self
                .disk_scheduler
                .schedule(DiskRequest::Read { page_id })?;
            let response = rx.recv_timeout(Duration::from_secs(2))?;

            let result = match response {
                DiskResponse::Read { data } => Ok(data),
                DiskResponse::Error(err) => Err(BufferManagerError::StorageError(err)),
                _ => Err(BufferManagerError::FetchPage(
                    "Unexpected disk response from disk scheduler".to_string(),
                )),
            }?;

            data = result;
        }

        self.page_table.insert(page_id, frame_id);
        let frame = Frame {
            pin_count: 1,
            page_id,
            frame_id,
            data,
            is_dirty: false,
        };

        {
            let mut f = self.pages[frame_id].write();
            *f = frame;
        }
        {
            let mut replacer = self.replacer.write();
            replacer.set_evictable(frame_id, false);
            replacer.record_access(frame_id);
        }

        Ok(FrameHeader {
            replacer: self.replacer.clone(),
            frame: self.pages[frame_id].clone(),
        })
    }

    pub fn size(&self) -> usize {
        self.free_list.read().len()
    }

    fn allocate_frame(&self) -> BufferManagerResult<FrameId> {
        {
            let mut free_list = self.free_list.write();
            if let Some(fid) = free_list.pop() {
                return Ok(fid);
            }
        }

        {
            let mut replacer = self.replacer.write();
            if let Some(frame_id) = replacer.evict() {
                return Ok(frame_id);
            }
        }

        Err(BufferManagerError::AllocatePage)
    }

    pub fn new_page(&self) -> BufferManagerResult<FrameHeader> {
        let free_list_len = self.free_list.read().len();
        let replacer_size = self.replacer.read().size();
        if free_list_len == 0 && replacer_size == 0 {
            return Err(BufferManagerError::FetchPage(
                "no page available".to_string(),
            ));
        }

        let frame_id = self.allocate_frame()?;
        let page_id =
            self.handle_disk_response(DiskRequest::Allocate, Duration::from_secs(1), |response| {
                match response {
                    DiskResponse::Allocate { page_id } => Ok(page_id),
                    DiskResponse::Error(err) => Err(err.into()),
                    _ => Err(BufferManagerError::Unexpected),
                }
            })?;

        {
            let mut frame = self.pages[frame_id].write();
            if frame.is_dirty {
                let rx = self.disk_scheduler.schedule(DiskRequest::Write {
                    page_id: frame.page_id,
                    data: frame.data.clone().freeze(),
                })?;
                rx.recv_timeout(Duration::from_secs(2))?;
            }

            *frame = Frame::new(frame_id).with_page_id(page_id);
        }

        self.replacer.write().record_access(frame_id);
        self.replacer.write().set_evictable(frame_id, false);
        self.page_table.insert(page_id, frame_id);

        Ok(FrameHeader {
            replacer: self.replacer.clone(),
            frame: self.pages[frame_id].clone(),
        })
    }

    // delete_page deletes a page from the buffer pool. If page_id is not in the buffer pool,
    // do nothing and return true. If the page is pinned and cannot be deleted,
    // return false immediately.
    pub fn delete_page(&self, page_id: PageId) -> BufferManagerResult<DeletePageResponse> {
        let frame_id = self.page_table.get(&page_id);
        if frame_id.is_none() {
            return Ok(DeletePageResponse::NotFound);
        }

        let frame_id = *frame_id.unwrap();
        {
            let mut frame = self.pages[frame_id].write();
            let pin_count = frame.pin_count;
            if pin_count > 0 {
                self.replacer.write().set_evictable(frame_id, false);
                return Err(BufferManagerError::PagePinned);
            }

            self.replacer.write().set_evictable(frame_id, true);
            frame.reset();
        }
        {
            let r = self.replacer.write().remove(frame_id);
            if let Err(_) = r {
                // TODO: fix error handling; use error stored in Result.
                return Err(BufferManagerError::Unexpected);
            }
        }

        self.free_list.write().push(frame_id);

        self.page_table.remove(&page_id);
        let rx = self
            .disk_scheduler
            .schedule(DiskRequest::Deallocate(page_id));
        rx?.recv_timeout(Duration::from_secs(1))?;

        Ok(DeletePageResponse::Deleted)
    }

    pub fn get_pin_count(&self, page_id: PageId) -> BufferManagerResult<u32> {
        let frame_id = self.page_table.get(&page_id);
        if frame_id.is_none() {
            return Err(BufferManagerError::PageNotFound);
        }

        let frame_id = *frame_id.unwrap();
        Ok(self.pages[frame_id].read().pin_count)
    }

    pub fn flush_page(&self, page_id: PageId) -> BufferManagerResult<()> {
        let frame_id = *self
            .page_table
            .get(&page_id)
            .ok_or(BufferManagerError::PageNotFound)?;

        {
            let mut frame = self.pages[frame_id].write();
            self.disk_scheduler.schedule(DiskRequest::Write {
                page_id,
                data: frame.data.clone().freeze(),
            })?;
            frame.pin_count -= 1;
            frame.is_dirty = false;
        }

        Ok(())
    }

    pub fn flush_all(&mut self) -> BufferManagerResult<()> {
        let pages: Vec<PageId> = self.page_table.iter().map(|kv| *kv.key()).collect();

        for page_id in pages {
            self.flush_page(page_id)?;
        }

        Ok(())
    }

    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> bool {
        if let Some(kv) = self.page_table.get(&page_id) {
            let frame_id = *kv.value();

            let zero = self.pages[frame_id].read().pin_count == 0;
            if !zero {
                self.pages[frame_id].write().pin_count -= 1;
                let pin_count = self.pages[frame_id].read().pin_count;
                if pin_count == 0 {
                    self.replacer.write().set_evictable(frame_id, true);
                }

                let id = self.pages[frame_id].read().is_dirty;
                if is_dirty && !id {
                    self.pages[frame_id].write().is_dirty = is_dirty;
                }

                return true;
            }
        }

        false
    }

    /// Write data to a specific page. This method will:
    /// 1. Fetch the page (or create if doesn't exist)
    /// 2. Write the data
    /// 3. Mark as dirty
    /// 4. Unpin the page
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> BufferManagerResult<()> {
        // Get the page
        let frame_header = self.fetch_page(page_id)?;

        {
            let mut frame = frame_header.frame.write();
            frame.data.clear();
            frame.data.extend_from_slice(data);
            frame.is_dirty = true;
        }

        Ok(())
    }

    fn handle_disk_response<T, F>(
        &self,
        request: DiskRequest,
        timeout: Duration,
        extractor: F,
    ) -> BufferManagerResult<T>
    where
        F: FnOnce(DiskResponse) -> BufferManagerResult<T>,
    {
        let rx = self
            .disk_scheduler
            .schedule(request)
            .map_err(BufferManagerError::from)?;

        let response = rx.recv_timeout(timeout).map_err(BufferManagerError::from)?;
        extractor(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::types::BufferManagerResult;
    use crate::storage::disk::disk_manager::DiskManager;
    use tempfile::TempDir;

    #[test]
    fn test_frame_header() {
        let frame = Arc::new(RwLock::new(Frame::new(23)));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(2, 2)));

        let frame_header = FrameHeader {
            frame: frame.clone(),
            replacer,
        };
        assert_eq!(Arc::strong_count(&frame), 2);
        assert_eq!(frame_header.frame.read().frame_id, 23);
        assert_eq!(frame_header.frame.read().page_id, PageId(0));
        drop(frame_header);
        assert_eq!(Arc::strong_count(&frame), 1);
    }

    #[test]
    fn test_frame_header_drop() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        const BUFFER_SIZE: usize = 10;
        let ds = DiskScheduler::new(Arc::new(DiskManager::new(temp_dir.path()).unwrap()));
        let replacer = LRUKReplacer::new(2, BUFFER_SIZE);
        let m = BufferManager::new(BUFFER_SIZE, ds, replacer);

        let fh = m.new_page().unwrap();
        let current_pin_count = fh.frame.read().pin_count;
        assert_eq!(current_pin_count, 1);

        let page_id = fh.frame.read().page_id;
        assert!(m.unpin_page(page_id, false));

        let current_pin_count = fh.frame.read().pin_count;
        assert_eq!(current_pin_count, 0);

        assert!(m.delete_page(page_id).is_ok());
        assert!(matches!(
            m.delete_page(page_id),
            Ok(DeletePageResponse::NotFound)
        ));
    }

    #[test]
    fn test_frame() {
        const FRAME_ID: usize = 50;
        let f = Frame::new(FRAME_ID);
        assert_eq!(f.frame_id, FRAME_ID);
        assert_eq!(f.pin_count, 1);
    }

    #[test]
    fn test_disk_manager_full() -> BufferManagerResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        const BUFFER_SIZE: usize = 3;
        let ds = DiskScheduler::new(Arc::new(DiskManager::new(temp_dir.path()).unwrap()));
        let replacer = LRUKReplacer::new(2, BUFFER_SIZE);
        let m = BufferManager::new(BUFFER_SIZE, ds, replacer);

        let page1 = m.new_page();
        assert!(page1.is_ok());
        let page2 = m.new_page();
        assert!(page2.is_ok());
        let page3 = m.new_page();
        assert!(page3.is_ok());
        let page4 = m.new_page();
        assert!(page4.is_err());
        assert!(matches!(page4, Err(BufferManagerError::FetchPage(_))));

        Ok(())
    }

    #[test]
    fn test_disk_manager_simple() -> BufferManagerResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        const BUFFER_SIZE: usize = 3;
        let ds = DiskScheduler::new(Arc::new(DiskManager::new(temp_dir.path()).unwrap()));
        let replacer = LRUKReplacer::new(2, BUFFER_SIZE);
        let m = BufferManager::new(BUFFER_SIZE, ds, replacer);
        assert_eq!(m.size(), BUFFER_SIZE);
        assert_eq!(m.pages.len(), BUFFER_SIZE);
        assert_eq!(m.free_list.read().len(), BUFFER_SIZE);

        {
            for i in 0..BUFFER_SIZE {
                let frame_header = m.new_page()?;
                let page_id = frame_header.frame.read().page_id;
                let frame_id = frame_header.frame.read().frame_id;
                assert_eq!(page_id, PageId(i));
                assert_eq!(frame_id, *m.page_table.get(&page_id).unwrap().value());
                assert_eq!(frame_id, BUFFER_SIZE - (i + 1));

                let pin_count = m.get_pin_count(PageId(i))?;
                assert_eq!(pin_count, 1);
                assert_eq!(m.free_list.read().len(), BUFFER_SIZE - (i + 1));

                let result = m.delete_page(PageId(i));
                assert!(matches!(result, Err(BufferManagerError::PagePinned)));
            }
        }

        assert!(m.free_list.read().is_empty());
        for i in 0..BUFFER_SIZE {
            let pin_count = m.get_pin_count(PageId(i))?;
            assert_eq!(pin_count, 0);
        }

        Ok(())
    }

    #[test]
    fn test_disk_manager_fetch() -> BufferManagerResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        const BUFFER_SIZE: usize = 10;
        let ds = DiskScheduler::new(Arc::new(DiskManager::new(temp_dir.path()).unwrap()));
        let replacer = LRUKReplacer::new(2, BUFFER_SIZE);
        let m = BufferManager::new(BUFFER_SIZE, ds, replacer);

        let fh = m.new_page()?;
        assert_eq!(fh.frame.read().data.len(), PAGE_SIZE);
        assert_eq!(fh.frame.read().data, BytesMut::zeroed(PAGE_SIZE).freeze());

        let page_id = fh.frame.read().page_id;
        let fh = m.fetch_page(page_id)?;
        assert_eq!(fh.frame.read().data.len(), PAGE_SIZE);
        assert_eq!(fh.frame.read().data, BytesMut::zeroed(PAGE_SIZE).freeze());
        assert_eq!(m.get_pin_count(page_id)?, 2);

        let fh = m.fetch_page(page_id)?;
        assert_eq!(fh.frame.read().data.len(), PAGE_SIZE);
        assert_eq!(fh.frame.read().data, BytesMut::zeroed(PAGE_SIZE).freeze());
        assert_eq!(m.get_pin_count(page_id)?, 3);

        drop(fh);
        assert_eq!(m.get_pin_count(page_id)?, 2);

        {
            let fh = m.fetch_page(page_id)?;
            assert_eq!(fh.frame.read().data.len(), PAGE_SIZE);
            assert_eq!(fh.frame.read().data, BytesMut::zeroed(PAGE_SIZE).freeze());
            assert_eq!(m.get_pin_count(page_id)?, 3);
        }
        assert_eq!(m.get_pin_count(page_id)?, 2);

        for _ in 0..5 {
            m.fetch_page(page_id)?;
        }
        assert_eq!(m.get_pin_count(page_id)?, 2);

        Ok(())
    }
}
