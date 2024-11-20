use crate::buffer::lruk_replacer::LRUKReplacer;
use crate::buffer::types::{BufferManagerError, BufferManagerResult, FrameId, PageId};
use crate::storage::disk::disk_scheduler::{DiskRequest, DiskResponse, DiskScheduler};
use crate::storage::disk::PAGE_SIZE;
use bytes::BytesMut;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

fn empty_data(size: Option<usize>) -> BytesMut {
    if let Some(size) = size {
        return BytesMut::zeroed(size);
    }

    BytesMut::zeroed(PAGE_SIZE)
}

// FrameHeader corresponds to the frame (page) in memory.
struct FrameHeader {
    frame_id: FrameId,
    pin_count: u32,
    is_dirty: bool,
    data: BytesMut,
}

impl FrameHeader {
    pub fn new(frame_id: FrameId) -> Self {
        Self {
            frame_id,
            is_dirty: false,
            pin_count: 0,
            data: empty_data(None),
        }
    }
}


// BufferManager manages data between disk and memory.
// Each data contains PAGE_SIZE bytes in it (4KB by default).
pub struct BufferManager {
    next_page_id: AtomicU32,
    // frames corresponds to the current buffer.
    frames: Vec<Arc<RwLock<FrameHeader>>>,

    // page_table keeps track of the mapping between pages and buffer pool frames.
    page_table: HashMap<PageId, FrameId>,
    free_frames: Vec<FrameId>,
    replacer: Arc<RwLock<LRUKReplacer>>,
    disk_scheduler: Arc<DiskScheduler>,
}

impl BufferManager {
    pub fn new(num_frames: usize, disk_scheduler: DiskScheduler, replacer: LRUKReplacer) -> BufferManager {
        // TODO: check if replacer len and num_frames match
        let next_page_id = AtomicU32::new(0);
        let mut free_frames = Vec::new();
        let mut frames = Vec::new();
        for i in 0..num_frames {
            frames.push(Arc::new(RwLock::new(FrameHeader::new(i))));
            free_frames.push(i);
        }

        BufferManager {
            frames,
            next_page_id,
            page_table: HashMap::new(),
            free_frames,
            replacer: Arc::new(RwLock::new(replacer)),
            disk_scheduler: Arc::new(disk_scheduler),
        }
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
        let rx = self.disk_scheduler
            .schedule(request)
            .map_err(BufferManagerError::from)?;

        let response = rx.recv_timeout(timeout).map_err(BufferManagerError::from)?;
        extractor(response)
    }

    pub fn size(&self) -> usize { self.free_frames.len() }

    pub fn new_page(&mut self) -> BufferManagerResult<PageId> {
        let frame_id = self.allocate_frame()?;

        let page_id = self.handle_disk_response(
            DiskRequest::Allocate,
            Duration::from_secs(1),
            |response| match response {
                DiskResponse::Allocate { page_id } => Ok(page_id),
                DiskResponse::Error(err) => Err(err.into()),
                _ => Err(BufferManagerError::Unexpected),
            },
        )?;

        self.page_table.insert(page_id, frame_id);

        {
            let mut frame = self.frames[frame_id].write();
            *frame = FrameHeader::new(frame_id);
            frame.pin_count += 1;
        }
        {
            self.replacer.write().record_access(frame_id);
        }

        Ok(page_id)
    }

    pub fn delete_page(&mut self, page_id: PageId) -> BufferManagerResult<()> {
        let frame_id = self.page_table.get(&page_id);
        if frame_id.is_none() {
            return Err(BufferManagerError::PageNotFound);
        }

        let frame_id = *frame_id.unwrap();
        {
            let pin_count = self.frames[frame_id].read().pin_count;
            if pin_count > 0 {
                return Err(BufferManagerError::PagePinned);
            }
        }

        self.page_table.remove(&page_id);
        self.free_frames.push(frame_id);
        self.replacer.write().remove(frame_id);
        self.disk_scheduler.schedule(DiskRequest::Deallocate(frame_id));

        Ok(())
    }

    pub fn get_pin_count(&self, page_id: PageId) -> BufferManagerResult<u32> {
        let frame_id = self.page_table.get(&page_id);
        if frame_id.is_none() {
            return Err(BufferManagerError::PageNotFound);
        }

        let frame_id = *frame_id.unwrap();
        Ok(self.frames[frame_id].read().pin_count)
    }

    fn allocate_frame(&mut self) -> BufferManagerResult<FrameId> {
        if let Some(page_id) = self.free_frames.pop() {
            return Ok(page_id);
        }

        let mut g = self.replacer.write();
        if let Some(page_id) = g.evict() {
            return Ok(page_id);
        }
        drop(g);

        Err(BufferManagerError::AllocatePage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::types::BufferManagerResult;
    use crate::storage::disk::disk_manager::DiskManager;
    use tempfile::TempDir;


    #[test]
    fn simple() -> BufferManagerResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        const BUFFER_SIZE: usize = 10;
        let ds = DiskScheduler::new(
            Arc::new(
                DiskManager::new(temp_dir.path()).unwrap(),
            ),
        );
        let replacer = LRUKReplacer::new(2, BUFFER_SIZE);
        let mut m = BufferManager::new(BUFFER_SIZE, ds, replacer);
        assert_eq!(m.size(), BUFFER_SIZE);
        assert_eq!(m.frames.len(), BUFFER_SIZE);
        assert_eq!(m.free_frames.len(), BUFFER_SIZE);

        {
            for i in 0..10 {
                let page_id = m.new_page()?;
                assert_eq!(page_id, i);
                assert_eq!(m.free_frames.len(), BUFFER_SIZE - (i + 1));
            }
        }

        {
            for i in 0..10 {
                let pin_count = m.get_pin_count(i)?;
                assert_eq!(pin_count, 1);
                let result = m.delete_page(i);
                assert!(matches!(result, Err(BufferManagerError::PagePinned)));
            }
        }


        Ok(())
    }
}
