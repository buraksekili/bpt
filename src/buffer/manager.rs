use crate::buffer::lruk_replacer::LRUKReplacer;
use crate::buffer::types::{BufferManagerError, BufferManagerResult, FrameId, PageId};
use crate::storage::disk::disk_scheduler::{DiskRequest, DiskScheduler};
use crate::storage::disk::PAGE_SIZE;
use bytes::BytesMut;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

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

    pub fn size(&self) -> usize { self.free_frames.len() }

    pub fn new_page(&mut self) -> BufferManagerResult<PageId> {
        let frame_id = self.allocate_frame()?;
        let page_id = self.disk_scheduler.schedule(DiskRequest::Allocate);
        Ok(2)
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
