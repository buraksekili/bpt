use crate::buffer::types::PageId;
use crate::storage::disk::PAGE_SIZE;
use crate::storage::types::{StorageError, StorageResult};
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct DiskManager {
    db_file: Mutex<File>,
    next_page_id: AtomicUsize,
}

impl DiskManager {
    pub fn new<P: AsRef<Path>>(db_file: P) -> StorageResult<Self> {
        let base_path = db_file.as_ref().to_path_buf();

        let db_path = base_path.join("testing.db");
        let db_file = OpenOptions::new()
            .create(true)
            .write(true)
            // Do not set append(true) as it will prevent `seek` not to work.
            // .append(true)
            .read(true)
            .open(&db_path)?;

        Ok(DiskManager {
            db_file: Mutex::new(db_file),
            // TODO: this won't work after restarts, fix this.
            next_page_id: AtomicUsize::new(0),
        })
    }

    pub fn deallocate_page(&self, page_id: PageId) -> StorageResult<()> {
        let mut f = self.db_file.lock();
        let offset = (page_id.0 as u64) * (PAGE_SIZE as u64);
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(&[0; PAGE_SIZE])?;
        f.flush()?;

        Ok(())
    }

    pub fn allocate_new_page(&self) -> StorageResult<PageId> {
        let mut file = self.db_file.lock();

        let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let offset = (new_page_id as u64) * (PAGE_SIZE as u64);

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&[0; PAGE_SIZE])?;
        file.flush()?;

        Ok(PageId(new_page_id))
    }

    pub fn write_page(&self, page_id: PageId, page_data: &[u8]) -> StorageResult<()> {
        let mut file = self.db_file.lock();
        let offset = (page_id.0 as u64) * (PAGE_SIZE as u64);

        if page_data.len() != PAGE_SIZE {
            return Err(StorageError::ManagerReadPage(
                "page needs to be 4kb".to_string(),
            ));
        }

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(page_data)?;
        file.flush()?;

        Ok(())
    }

    pub fn read_page(&self, page_id: PageId) -> StorageResult<Bytes> {
        let mut file = self.db_file.lock();
        let mut buffer = BytesMut::zeroed(PAGE_SIZE);

        let offset = (page_id.0 * PAGE_SIZE) as u64;

        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buffer)?;

        // Convert BytesMut to Bytes
        Ok(buffer.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{create_dummy_page, read_page_content};
    use bytes::{BufMut, BytesMut};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_page_content_creation() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temp working directory");
        let dm = DiskManager::new(temp_dir.path())?;

        let page_id = dm.allocate_new_page()?;
        let test_content = "hello world!";
        let page_data = create_dummy_page(test_content);

        dm.write_page(page_id, &page_data)?;

        let read_page = dm.read_page(page_id)?;
        let read_content = read_page_content(&read_page);
        assert_eq!(read_content, test_content);

        Ok(())
    }

    #[test]
    fn test_page_content_update() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let dm = DiskManager::new(temp_dir.path())?;

        let page_id = dm.allocate_new_page()?;
        let initial_content = "Initial content";
        let initial_page = create_dummy_page(initial_content);
        dm.write_page(page_id, &initial_page)?;

        let updated_content = "Updated content";
        let updated_page = create_dummy_page(updated_content);
        dm.write_page(page_id, &updated_page)?;

        let read_page = dm.read_page(page_id)?;
        let read_content = read_page_content(&read_page);
        assert_eq!(read_content, updated_content);

        Ok(())
    }

    #[test]
    fn test_multiple_pages() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let dm = DiskManager::new(temp_dir.path())?;

        let contents = vec!["Page One", "Page Two", "Page Three"];
        let mut page_ids = Vec::new();

        for content in &contents {
            let page_id = dm.allocate_new_page()?;
            let page_data = create_dummy_page(content);
            dm.write_page(page_id, &page_data)?;
            page_ids.push(page_id);
        }

        for (page_id, expected_content) in page_ids.iter().zip(contents.iter()) {
            let read_page = dm.read_page(*page_id)?;
            let read_content = read_page_content(&read_page);
            assert_eq!(&read_content, *expected_content);
        }

        Ok(())
    }

    #[test]
    fn test_page_size_validation() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let dm = DiskManager::new(temp_dir.path()).unwrap();

        // try writing a page with incorrect size
        let page_id = dm.allocate_new_page().unwrap();
        let invalid_page = BytesMut::with_capacity(PAGE_SIZE - 1); // Wrong size

        // this should return an error
        let result = dm.write_page(page_id, &invalid_page);
        assert!(result.is_err());
    }

    #[test]
    fn test_concurrent_page_updates() -> StorageResult<()> {
        use std::thread;

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let dm = Arc::new(DiskManager::new(temp_dir.path())?);

        let page_id = dm.allocate_new_page()?;
        let initial_page = create_dummy_page("Initial");
        dm.write_page(page_id, &initial_page)?;

        let mut handles = vec![];
        for i in 0..5 {
            let dm_clone = Arc::clone(&dm);
            let content = format!("Update {}", i);

            handles.push(thread::spawn(move || {
                let page_data = create_dummy_page(&content);
                dm_clone.write_page(page_id, &page_data).unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let final_page = dm.read_page(page_id)?;
        let final_content = read_page_content(&final_page);
        assert!(final_content.starts_with("Update "));

        Ok(())
    }

    #[test]
    fn test_page_boundaries() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let dm = DiskManager::new(temp_dir.path())?;

        let page_id = dm.allocate_new_page()?;
        let mut full_page = BytesMut::with_capacity(PAGE_SIZE);
        full_page.put_bytes(b'A', PAGE_SIZE);

        dm.write_page(page_id, &full_page)?;

        let read_page = dm.read_page(page_id)?;
        assert_eq!(read_page.len(), PAGE_SIZE);
        assert!(read_page.iter().all(|&b| b == b'A'));

        Ok(())
    }
}
