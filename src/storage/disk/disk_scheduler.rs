use crate::buffer::types::PageId;
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::types::{StorageError, StorageResult};
use bytes::{Bytes, BytesMut};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum DiskError {
    Io(std::io::Error),
    Cancelled,
}

pub enum DiskResponse {
    Read { data: BytesMut },
    Write,
    Allocate { page_id: PageId },
    Error(StorageError),
}

pub enum DiskRequest {
    Write { page_id: PageId, data: Bytes },
    Read { page_id: PageId },
    Allocate,
    Deallocate(PageId),
    Shutdown,
}

type AsyncDiskRequest = (DiskRequest, Sender<DiskResponse>);

pub struct DiskScheduler {
    tx_request_queue: Sender<AsyncDiskRequest>,
    background_thread: Option<thread::JoinHandle<()>>,
    // tx_start_worker: Sender<()>,
}

// TODO: do we need batching?
impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        let (tx_request_queue, rx_request_queue) = mpsc::channel::<AsyncDiskRequest>();
        // let (tx_start_worker, rx_start_worker) = mpsc::channel::<()>();

        let background_thread = thread::Builder::new()
            .name(String::from("disk-scheduler-background-thread"))
            .spawn(move || {
                Self::start_worker(rx_request_queue, disk_manager);
            })
            .expect("failed to spawn background thread for disk scheduler");

        DiskScheduler {
            tx_request_queue,
            background_thread: Some(background_thread),
            // tx_start_worker,
        }
    }

    pub fn schedule(&self, r: DiskRequest) -> StorageResult<Receiver<DiskResponse>> {
        let (promise, future) = Self::create_promise();

        match self.tx_request_queue.send((r, promise)) {
            Ok(_) => Ok(future),
            Err(e) => Err(StorageError::Schedule(e.to_string())),
        }
    }

    fn create_promise() -> (Sender<DiskResponse>, Receiver<DiskResponse>) {
        mpsc::channel()
    }

    fn start_worker(rx_request_queue: Receiver<AsyncDiskRequest>, disk_manager: Arc<DiskManager>) {
        loop {
            match rx_request_queue.recv() {
                Ok(disk_request) => {
                    let callback = disk_request.1;

                    match disk_request.0 {
                        DiskRequest::Write { page_id, data } => {
                            if let Err(e) = disk_manager.write_page(page_id, &data) {
                                callback.send(DiskResponse::Error(e)).unwrap();
                                continue;
                            }

                            callback.send(DiskResponse::Write).unwrap();
                        }
                        DiskRequest::Read { page_id } => match disk_manager.read_page(page_id) {
                            Ok(data) => {
                                callback.send(DiskResponse::Read { data }).unwrap();
                            }
                            Err(err) => {
                                callback.send(DiskResponse::Error(err)).unwrap();
                            }
                        },
                        DiskRequest::Allocate => match disk_manager.allocate_new_page() {
                            Ok(page_id) => {
                                callback.send(DiskResponse::Allocate { page_id }).unwrap();
                            }
                            Err(e) => {
                                callback.send(DiskResponse::Error(e)).unwrap();
                            }
                        },
                        DiskRequest::Deallocate(page_id) => {
                            match disk_manager.deallocate_page(page_id) {
                                Err(err) => callback.send(DiskResponse::Error(err)).unwrap(),
                                _ => callback.send(DiskResponse::Write).unwrap(),
                            }
                        }
                        DiskRequest::Shutdown => {
                            println!("shutting down the worker thread");
                            return;
                        }
                    }
                }
                Err(receive_error) => {
                    eprintln!("WORKER COULD NOT RECEIVE A REQUEST, {:?}", receive_error);
                }
            };
        }
    }
}

// Implement clean shutdown
impl Drop for DiskScheduler {
    fn drop(&mut self) {
        let start = Instant::now();
        let timeout = Duration::from_secs(20);

        let (tx, _) = Self::create_promise();
        let _ = self.tx_request_queue.send((DiskRequest::Shutdown, tx));

        if let Some(thread) = self.background_thread.take() {
            let remaining = timeout
                .checked_sub(start.elapsed())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                return;
            }

            if thread.join().is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::disk_manager::DiskManager;
    use crate::storage::disk::disk_scheduler::{DiskRequest, DiskResponse, DiskScheduler};
    use crate::storage::disk::PAGE_SIZE;
    use crate::util::{create_dummy_page, read_page_content};
    use bytes::BytesMut;
    use std::sync::Arc;
    use tempfile::TempDir;

    // Helper function to create a scheduler with temp directory
    fn create_test_scheduler() -> (TempDir, DiskScheduler) {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let dm = DiskManager::new(temp_dir.path()).unwrap();
        let scheduler = DiskScheduler::new(Arc::new(dm));
        (temp_dir, scheduler)
    }

    #[test]
    fn test_basic_page_lifecycle() -> StorageResult<()> {
        let (_temp_dir, scheduler) = create_test_scheduler();

        let rx = scheduler.schedule(DiskRequest::Allocate)?;
        let page_id = match rx.recv().unwrap() {
            DiskResponse::Allocate { page_id } => page_id,
            _ => panic!("unexpected response"),
        };

        const TEST_CONTENT: &str = "test data";
        let write_data = create_dummy_page(TEST_CONTENT);
        let rx = scheduler.schedule(DiskRequest::Write {
            page_id,
            data: write_data.clone(),
        })?;
        assert!(matches!(rx.recv().unwrap(), DiskResponse::Write));

        let rx = scheduler.schedule(DiskRequest::Read { page_id })?;
        match rx.recv().unwrap() {
            DiskResponse::Read { data } => {
                assert_eq!(data.len(), PAGE_SIZE);
                assert_eq!(
                    read_page_content(&write_data),
                    read_page_content(&data.clone().freeze())
                );
                assert_eq!(&data[..data.len()], &write_data[..data.len()]);
                assert!(data[TEST_CONTENT.len()..].iter().all(|&x| x == 0));
            }
            _ => panic!("unexpected response"),
        }

        Ok(())
    }

    #[test]
    fn test_concurrent_reads() -> StorageResult<()> {
        let (_temp_dir, scheduler) = create_test_scheduler();
        let scheduler = Arc::new(scheduler);

        // First, allocate and write a page
        let rx = scheduler.schedule(DiskRequest::Allocate)?;
        let page_id = match rx.recv().unwrap() {
            DiskResponse::Allocate { page_id } => page_id,
            _ => panic!("unexpected response"),
        };

        let data = BytesMut::from(&b"concurrent test data"[..]);
        let mut write_data = BytesMut::zeroed(PAGE_SIZE);
        write_data[..data.len()].copy_from_slice(&data);
        let write_data = write_data.freeze();

        let rx = scheduler.schedule(DiskRequest::Write {
            page_id,
            data: write_data,
        })?;
        rx.recv().unwrap();

        // Now spawn multiple threads to read the same page
        let mut handles = vec![];
        for _ in 0..5 {
            let scheduler_clone = scheduler.clone();
            let handle = thread::spawn(move || {
                let rx = scheduler_clone
                    .schedule(DiskRequest::Read { page_id })
                    .unwrap();
                rx.recv().unwrap()
            });
            handles.push(handle);
        }

        // Verify all reads were successful
        for handle in handles {
            match handle.join().unwrap() {
                DiskResponse::Read { data } => {
                    assert_eq!(data.len(), PAGE_SIZE);
                }
                _ => panic!("unexpected response"),
            }
        }

        Ok(())
    }

    // #[test]
    // fn test_error_handling() -> StorageResult<()> {
    //     let (_temp_dir, scheduler) = create_test_scheduler();
    //
    //     let rx = scheduler.schedule(
    //         DiskRequest::Read {
    //             page_id: PageId(99999),
    //         },
    //     )?;
    //     match rx.recv().unwrap() {
    //         DiskResponse::Error(_) => (),
    //         _ => panic!("expected error response"),
    //     }
    //
    //     let data = BytesMut::zeroed(PAGE_SIZE).freeze();
    //     let rx = scheduler.schedule(DiskRequest::Write {
    //         page_id: PageId(9999),
    //         data,
    //     })?;
    //     match rx.recv().unwrap() {
    //         DiskResponse::Error(_) => (),
    //         _ => panic!("expected error response"),
    //     }
    //
    //     Ok(())
    // }

    #[test]
    fn test_sequential_operations() -> StorageResult<()> {
        let (_temp_dir, scheduler) = create_test_scheduler();

        let mut page_ids = vec![];
        let num_pages = 5;

        // Allocate several pages
        for _ in 0..num_pages {
            let rx = scheduler.schedule(DiskRequest::Allocate)?;
            match rx.recv().unwrap() {
                DiskResponse::Allocate { page_id } => page_ids.push(page_id),
                _ => panic!("unexpected response"),
            }
        }

        // Verify pages are allocated sequentially
        for i in 1..page_ids.len() {
            assert_eq!(page_ids[i], PageId(page_ids[i - 1].0 + 1));
        }

        // Write to each page
        for (i, &page_id) in page_ids.iter().enumerate() {
            let mut data = BytesMut::zeroed(PAGE_SIZE);
            let content = format!("page content {}", i);
            data[..content.len()].copy_from_slice(content.as_bytes());
            let data = data.freeze();

            let rx = scheduler.schedule(DiskRequest::Write {
                page_id,
                data: data.clone(),
            })?;
            assert!(matches!(rx.recv().unwrap(), DiskResponse::Write));

            // Immediately read and verify
            let rx = scheduler.schedule(DiskRequest::Read { page_id })?;
            match rx.recv().unwrap() {
                DiskResponse::Read { data: read_data } => {
                    assert_eq!(&read_data[..content.len()], content.as_bytes());
                }
                _ => panic!("unexpected response"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_scheduler_shutdown() -> StorageResult<()> {
        let (_temp_dir, scheduler) = create_test_scheduler();
        let scheduler = Arc::new(scheduler);
        let scheduler_clone = scheduler.clone();

        // Spawn a thread that will try to use the scheduler after a delay
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            let rx = scheduler_clone.schedule(DiskRequest::Allocate).unwrap();
            rx.recv().unwrap()
        });

        // Drop the original scheduler, triggering shutdown
        drop(scheduler);

        // The spawned thread should still complete its operation
        match handle.join().unwrap() {
            DiskResponse::Allocate { page_id: _ } => (),
            _ => panic!("unexpected response"),
        }

        Ok(())
    }

    #[test]
    fn test_multiple_schedulers() -> StorageResult<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let dm = Arc::new(DiskManager::new(temp_dir.path())?);

        // Create multiple schedulers sharing the same DiskManager
        let scheduler1 = DiskScheduler::new(dm.clone());
        let scheduler2 = DiskScheduler::new(dm.clone());

        // Use both schedulers
        let rx1 = scheduler1.schedule(DiskRequest::Allocate)?;
        let rx2 = scheduler2.schedule(DiskRequest::Allocate)?;

        let page_id1 = match rx1.recv().unwrap() {
            DiskResponse::Allocate { page_id } => page_id,
            _ => panic!("unexpected response"),
        };

        let page_id2 = match rx2.recv().unwrap() {
            DiskResponse::Allocate { page_id } => page_id,
            _ => panic!("unexpected response"),
        };

        // Verify pages are unique
        assert_ne!(page_id1, page_id2);

        Ok(())
    }

    #[test]
    fn disk_scheduler_write_read_update() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        println!("USING {:?}", temp_dir.path());

        let dm = DiskManager::new(temp_dir.path()).unwrap();
        let disk_scheduler = DiskScheduler::new(Arc::new(dm));

        let rx = disk_scheduler.schedule(DiskRequest::Allocate).unwrap();
        let result = rx.recv().unwrap();
        let page_id = match result {
            DiskResponse::Allocate { page_id } => page_id,
            _ => panic!("unexpected response"),
        };

        let rx = disk_scheduler
            .schedule(DiskRequest::Read { page_id })
            .unwrap();
        let result = rx.recv().unwrap();
        match result {
            DiskResponse::Read { data } => {
                assert_eq!(data.len(), PAGE_SIZE);
            }
            _ => panic!("unexpected response"),
        };

        const PAGE_CONTENT: &str = "hello world";

        let rx = disk_scheduler
            .schedule(DiskRequest::Write {
                page_id,
                data: create_dummy_page(PAGE_CONTENT),
            })
            .unwrap();
        let result = rx.recv().unwrap();
        match result {
            DiskResponse::Write => {}
            DiskResponse::Error(err) => panic!("unexpected error: {}", err),
            _ => panic!("unexpected response"),
        };

        let rx = disk_scheduler
            .schedule(DiskRequest::Read { page_id })
            .unwrap();
        let result = rx.recv().unwrap();
        match result {
            DiskResponse::Read { data } => {
                assert_eq!(PAGE_SIZE, data.len());
                assert_eq!(read_page_content(&data.freeze()), PAGE_CONTENT);
            }
            DiskResponse::Error(err) => panic!("unexpected error: {}", err),
            _ => panic!("unexpected response"),
        };
    }
}
