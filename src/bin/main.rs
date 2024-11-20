fn main() {
    // let dm = DiskManager::new(current_dir().unwrap()).unwrap();
    // let disk_scheduler = DiskScheduler::new(Arc::new(dm));

    // let data = bytes_from_str("helloworld");

    // let (promise1, future1) = DiskScheduler::create_promise();
    // disk_scheduler.schedule(Some(DiskRequest {
    //     is_write: true,
    //     data: Some(data),
    //     page_id: 0,
    //     callback: promise1,
    // }));

    // future1.recv().unwrap();
    // let (promise2, future2) = DiskScheduler::create_promise();
    // disk_scheduler.schedule(Some(DiskRequest {
    //     is_write: false,
    //     data: None,
    //     page_id: 0,
    //     callback: promise2,
    // }));

    // let resp = future2.recv().unwrap();

    // let l = (u32::from_le_bytes(resp.data.as_ref().unwrap()[0..4].try_into().unwrap()) as usize);
    // let content = String::from_utf8_lossy(&resp.data.as_ref().unwrap()[4..4 + l]);
}
