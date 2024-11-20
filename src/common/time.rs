// Timestamp corresponds to nanoseconds
pub type Timestamp = u128;

pub fn current_time(add_seconds: Option<u64>) -> Timestamp {
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
