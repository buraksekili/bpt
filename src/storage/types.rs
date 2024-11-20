use failure::Fail;
use std::fs::File;
use std::sync::{MutexGuard, PoisonError};

#[derive(Fail, Debug)]
pub enum StorageError {
    #[fail(display = "Failed to process storage API call, err: {:?}", _0)]
    Generic(String),
    #[fail(display = "Storage IO error, err: {:?}", _0)]
    IO(std::io::Error),
    #[fail(display = "Failed to process IO lock, err: {:?}", _0)]
    FileLock(String),
    #[fail(display = "Failed to read disk page, err: {:?}", _0)]
    ManagerReadPage(String),
    #[fail(display = "Failed to schedule request to disk manager, err: {:?}", _0)]
    Schedule(String),
    #[fail(display = "Failed to read log")]
    ManagerReadLog,
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl From<PoisonError<MutexGuard<'_, File>>> for StorageError {
    fn from(err: PoisonError<MutexGuard<'_, File>>) -> Self {
        Self::FileLock(err.to_string())
    }
}
