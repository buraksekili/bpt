use std::fs::File;
use std::sync::{MutexGuard, PoisonError};

#[derive(Debug)]
pub enum BufferManagerError {
    IO(std::io::Error),
    FileLock(String),
    AllocatePage,
}

pub type BufferManagerResult<T> = std::result::Result<T, BufferManagerError>;

impl From<std::io::Error> for BufferManagerError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl From<PoisonError<MutexGuard<'_, File>>> for BufferManagerError {
    fn from(err: PoisonError<MutexGuard<'_, File>>) -> Self {
        Self::FileLock(err.to_string())
    }
}

pub type FrameId = usize;
pub type PageId = usize;