use bytes::BytesMut;

use crate::storage::common::BoundedReader;
use crate::storage::types::StorageError;
use std::fs::File;
use std::ops::Deref;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{MutexGuard, PoisonError};

#[derive(Debug)]
pub enum BufferManagerError {
    IO(std::io::Error),
    FileLock(String),
    AllocatePage,
    StorageError(StorageError),
    Concurrency(String),
    Unexpected,
    PageNotFound,
    PagePinned,
    FetchPage(String),
}

pub type BufferManagerResult<T> = std::result::Result<T, BufferManagerError>;

impl From<StorageError> for BufferManagerError {
    fn from(err: StorageError) -> Self {
        BufferManagerError::StorageError(err)
    }
}

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

impl From<RecvTimeoutError> for BufferManagerError {
    fn from(err: RecvTimeoutError) -> Self {
        BufferManagerError::Concurrency(err.to_string())
    }
}

pub type FrameId = usize;
#[derive(Debug, Hash, Eq, PartialEq, Copy, Clone)]
pub struct PageId(pub usize);

impl PageId {
    pub fn serialize_into(&self, buf: &mut BytesMut) {
        buf.write_u32(self.0 as u32);
    }
}

impl Deref for PageId {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
pub const INVALID_PAGE_ID: PageId = PageId(0);
