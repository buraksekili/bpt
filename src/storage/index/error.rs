use crate::buffer::types::BufferManagerError;

#[derive(Debug)]
pub enum BPlusTreeError {
    PageFull,
    PageNotFound,
    InvalidOperation,
    BufferPoolError(BufferManagerError),
    IndexOutOfBounds,
    KeyNotFound(usize),
    Generic(String),
    BufferTooSmall,
    NotInitialized,
    AlreadyInitialized,
    CreateLeafPage,
    PageError(String),
}

impl From<BufferManagerError> for BPlusTreeError {
    fn from(value: BufferManagerError) -> Self {
        Self::BufferPoolError(value)
    }
}

impl From<&str> for BPlusTreeError {
    fn from(value: &str) -> Self {
        Self::Generic(value.to_string())
    }
}

impl From<String> for BPlusTreeError {
    fn from(value: String) -> Self {
        Self::Generic(value)
    }
}

pub type BPlusTreeResult<R> = std::result::Result<R, BPlusTreeError>;
