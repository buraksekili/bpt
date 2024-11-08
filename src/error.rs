#[derive(Debug)]
pub struct BufferError {
    pub detail: String,
}
pub type Result<T, E = BufferError> = std::result::Result<T, E>;
