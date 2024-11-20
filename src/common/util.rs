use crate::storage::disk::PAGE_SIZE;
use bytes::BytesMut;

pub fn create_dummy_page(content: &str) -> BytesMut {
    let mut page = BytesMut::zeroed(PAGE_SIZE);
    let content_bytes = content.as_bytes();
    page[..content_bytes.len()].copy_from_slice(content_bytes);
    page
}

pub fn read_page_content(page: &BytesMut) -> String {
    let content = page
        .iter()
        .take_while(|&&b| b != 0)
        .copied()
        .collect::<Vec<u8>>();
    String::from_utf8(content).unwrap_or_default()
}

