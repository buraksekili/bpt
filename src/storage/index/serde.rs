use std::u8;

use crate::buffer::types::PageId;
use crate::storage::common::BoundedReader;
use crate::storage::index::error::{BPlusTreeError, BPlusTreeResult};
use crate::storage::index::pages::{BaseHeader, IndexPageKey, IndexPageType, InternalPageHeader, LeafPageHeader, PageHeader, INTERNAL_PAGE_HEADER_SIZE, LEAF_PAGE_HEADER_SIZE};
use bytes::BytesMut;

use super::{ENUM_IDX_INTERNAL_PAGE, ENUM_IDX_LEAF_PAGE};

pub trait Serde {
    fn serialize_into(&self, buffer: &mut BytesMut);
}

/// Helper method to parse page header from raw bytes
/// Minimum page header is 9
///     1 byte - page type
///     4 byte - size
///     4 byte - max_size
/// 9 bytes for internal nodes
/// 13 bytes for leaf nodes
pub fn parse_page_header(data: &BytesMut) -> BPlusTreeResult<(usize, PageHeader)> {
    if data.len() < INTERNAL_PAGE_HEADER_SIZE {
        return Err(BPlusTreeError::InvalidOperation);
    }

    let mut ptr = 0;
    let page_type: u8 = match data.read_u8(ptr) {
        Ok(page_type_num) => page_type_num,
        Err(_) => return Err(BPlusTreeError::InvalidOperation),
    };
    ptr += 1;

    let page_type = match page_type {
        ENUM_IDX_LEAF_PAGE => IndexPageType::LeafPage,
        ENUM_IDX_INTERNAL_PAGE => IndexPageType::InternalPage,
        _ => return Err(BPlusTreeError::InvalidOperation),
    };

    if matches!(page_type, IndexPageType::LeafPage) && data.len() < LEAF_PAGE_HEADER_SIZE {
        return Err(BPlusTreeError::InvalidOperation);
    }

    // Next 4 bytes: current size
    let size = match data.read_u32(ptr) {
        Ok(size) => size,
        Err(_) => return Err(BPlusTreeError::InvalidOperation),
    };
    ptr += 4;

    // Next 4 bytes: max size
    let max_size = match data.read_u32(ptr) {
        Ok(max_size) => max_size,
        Err(_) => return Err(BPlusTreeError::InvalidOperation),
    };
    ptr += 4;

    if matches!(page_type, IndexPageType::InternalPage) {
        return Ok((ptr, PageHeader::Internal(InternalPageHeader {
            base: BaseHeader {
                page_type,
                size: size as usize,
                max_size: max_size as usize,
            },
        })));
    }

    // Next 4 bytes: next_page_id for leaf nodes
    let next_page_id = PageId((data.read_u32(ptr)?) as usize);
    ptr += 4;

    Ok(
        (ptr, PageHeader::Leaf(LeafPageHeader {
            base: BaseHeader {
                page_type,
                size: size as usize,
                max_size: max_size as usize,
            },
            next_page_id,
        })))
}

pub fn parse_key_from(start: usize, buf: &BytesMut) -> Result<IndexPageKey, String> {
    match buf.read_u32(start) {
        Ok(num) => Ok(num as IndexPageKey),
        Err(err) => Err(err.to_string()),
    }
}