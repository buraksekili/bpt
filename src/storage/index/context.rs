use crate::buffer::types::PageId;
use std::collections::VecDeque;

/// Context helps us to keep track of page IDs that are read and written during
/// certain B+Tree index operations.
///
/// For example, while inserting a node into a tree, we may need node splits which
/// eventually requires us to know parent IDs to handle node movements effectively.
/// In such scenarios, context can be used to know parent IDs of the particular node.
///
/// See the CMU BusTub implementation for reference:
/// <https://github.com/cmu-db/bustub/blob/master/src/include/storage/index/b_plus_tree.h>
pub struct Context {
    /// read_set represents a double ended queue used for tracking page IDs
    /// of nodes (pages) we have read while traversing from the root to the target leaf node.
    ///
    /// As we traverse the tree to find the node, we push the page IDs of the internal
    /// nodes we visit into `read_set`.
    ///
    /// This is essential because if we need to update parent nodes (e.g., during node splits),
    /// we can refer back to the parents via this stack.
    ///
    /// Since we push page IDs as we traverse the tree, the `read_set` effectively becomes
    /// a stack of parent page IDs, with the immediate parent of the current node at the end
    /// (`pop_back` retrieves the last inserted page ID).
    pub read_set: VecDeque<PageId>,
}

impl Context {
    pub fn new() -> Context {
        Context {
            read_set: VecDeque::new(),
        }
    }

    pub fn push_into_read_set(&mut self, pid: PageId) {
        self.read_set.push_back(pid);
    }

    pub fn pop_back(&mut self) -> Option<PageId> {
        self.read_set.pop_back()
    }
}
