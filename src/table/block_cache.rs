use std::sync::Arc;

use crate::block::Block;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;