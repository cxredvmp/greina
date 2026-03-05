use std::ops::{Deref, DerefMut};

use zerocopy::TryFromBytes;

use crate::{
    block::Block,
    tree::{Error, Result, node::Header},
};
