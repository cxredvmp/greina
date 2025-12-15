use block::*;

pub mod block;

/// A model of a blocked physical storage device.
pub struct Storage {
    blocks: Box<[Block]>,
}

impl Storage {
    /// Constructs a zero-initialized [Storage] of given size in bytes.
    ///
    /// # Panics
    /// Panics if:
    /// - `size` is not a multiple of [BLOCK_SIZE]
    pub fn new(size: usize) -> Self {
        assert!(size.is_multiple_of(BLOCK_SIZE));
        let block_count = size / BLOCK_SIZE;
        let blocks = vec![Block::default(); block_count].into_boxed_slice();
        Self { blocks }
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Returns the copy of a persistent block at `id`.
    pub fn read_block(&self, id: usize) -> Result<Block> {
        let block = self.blocks.get(id).ok_or(Error::BlockIdOutOfBounds)?;
        Ok(*block)
    }

    /// Returns a vector of copies of persistent blocks at `ids`.
    pub fn read_blocks(&self, ids: &[usize]) -> Result<Box<[Block]>> {
        let mut blocks = Vec::with_capacity(ids.len());
        for &i in ids {
            let block = self.blocks.get(i).ok_or(Error::BlockIdOutOfBounds)?;
            blocks.push(*block);
        }
        Ok(blocks.into_boxed_slice())
    }

    /// Writes data from the `src` block into the persistent block at `id`.
    pub fn write_block(&mut self, id: usize, src: &Block) -> Result<()> {
        let dst = self.blocks.get_mut(id).ok_or(Error::BlockIdOutOfBounds)?;
        *dst = *src;
        Ok(())
    }

    /// Writes data from the 'srcs' blocks into persistent blocks at `ids`.
    ///
    /// # Panics
    /// Panics if:
    /// - lengths of `srcs` and `ids` are mismatched
    pub fn write_blocks(&mut self, ids: &[usize], srcs: &[Block]) -> Result<()> {
        assert!(
            srcs.len() == ids.len(),
            "Length of 'srcs' {} does not equal to length of 'ids' {}",
            srcs.len(),
            ids.len()
        );
        for (src, &i) in srcs.iter().zip(ids.iter()) {
            self.write_block(i, src)?
        }
        Ok(())
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    BlockIdOutOfBounds,
}
