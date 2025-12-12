use block::*;

pub mod block;

/// A model of a blocked physical storage device.
pub struct Storage {
    blocks: Box<[Block]>,
}

impl Storage {
    /// Constructs a zero-initialized [Storage] of given size specified in bytes.
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

    /// Returns the copy of a persistent block at specified index.
    pub fn read_block(&self, index: usize) -> Result<Block, Error> {
        let block = self.blocks.get(index).ok_or(Error::BlockIndexOutOfBounds)?;
        Ok(*block)
    }

    /// Returns a vector of copies of persistent blocks at specified indeces.
    pub fn read_blocks(&self, indeces: &[usize]) -> Result<Box<[Block]>, Error> {
        let mut blocks = Vec::with_capacity(indeces.len());
        for &i in indeces {
            let block = self.blocks.get(i).ok_or(Error::BlockIndexOutOfBounds)?;
            blocks.push(*block);
        }
        Ok(blocks.into_boxed_slice())
    }

    /// Writes data from the `src` block into the persistent block at specified index.
    pub fn write_block(&mut self, index: usize, src: &Block) -> Result<(), Error> {
        let dst = self
            .blocks
            .get_mut(index)
            .ok_or(Error::BlockIndexOutOfBounds)?;
        *dst = *src;
        Ok(())
    }

    /// Writes data from the 'srcs' blocks into persistent blocks at specified indeces.
    ///
    /// # Panics
    /// Panics if:
    /// - lengths of `srcs` and `indeces` are mismatched
    pub fn write_blocks(&mut self, indeces: &[usize], srcs: &[Block]) -> Result<(), Error> {
        assert!(
            srcs.len() == indeces.len(),
            "Length of 'srcs' {} does not equal to length of 'indeces' {}",
            srcs.len(),
            indeces.len()
        );
        for (src, &i) in srcs.iter().zip(indeces.iter()) {
            self.write_block(i, src)?
        }
        Ok(())
    }
}

/// [Storage]-related errors.
#[derive(Debug)]
pub enum Error {
    BlockIndexOutOfBounds,
}
