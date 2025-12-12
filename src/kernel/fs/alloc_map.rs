use zerocopy::{Immutable, IntoBytes, TryFromBytes};

/// Tracks allocation state of objects.
pub struct AllocMap {
    flags: Box<[AllocFlag]>,
}

impl AllocMap {
    /// Constructs a zero-initialized [AllocMap] that represents a list of objects of given count.
    pub fn new(count: usize) -> Self {
        AllocMap {
            flags: vec![AllocFlag::default(); count].into_boxed_slice(),
        }
    }

    // NOTE: Explore using the Next-fit algorithm
    /// Tries to find a contiguous span of free objects of `count` length, using the First-fit algorithm.
    /// On success, returns a (start, end) tuple, representing an exclusive range of indices.
    fn find_free(&self, count: usize) -> Option<(usize, usize)> {
        if count == 0 {
            return None;
        }
        let mut start = 0;
        for (i, flag) in self.flags.iter().enumerate() {
            if *flag == AllocFlag::Used {
                start = i + 1;
                continue;
            }
            if (i + 1) - start == count {
                return Some((start, i + 1));
            }
        }
        None
    }

    /// Tries to allocate a contiguous span of objects of `count` length.
    /// On success, returns a (start, end) tuple, representing an exclusive range of indices.
    pub fn allocate(&mut self, count: usize) -> Result<(usize, usize), Error> {
        let span = self.find_free(count).ok_or(Error::OutOfSpace)?;
        for flag in &mut self.flags[span.0..span.1] {
            *flag = AllocFlag::Used;
        }
        Ok(span)
    }

    /// Tries to allocate the object at given index.
    pub fn allocate_at(&mut self, index: usize) -> Result<(), Error> {
        let flag = self.flags.get_mut(index).ok_or(Error::IndexOutOfBounds)?;
        if *flag == AllocFlag::Used {
            return Err(Error::ObjectOccupied);
        }
        *flag = AllocFlag::Used;
        Ok(())
    }

    /// Tries to allocate the specified span of objects.
    ///
    /// # Panics
    /// Panics if:
    /// - `span` is not a valid span
    pub fn allocate_span(&mut self, span: (usize, usize)) -> Result<(), Error> {
        assert!(span.0 < span.1);
        let span = self
            .flags
            .get_mut(span.0..span.1)
            .ok_or(Error::IndexOutOfBounds)?;
        if span.iter().any(|&f| f == AllocFlag::Used) {
            return Err(Error::ObjectOccupied);
        }
        span.fill(AllocFlag::Used);
        Ok(())
    }

    /// Marks the specified span of objects as free.
    ///
    /// # Panics
    /// Panics if:
    /// - `span` is not a valid span
    pub fn free(&mut self, span: (usize, usize)) -> Result<(), Error> {
        assert!(span.0 < span.1);
        let span = self
            .flags
            .get_mut(span.0..span.1)
            .ok_or(Error::IndexOutOfBounds)?;
        span.fill(AllocFlag::Free);
        Ok(())
    }

    /// Returns a view of the allocation map as a slice of [AllocFlag].
    pub fn as_slice(&self) -> &[AllocFlag] {
        &self.flags
    }

    /// Constructs [AllocMap] from a slice of [AllocFlag].
    pub fn from_slice(flags: &[AllocFlag]) -> Self {
        Self {
            flags: Box::from(flags),
        }
    }
}

/// Represents allocation state of an object.
#[derive(Default, Clone, Copy, PartialEq, Eq)]
#[derive(TryFromBytes, IntoBytes, Immutable)]
#[repr(u8)]
pub enum AllocFlag {
    #[default]
    Free,
    Used,
}

/// [AllocMap]-related errors.
#[derive(Debug)]
pub enum Error {
    IndexOutOfBounds,
    ObjectOccupied,
    OutOfSpace,
}
