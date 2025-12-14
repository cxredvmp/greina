use std::borrow::Cow;

pub type Part<'a> = Cow<'a, str>;

#[derive(Clone)]
pub struct Path<'a>(Cow<'a, str>);

impl<'a> Path<'a> {
    /// Constructs a path from a borrowed string.
    pub fn new(string: &'a str) -> Self {
        Self(Cow::Borrowed(string))
    }

    /// Constructs a path from an owned string.
    pub fn new_owned(string: String) -> Self {
        Self(Cow::Owned(string))
    }

    /// Checks whether the path starts with `/`.
    pub fn is_absolute(&self) -> bool {
        self.0.starts_with('/')
    }

    /// Returns an iterator over the parts of the path.
    pub fn as_parts(&'a self) -> impl DoubleEndedIterator<Item = Part<'a>> {
        let root = if self.0.starts_with('/') {
            Some(Cow::Borrowed(&self.0[..1]))
        } else {
            None
        };
        root.into_iter().chain(
            self.0
                .split('/')
                .filter(|s| !s.is_empty())
                .map(Cow::Borrowed),
        )
    }

    /// Splits the path into its parent path and the file name.
    /// Returns `None` if the path is empty or is the root directory `/`.
    pub fn split_last(&'a self) -> Option<(Path<'a>, Part<'a>)> {
        let name = self.as_parts().next_back()?;
        if name == "/" {
            return None;
        }
        let parent_len = name.as_ptr() as usize - self.0.as_ptr() as usize;
        let parent = &self.0[..parent_len];
        Some((Path::new(parent), name))
    }

    /// Returns the path as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Tries to construct a borrowed path from a byte slice.
    pub fn try_from_bytes(bytes: &'a [u8]) -> Result<Self, Error> {
        let string = str::from_utf8(bytes).map_err(|_| Error::CorruptedPath)?;
        Ok(Self(Cow::Borrowed(string)))
    }

    /// Tries to construct an owned path from a byte slice.
    pub fn try_from_bytes_owned(bytes: &[u8]) -> Result<Self, Error> {
        let string = str::from_utf8(bytes).map_err(|_| Error::CorruptedPath)?;
        Ok(Self(Cow::Owned(string.to_string())))
    }
}

#[derive(Debug)]
pub enum Error {
    CorruptedPath,
}
