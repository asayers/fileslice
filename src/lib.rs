/*! Slices of files

[`FileSlice`] is to `File` what [`Bytes`](https://docs.rs/bytes/) is to
`Vec<u8>`.  Advantages over `File`:

* You can slice it, reducing the scope to a range within the original file
* Cloning is cheap (atomic addition; no syscall)
* Seeking is very cheap (normal addition; no syscall)
* Clones can't affect each other at all (the fd's real cursor is never
  used).

Once created, a `FileSlice` never changes length, even if the underlying file
does.  For example, if another process appends some data to the file, you need
to call [`FileSlice::expand`] on your slice in order to add the new data.

## Optional features

Optional integrations for crates which naturally benefit from file slicing:

* `tar`: Adds a [`slice_tarball`] helper method for splitting up a
  `tar::Archive` into a bunch of `FileSlice`s.
* `parquet`: Adds a [`ChunkReader`][parquet::file::reader::ChunkReader]
  impl for [`FileSlice`].  A parquet file contains many pages, and the decoder
  needs to interleave reads from these pages.  The `ChunkReader` impl for
  `File` accomplishes this by making many clones of the fd.  Using `FileSlice`
  instead lets you open ~7 as many parquet files before you hit your fd limit.

*/

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

/// A slice of a file
///
/// Behaves like a regular file, but emulated in userspace using the
/// `pread` API.
#[derive(Clone, Debug)]
pub struct FileSlice {
    file: Arc<File>,
    // Can go beyond `end` but must not be before `start`
    cursor: u64,
    start: u64,
    end: u64,
}

impl FileSlice {
    /// Create a new slice covering the whole file
    pub fn new(file: File) -> FileSlice {
        let end = file.metadata().unwrap().len();
        FileSlice {
            file: Arc::new(file),
            cursor: 0,
            start: 0,
            end,
        }
    }

    /// Take a sub-slice of this file
    pub fn slice<T>(&self, range: T) -> FileSlice
    where
        T: RangeBounds<u64>,
    {
        // The parameters are interpreted relative to `self`
        let start = match range.start_bound() {
            Bound::Included(x) => self.start + x,
            Bound::Excluded(x) => self.start + x + 1,
            Bound::Unbounded => self.start,
        };
        let end = match range.end_bound() {
            Bound::Included(x) => self.start + x + 1,
            Bound::Excluded(x) => self.start + x,
            Bound::Unbounded => self.end,
        };
        let end = end
            .min(self.end) // Not allowed to expand beyond `self`
            .max(start); // We require that `start <= end`
        FileSlice {
            file: self.file.clone(),
            cursor: start,
            start,
            end,
        }
    }
}

impl FileSlice {
    /// The position at which this slice begins, as a byte offset into the
    /// underlying file
    pub fn start_pos(&self) -> u64 {
        self.start
    }

    /// The position at which this slice ends, as a byte offset into the
    /// underlying file
    pub fn end_pos(&self) -> u64 {
        self.end
    }

    /// The next byte to be read, as an offset into the underlying file
    pub fn cursor_pos(&self) -> u64 {
        self.cursor
    }
}

impl Read for FileSlice {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = (self.end - self.cursor) as usize;
        let buf = if buf.len() > remaining {
            &mut buf[..remaining]
        } else {
            buf
        };

        let x;
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::FileExt;
            x = self.file.read_at(buf, self.cursor)?;
        }
        #[cfg(target_family = "windows")]
        {
            use std::os::windows::fs::FileExt;
            x = self.file.seek_read(buf, self.cursor)?;
        }
        #[cfg(target_family = "wasm")]
        {
            use std::os::wasi::fs::FileExt;
            x = self.file.read_at(buf, self.cursor)?;
        }

        self.cursor += x as u64;
        Ok(x)
    }
}

impl Seek for FileSlice {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let cursor = match pos {
            SeekFrom::Current(x) => i128::from(self.cursor) + i128::from(x),
            SeekFrom::Start(x) => i128::from(self.start + x),
            SeekFrom::End(x) => i128::from(self.end) + i128::from(x),
        };
        let cursor = match u64::try_from(cursor) {
            Ok(x) if x >= self.start => x,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Out of bounds",
                ))
            }
        };
        self.cursor = cursor;
        self.stream_position()
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        Ok(self.cursor - self.start)
    }
}

impl FileSlice {
    /// Expand the slice to cover the whole file
    ///
    /// This queries the underlying file for its current length, which may have
    /// changed since this `FileSlice` was created.  Counter-intuitively, this
    /// means that calling this method _could_ in theory cause the length of the
    /// `FileSlice` to reduce (if the underlying file has been truncated).
    pub fn expand(&mut self) {
        self.start = 0;
        self.end = self.file.metadata().unwrap().len();
    }

    /// Try to get back the inner `File`
    ///
    /// This only works if this `FileSlice` has no living clones.  If there are
    /// other `FileSlices` using the same `File`, this method will return the
    /// original `FileSlice` unmodified.
    pub fn try_unwrap(self) -> Result<File, FileSlice> {
        Arc::try_unwrap(self.file).map_err(|file| FileSlice {
            file,
            cursor: self.cursor,
            start: self.start,
            end: self.end,
        })
    }
}

#[cfg(feature = "parquet")]
mod parquet_impls {
    use super::*;
    use bytes::Bytes;
    use parquet::file::reader::{ChunkReader, Length};

    impl Length for FileSlice {
        fn len(&self) -> u64 {
            self.end - self.cursor
        }
    }

    impl ChunkReader for FileSlice {
        type T = FileSlice;

        fn get_read(&self, start: u64) -> parquet::errors::Result<FileSlice> {
            Ok(self.slice(start..self.end))
        }

        fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
            let mut buf = vec![0; length];
            self.slice(start..(start + length as u64))
                .read_exact(&mut buf)?;
            Ok(buf.into())
        }
    }
}

#[cfg(feature = "tar")]
pub fn slice_tarball(
    mut archive: tar::Archive<File>,
) -> std::io::Result<impl Iterator<Item = (tar::Header, FileSlice)>> {
    let headers = archive
        .entries_with_seek()?
        .map(move |entry| {
            let entry = entry.unwrap();
            let start = entry.raw_file_position();
            let end = start + entry.size();
            (entry.header().clone(), start, end)
        })
        .collect::<Vec<_>>();
    let file = FileSlice::new(archive.into_inner());
    Ok(headers
        .into_iter()
        .map(move |(header, start, end)| (header, file.slice(start..end))))
}
