/*! Slices of files

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
use std::os::unix::fs::FileExt;
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
    pub fn slice(&self, start: u64, end: u64) -> FileSlice {
        assert!(start <= end);
        let start = self.start + start;
        let end = (self.start + end).min(self.end);
        FileSlice {
            file: self.file.clone(),
            cursor: start,
            start,
            end,
        }
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
            x = self.file.read_at(buf, self.cursor)?;
        }
        #[cfg(target_family = "windows")]
        {
            x = self.file.seek_read(buf, self.cursor)?;
        }
        #[cfg(target_family = "wasm")]
        {
            x = todo!();
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

#[cfg(feature = "parquet")]
mod parquet_impls {
    use super::*;
    use parquet::file::reader::{ChunkReader, Length};

    impl Length for FileSlice {
        fn len(&self) -> u64 {
            self.end - self.cursor
        }
    }

    impl ChunkReader for FileSlice {
        type T = FileSlice;

        fn get_read(&self, start: u64, length: usize) -> parquet::errors::Result<FileSlice> {
            Ok(self.slice(start, start + length as u64))
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
        .map(move |(header, start, end)| (header, file.slice(start, end))))
}
