//! Windows platform support.

use std::fs::File;
use std::io::Result;
use std::os::windows::fs::FileExt;

/// Fills a buffer from the given file at the given offset.
///
/// This command fills the entire buffer unless at EOF.
pub fn fill_buffer(file: &File, buffer: &mut [u8], offset: u64) -> Result<usize> {
    let mut read = 0;
    while read < buffer.len() {
        let offset = offset + u64::try_from(read).unwrap();
        match file.seek_read(&mut buffer[read..], offset)? {
            0 => break,
            n => read += n,
        }
    }

    Ok(read)
}
