//! Unix platform support.

use std::fs::File;
use std::io::Result;
use std::os::unix::fs::FileExt;

/// Helper for writing to a file at a given offset.
///
/// This is simply a wrapper around the `write_at` unix extension method.
pub fn write_at(file: &File, mut buffer: &[u8], mut offset: u64) -> Result<usize> {
    let total = buffer.len();
    while !buffer.is_empty() {
        let n = file.write_at(buffer, offset)?;
        buffer = &buffer[n..];
        offset += n as u64;
    }
    Ok(total)
}
