//! Unix platform support.

use std::fs::File;
use std::io::Result;
use std::os::unix::fs::FileExt;

/// Helper for writing to a file at a given offset.
///
/// This is simply a wrapper around the `write_at` unix extension method.
pub fn write_at(file: &File, buffer: &[u8], offset: u64) -> Result<usize> {
    file.write_at(buffer, offset)
}
