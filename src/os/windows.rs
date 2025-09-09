//! Windows platform support.

use std::fs::File;
use std::io::Result;
use std::os::windows::io::AsRawHandle;

use windows::Win32::Foundation::*;
use windows::Win32::Storage::FileSystem::*;
use windows::Win32::System::IO::*;

/// Fills a buffer from the given file at the given offset.
///
/// This command fills the entire buffer unless at EOF.
pub fn fill_buffer(file: &File, buffer: &mut [u8], offset: u64) -> Result<usize> {
    let mut read = 0;
    while read < buffer.len() {
        let offset = offset + u64::try_from(read).unwrap();
        let mut overlapped = OVERLAPPED {
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: (offset & 0xFFFFFFFF) as u32,
                    OffsetHigh: (offset >> 32) as u32,
                },
            },
            ..Default::default()
        };

        unsafe {
            let mut this_read = 0;
            match ReadFile(
                HANDLE(file.as_raw_handle()),
                Some(&mut buffer[read..]),
                Some(&mut this_read),
                Some(&mut overlapped),
            ) {
                Ok(()) => {
                    read += this_read as usize;
                }
                Err(e) if e.code() == ERROR_HANDLE_EOF.into() => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    Ok(read)
}
