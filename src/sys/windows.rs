//! Windows platform support.

use std::fs::File;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::os::windows::io::AsRawHandle;

use windows::Win32::Foundation::HANDLE;
use windows::Win32::Storage::FileSystem::WriteFile;
use windows::Win32::System::IO::OVERLAPPED;
use windows::Win32::System::IO::OVERLAPPED_0;
use windows::Win32::System::IO::OVERLAPPED_0_0;

/// Helper for writing to a file at a given offset.
pub fn write_at(file: &File, buffer: &[u8], offset: u64) -> Result<usize> {
    let mut written = 0;
    while written < buffer.len() {
        let offset =
            offset + u64::try_from(written).map_err(|e| Error::new(ErrorKind::Other, e))?;
        let mut overlapped = OVERLAPPED {
            Anonymous: OVERLAPPED_0 {
                Anonymous: OVERLAPPED_0_0 {
                    Offset: offset as u32,
                    OffsetHigh: (offset >> 32) as u32,
                },
            },
            ..Default::default()
        };

        unsafe {
            match WriteFile(
                HANDLE(file.as_raw_handle()),
                Some(&buffer[written..]),
                None,
                Some(&mut overlapped),
            ) {
                Ok(()) => {
                    written += ((((overlapped.InternalHigh as u64) << 32)
                        | overlapped.Internal as u64)
                        - offset) as usize;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    Ok(written)
}
