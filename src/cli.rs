//! Utility code for CLI implementations.

use std::collections::HashMap;
use std::fmt;

use chrono::TimeDelta;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::{self};
use tokio_util::sync::CancellationToken;
use tracing::Span;
use tracing::warn_span;
use tracing_indicatif::span_ext::IndicatifSpanExt;
use tracing_indicatif::style::ProgressStyle;

use crate::Location;
use crate::TransferEvent;
use crate::UrlExt;

/// Extension methods for [`TimeDelta`].
#[cfg_attr(docsrs, doc(cfg(feature = "cli")))]
pub trait TimeDeltaExt {
    /// Returns a display implementation for `TimeDelta` that displays days,
    /// hours, minutes, and seconds in english.
    fn english(&self) -> impl fmt::Display;
}

impl TimeDeltaExt for TimeDelta {
    fn english(&self) -> impl fmt::Display {
        struct Display(TimeDelta);

        impl fmt::Display for Display {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if self.0.num_seconds() == 0 {
                    return write!(f, "0 seconds");
                }

                let days = self.0.num_days();
                let hours = self.0.num_hours() - (days * 24);
                let minutes = self.0.num_minutes() - (days * 24 * 60) - (hours * 60);
                let seconds = self.0.num_seconds()
                    - (days * 24 * 60 * 60)
                    - (hours * 60 * 60)
                    - (minutes * 60);

                if days > 0 {
                    write!(f, "{days} day{s}", s = if days == 1 { "" } else { "s" })?;
                }

                if hours > 0 {
                    if days > 0 {
                        write!(f, ", ")?;
                    }

                    write!(f, "{hours} hour{s}", s = if hours == 1 { "" } else { "s" })?;
                }

                if minutes > 0 {
                    if days > 0 || hours > 0 {
                        write!(f, ", ")?;
                    }

                    write!(
                        f,
                        "{minutes} minute{s}",
                        s = if minutes == 1 { "" } else { "s" }
                    )?;
                }

                if seconds > 0 {
                    if days > 0 || hours > 0 || minutes > 0 {
                        write!(f, ", ")?;
                    }

                    write!(
                        f,
                        "{seconds} second{s}",
                        s = if seconds == 1 { "" } else { "s" }
                    )?;
                }

                Ok(())
            }
        }

        Display(*self)
    }
}

/// Represents statistics from transferring files.
#[cfg(feature = "cli")]
#[cfg_attr(docsrs, doc(cfg(feature = "cli")))]
#[derive(Debug, Clone, Copy, Default)]
pub struct TransferStats {
    /// The number of files that were transferred.
    pub files: usize,
    /// The total number of bytes transferred for all files.
    pub bytes: u64,
}

/// Handles events that may occur during a copy operation.
///
/// This is responsible for showing and updating progress bars for files
/// being transferred.
///
/// Used from CLI implementations.
///
/// Returns `None` if the event stream lagged and reliable statistics aren't
/// available.
///
/// Returns `Some` if the event stream did not lag and reliable statistics are
/// available.
#[cfg_attr(docsrs, doc(cfg(feature = "cli")))]
pub async fn handle_events(
    mut events: broadcast::Receiver<TransferEvent>,
    cancel: CancellationToken,
) -> Option<TransferStats> {
    struct BlockTransferState {
        /// The number of bytes that were transferred for the block.
        transferred: u64,
    }

    struct TransferState {
        /// The progress bar to display for a transfer.
        bar: Span,
        /// The total number of bytes transferred.
        transferred: u64,
        /// Block transfer state.
        block_transfers: HashMap<u64, BlockTransferState>,
    }

    let mut indeterminate = None;
    let mut transfers = HashMap::new();
    let mut stats = TransferStats::default();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            event = events.recv() => match event {
                Ok(event) if indeterminate.is_none() => match event {
                    TransferEvent::TransferStarted { id, source, destination, size, .. } => {
                        let bar = warn_span!("progress");

                        let style = match size {
                            Some(size) => {
                                bar.pb_set_length(size);
                                ProgressStyle::with_template(
                                    "[{elapsed_precise:.cyan/blue}] {bar:20.cyan/blue} \
                                    {bytes:.cyan/blue} / {total_bytes:.cyan/blue} \
                                    ({bytes_per_sec:.cyan/blue}) [ETA {eta_precise:.cyan/blue}]: \
                                    {msg}",
                                )
                                .unwrap()
                            }
                            None => ProgressStyle::with_template(
                                "[{elapsed_precise:.cyan/blue}] {spinner:.cyan/blue} \
                                {bytes:.cyan/blue} ({bytes_per_sec:.cyan/blue}): {msg}",
                            )
                            .unwrap(),
                        };

                        let message = match (&source, &destination) {
                            // Use the source path for local file copies
                            (Location::Path(path), Location::Path(_)) => format!("copying `{path}`", path = path.to_str().unwrap_or("<path not UTF-8>")),
                            // Use the source path for uploads
                            (Location::Path(path), _) => format!("uploading `{path}`", path = path.to_str().unwrap_or("<path not UTF-8>")),
                            // Use the remote URL for downloads
                            (Location::Url(url), _) => format!("downloading `{url}`", url = url.display())
                        };

                        bar.pb_set_style(&style);
                        bar.pb_set_message(&message);
                        bar.pb_start();
                        transfers.insert(
                            id,
                            TransferState {
                                bar,
                                transferred: 0,
                                block_transfers: HashMap::new(),
                            },
                        );
                    }
                    TransferEvent::BlockStarted { id, block, .. } => {
                        if let Some(transfer) = transfers.get_mut(&id) {
                            transfer
                                .block_transfers
                                .insert(block, BlockTransferState { transferred: 0 });
                        }
                    }
                    TransferEvent::BlockProgress {
                        id,
                        block,
                        transferred,
                    } => {
                        if let Some(transfer) = transfers.get_mut(&id)
                            && let Some(block) = transfer.block_transfers.get_mut(&block)
                        {
                            transfer.transferred += transferred - block.transferred;
                            block.transferred = transferred;
                            transfer.bar.pb_set_position(transfer.transferred);
                        }
                    }
                    TransferEvent::BlockCompleted { id, block, failed } => {
                        if let Some(transfer) = transfers.get_mut(&id)
                            && let Some(block) = transfer.block_transfers.get_mut(&block)
                        {
                            if failed {
                                transfer.transferred -= block.transferred;
                            }

                            transfer.bar.pb_set_position(transfer.transferred);
                        }
                    }
                    TransferEvent::TransferCompleted { id, failed } => {
                        if let Some(transfer) = transfers.remove(&id)
                            && !failed
                        {
                            stats.files += 1;
                            stats.bytes += transfer.transferred;
                        }
                    }
                },
                Ok(_) => continue,
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(_)) => {
                    // Clear the state to remove existing progress bars
                    transfers.clear();

                    // Show a single spinner progress bar for the remainder of the stream
                    let bar = warn_span!("progress");
                    bar.pb_set_style(
                        &ProgressStyle::with_template(
                            "{spinner:.cyan/blue} transfer progress is unavailable due to missed events",
                        )
                        .unwrap(),
                    );
                    bar.pb_start();

                    indeterminate = Some(bar);
                }
            }
        }
    }

    if indeterminate.is_none() {
        Some(stats)
    } else {
        None
    }
}
