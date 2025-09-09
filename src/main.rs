//! Cloud storage copy utility.

use std::io::IsTerminal;
use std::io::stderr;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use byte_unit::Byte;
use byte_unit::UnitType;
use chrono::Utc;
use clap::Parser;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use cloud_copy::AzureConfig;
use cloud_copy::Config;
use cloud_copy::GoogleAuthConfig;
use cloud_copy::GoogleConfig;
use cloud_copy::HttpClient;
use cloud_copy::Location;
use cloud_copy::S3AuthConfig;
use cloud_copy::S3Config;
use cloud_copy::cli::TimeDeltaExt;
use cloud_copy::cli::handle_events;
use cloud_copy::copy;
use colored::Colorize;
use secrecy::SecretString;
use tokio::pin;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Parser, Debug)]
struct Args {
    /// The source location to copy from.
    #[clap(value_name = "SOURCE")]
    source: String,

    /// The destination location to copy to.
    #[clap(value_name = "DESTINATION")]
    destination: String,

    /// The cache directory to use for downloads.
    #[clap(long, value_name = "DIR")]
    cache_dir: Option<PathBuf>,

    /// Whether or not to create hard links to existing cached files.
    #[clap(long)]
    link_to_cache: bool,

    /// Whether or not to overwrite the destination for downloads.
    #[clap(long)]
    overwrite: bool,

    /// The block size to use for file transfers; the default block size depends
    /// on the cloud service.
    #[clap(long, value_name = "SIZE")]
    block_size: Option<u64>,

    /// The parallelism level for network operations; defaults to the host's
    /// available parallelism.
    #[clap(long, value_name = "NUM")]
    parallelism: Option<usize>,

    /// The number of retries to attempt for network operations.
    #[clap(long, value_name = "RETRIES")]
    retries: Option<usize>,

    /// The AWS Access Key ID to use.
    #[clap(long, env, value_name = "ID")]
    aws_access_key_id: Option<String>,

    /// The AWS Secret Access Key to use.
    #[clap(
        long,
        env,
        hide_env_values(true),
        value_name = "KEY",
        requires = "aws_access_key_id"
    )]
    aws_secret_access_key: Option<SecretString>,

    /// The default AWS region.
    #[clap(long, env, value_name = "REGION")]
    aws_default_region: Option<String>,

    /// The Google Cloud Storage HMAC access key to use.
    #[clap(long, env, value_name = "KEY")]
    google_hmac_access_key: Option<String>,

    /// The Google Cloud Storage HMAC secret to use.
    #[clap(
        long,
        env,
        hide_env_values(true),
        value_name = "SECRET",
        requires = "google_hmac_access_key"
    )]
    google_hmac_secret: Option<SecretString>,

    /// The verbosity level.
    #[command(flatten)]
    verbosity: Verbosity<WarnLevel>,
}

impl Args {
    /// Converts the arguments into a `Config`, HTTP client, source, and
    /// destination.
    fn into_parts(self) -> (Config, HttpClient, String, String) {
        let s3_auth =
            if let (Some(id), Some(key)) = (self.aws_access_key_id, self.aws_secret_access_key) {
                Some(S3AuthConfig {
                    access_key_id: id,
                    secret_access_key: key,
                })
            } else {
                None
            };

        let google_auth = if let (Some(access_key), Some(secret)) =
            (self.google_hmac_access_key, self.google_hmac_secret)
        {
            Some(GoogleAuthConfig { access_key, secret })
        } else {
            None
        };

        let config = Config {
            link_to_cache: self.link_to_cache,
            overwrite: self.overwrite,
            block_size: self.block_size,
            parallelism: self.parallelism,
            retries: self.retries,
            azure: AzureConfig { use_azurite: false },
            s3: S3Config {
                use_localstack: false,
                region: self.aws_default_region,
                auth: s3_auth,
            },
            google: GoogleConfig { auth: google_auth },
        };

        let client = self
            .cache_dir
            .map(HttpClient::new_with_cache)
            .unwrap_or_default();

        (config, client, self.source, self.destination)
    }
}

/// Runs the application.
async fn run(cancel: CancellationToken) -> Result<()> {
    let args = Args::parse();
    match std::env::var("RUST_LOG") {
        Ok(_) => {
            let indicatif_layer = IndicatifLayer::new();

            let subscriber = tracing_subscriber::fmt::Subscriber::builder()
                .with_env_filter(EnvFilter::from_default_env())
                .with_ansi(stderr().is_terminal())
                .with_writer(indicatif_layer.get_stderr_writer())
                .finish()
                .with(indicatif_layer);

            tracing::subscriber::set_global_default(subscriber)?;
        }
        Err(_) => {
            let indicatif_layer = IndicatifLayer::new();

            let subscriber = tracing_subscriber::fmt::Subscriber::builder()
                .with_max_level(args.verbosity)
                .with_ansi(stderr().is_terminal())
                .with_writer(indicatif_layer.get_stderr_writer())
                .finish()
                .with(indicatif_layer);

            tracing::subscriber::set_global_default(subscriber)?;
        }
    }

    // Only handle transfer events if for a terminal to display the progress
    let (events_tx, events_rx) = broadcast::channel(1000);
    let c = cancel.clone();
    let handler = tokio::spawn(async move { handle_events(events_rx, c).await });

    let start = Utc::now();

    let (config, client, source, destination) = args.into_parts();
    let result = copy(
        config,
        client,
        &source,
        &destination,
        cancel,
        Some(events_tx),
    )
    .await
    .with_context(|| {
        format!(
            "failed to copy `{source}` to `{destination}`",
            source = Location::new(&source),
            destination = Location::new(&destination),
        )
    });

    let end = Utc::now();

    let stats = handler.await.expect("failed to join events handler");

    // Print the statistics upon success
    if result.is_ok() {
        let delta = end - start;
        let seconds = delta.num_seconds();

        println!(
            "{files} file{s} copied with a total of {bytes:#} transferred in {time} ({speed})",
            files = stats.files.to_string().cyan(),
            s = if stats.files == 1 { "" } else { "s" },
            bytes = format!(
                "{:#.3}",
                Byte::from_u64(stats.bytes).get_appropriate_unit(UnitType::Binary)
            )
            .cyan(),
            time = delta.english().to_string().cyan(),
            speed = format!(
                "{bytes:#.3}/s",
                bytes = if seconds == 0 || stats.bytes < 60 {
                    Byte::from_u64(stats.bytes)
                } else {
                    Byte::from_u64(stats.bytes / seconds as u64)
                }
                .get_appropriate_unit(UnitType::Binary)
            )
            .cyan()
        );
    }

    result
}

#[cfg(unix)]
/// An async function that waits for a termination signal.
async fn terminate(cancel: CancellationToken) {
    use tokio::select;
    use tokio::signal::unix::SignalKind;
    use tokio::signal::unix::signal;
    use tracing::info;

    let mut sigterm = signal(SignalKind::terminate()).expect("failed to create SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to create SIGINT handler");

    let signal = select! {
        _ = sigterm.recv() => "SIGTERM",
        _ = sigint.recv() => "SIGINT",
    };

    info!("received {signal} signal: initiating shutdown");
    cancel.cancel();
}

#[cfg(windows)]
/// An async function that waits for a termination signal.
async fn terminate(cancel: CancellationToken) {
    use tokio::signal::windows::ctrl_c;
    use tracing::info;

    let mut signal = ctrl_c().expect("failed to create ctrl-c handler");
    signal.recv().await;

    info!("received Ctrl-C signal: initiating shutdown");
    cancel.cancel();
}

#[tokio::main]
async fn main() {
    let cancel = CancellationToken::new();

    let run = run(cancel.clone());
    pin!(run);

    loop {
        tokio::select! {
            biased;
            _ = terminate(cancel.clone()) => continue,
            r = &mut run => {
                if let Err(e) = r {
                    eprintln!(
                        "{error}: {e:?}",
                        error = if std::io::stderr().is_terminal() {
                            "error".red().bold()
                        } else {
                            "error".normal()
                        }
                    );

                    std::process::exit(1);
                }

                break;
            }
        }
    }
}
