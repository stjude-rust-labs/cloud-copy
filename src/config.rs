//! Implementation of cloud configuration.

use std::num::NonZero;
use std::sync::Arc;
use std::thread::available_parallelism;
use std::time::Duration;

use secrecy::SecretString;
use serde::Deserialize;
use tokio_retry2::strategy::ExponentialFactorBackoff;
use tokio_retry2::strategy::MaxInterval;

/// The default number of retries for network operations.
const DEFAULT_RETRIES: usize = 5;

/// The default S3 URL region.
const DEFAULT_REGION: &str = "us-east-1";

/// Represents authentication configuration for Azure Storage.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AzureAuthConfig {
    /// The Azure Storage account name to use.
    account_name: String,
    /// The Azure Storage access key Key to use.
    access_key: SecretString,
}

impl AzureAuthConfig {
    /// Gets the Azure Storage Account Name to use for authentication.
    pub fn account_name(&self) -> &str {
        &self.account_name
    }

    /// Gets the Azure Storage access key to use for authentication.
    pub fn access_key(&self) -> &SecretString {
        &self.access_key
    }
}

/// Represents configuration for Azure Storage.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AzureConfig {
    /// Stores the auth to use for Azure Storage.
    ///
    /// If `None`, no authentication header will be put on requests.
    #[serde(default)]
    auth: Option<AzureAuthConfig>,
    /// Stores whether or not Azurite is being used.
    #[serde(default)]
    use_azurite: bool,
}

impl AzureConfig {
    /// Sets the auth to use for Azure Storage.
    pub fn with_auth(
        mut self,
        account_name: impl Into<String>,
        access_key: impl Into<SecretString>,
    ) -> Self {
        self.auth = Some(AzureAuthConfig {
            account_name: account_name.into(),
            access_key: access_key.into(),
        });
        self
    }

    /// Sets whether or not [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite) is being used.
    ///
    /// Requests for Azurite are expected to use host suffix
    /// `blob.core.windows.net.localhost`.
    ///
    /// Any URLs that use the `az` scheme will be rewritten to use that suffix.
    ///
    /// This setting is primarily intended for local testing.
    pub fn with_use_azurite(mut self, use_azurite: bool) -> Self {
        self.use_azurite = use_azurite;
        self
    }

    /// Gets the Azure Storage authentication configuration.
    ///
    /// Returns `None` if requests are not using authentication.
    pub fn auth(&self) -> Option<&AzureAuthConfig> {
        self.auth.as_ref()
    }

    /// Gets whether or not [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite) is being used.
    pub fn use_azurite(&self) -> bool {
        self.use_azurite
    }
}

/// Represents authentication configuration for S3.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct S3AuthConfig {
    /// The AWS Access Key ID to use.
    access_key_id: String,
    /// The AWS Secret Access Key to use.
    secret_access_key: SecretString,
}

impl S3AuthConfig {
    /// Gets the AWS Access Key ID to use for authentication.
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Gets the AWS Secret Access Key to use for authentication.
    pub fn secret_access_key(&self) -> &SecretString {
        &self.secret_access_key
    }
}

/// Represents configuration for AWS S3.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct S3Config {
    /// Stores the default region to apply to `s3` schemed URLs.
    #[serde(default)]
    region: Option<String>,
    /// Stores the auth to use for S3.
    ///
    /// If `None`, no authentication header will be put on requests.
    #[serde(default)]
    auth: Option<S3AuthConfig>,
    /// Stores whether or not localstack is being used.
    #[serde(default)]
    use_localstack: bool,
}

impl S3Config {
    /// Sets the region to apply to `s3` schemed URLs.
    ///
    /// Defaults to `us-east-1`.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Sets the region to apply to `s3` schemed URLs.
    ///
    /// If `None`, the default region is used.
    ///
    /// Defaults to `us-east-1`.
    pub fn with_maybe_region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    /// Sets the auth to use for S3.
    pub fn with_auth(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<SecretString>,
    ) -> Self {
        self.auth = Some(S3AuthConfig {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        });
        self
    }

    /// Sets whether or not [localstack](https://github.com/localstack/localstack) is being used.
    ///
    /// The domain suffix is expected to be `localhost.localstack.cloud`.
    ///
    /// Any URLs that use the `s3` scheme will be rewritten to use that suffix.
    ///
    /// This setting is primarily intended for local testing.
    pub fn with_use_localstack(mut self, use_localstack: bool) -> Self {
        self.use_localstack = use_localstack;
        self
    }

    /// Gets the default region to apply to `s3` schemed URLs.
    ///
    /// Defaults to `us-east-1`
    pub fn region(&self) -> &str {
        self.region.as_deref().unwrap_or(DEFAULT_REGION)
    }

    /// Gets the S3 authentication configuration.
    ///
    /// Returns `None` if requests are not using authentication.
    pub fn auth(&self) -> Option<&S3AuthConfig> {
        self.auth.as_ref()
    }

    /// Gets whether or not [localstack](https://github.com/localstack/localstack) is being used.
    pub fn use_localstack(&self) -> bool {
        self.use_localstack
    }
}

/// Represents authentication configuration for Google Cloud Storage.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct GoogleAuthConfig {
    /// The HMAC Access Key to use.
    access_key: String,
    /// The HMAC Secret to use.
    secret: SecretString,
}

impl GoogleAuthConfig {
    /// Gets the HMAC Access Key to use for authentication.
    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// Gets the HMAC Secret to use for authentication.
    pub fn secret(&self) -> &SecretString {
        &self.secret
    }
}

/// Represents configuration for Google Cloud Storage.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GoogleConfig {
    /// The auth to use for Google Cloud Storage.
    ///
    /// If `None`, no authentication header will be put on requests.
    #[serde(default)]
    auth: Option<GoogleAuthConfig>,
}

impl GoogleConfig {
    /// Sets the auth to use for Google Cloud Storage.
    pub fn with_auth(
        mut self,
        access_key: impl Into<String>,
        secret: impl Into<SecretString>,
    ) -> Self {
        self.auth = Some(GoogleAuthConfig {
            access_key: access_key.into(),
            secret: secret.into(),
        });
        self
    }

    /// Gets the Google Cloud Storage authentication configuration.
    ///
    /// Returns `None` if requests are not using authentication.
    pub fn auth(&self) -> Option<&GoogleAuthConfig> {
        self.auth.as_ref()
    }
}

/// Stores the inner configuration for [`Config`].
#[derive(Debug, Default, Deserialize)]
struct ConfigInner {
    /// Stores whether or not we're linking to cache entries.
    #[serde(default)]
    link_to_cache: bool,
    /// Stores whether or not the destination should be overwritten.
    #[serde(default)]
    overwrite: bool,
    /// Stores the block size to use for file transfers.
    #[serde(default)]
    block_size: Option<u64>,
    /// Stores the parallelism level for network operations.
    #[serde(default)]
    parallelism: Option<usize>,
    /// Stores the number of retries to attempt for network operations.
    #[serde(default)]
    retries: Option<usize>,
    /// Stores the Azure Storage configuration.
    #[serde(default)]
    azure: AzureConfig,
    /// Stores the AWS S3 configuration.
    #[serde(default)]
    s3: S3Config,
    /// Stores the Google Cloud Storage configuration.
    #[serde(default)]
    google: GoogleConfig,
}

/// Used to build a [`Config`].
#[derive(Debug, Default)]
pub struct ConfigBuilder(ConfigInner);

impl ConfigBuilder {
    /// Sets whether or not cache entries should be linked.
    ///
    /// If `link_to_cache` is `true`, then a downloaded file that is already
    /// present (and fresh) in the cache will be hard linked at the requested
    /// destination instead of copied.
    ///
    /// If the creation of the hard link fails (for example, the cache exists on
    /// a different file system than the destination path), then a copy to the
    /// destination will be made instead.
    ///
    /// Note that cache files are created read-only; if the destination is
    /// created as a hard link, it will also be read-only. It is not recommended
    /// to make the destination writable as writing to the destination path
    /// would corrupt the corresponding content entry in the cache.
    ///
    /// When `false`, a copy to the destination is always performed.
    pub fn with_link_to_cache(mut self, link_to_cache: bool) -> Self {
        self.0.link_to_cache = link_to_cache;
        self
    }

    /// Sets whether or not the destination should be overwritten.
    ///
    /// If `false` and the destination is a local file that already exists, the
    /// copy operation will fail.
    ///
    /// If `false` and the destination is a remote file, a network request will
    /// be made for the URL; if the request succeeds, the copy operation will
    /// fail.
    pub fn with_overwrite(mut self, overwrite: bool) -> Self {
        self.0.overwrite = overwrite;
        self
    }

    /// Sets the block size to use for file transfers.
    ///
    /// The default block size depends on the cloud storage service.
    pub fn with_block_size(mut self, block_size: u64) -> Self {
        self.0.block_size = Some(block_size);
        self
    }

    /// Sets the block size to use for file transfers.
    ///
    /// If `None`, the default block sized is used.
    ///
    /// The default block size depends on the cloud storage service.
    pub fn with_maybe_block_size(mut self, block_size: Option<u64>) -> Self {
        self.0.block_size = block_size;
        self
    }

    /// Sets the parallelism supported for uploads and downloads.
    ///
    /// For uploads, this is the number of blocks that may be concurrently
    /// transferred for a single file.
    ///
    /// For downloads, this is the number of files that may be concurrently
    /// downloaded.
    ///
    /// Defaults to the host's available parallelism (or 1 if it cannot be
    /// determined).
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.0.parallelism = Some(parallelism);
        self
    }

    /// Sets the parallelism supported for uploads and downloads.
    ///
    /// For uploads, this is the number of blocks that may be concurrently
    /// transferred for a single file.
    ///
    /// For downloads, this is the number of files that may be concurrently
    /// downloaded.
    ///
    /// If `None`, the default parallelism is used.
    ///
    /// Defaults to the host's available parallelism (or 1 if it cannot be
    /// determined).
    pub fn with_maybe_parallelism(mut self, parallelism: Option<usize>) -> Self {
        self.0.parallelism = parallelism;
        self
    }

    /// Sets the number of retries to attempt for network operations.
    ///
    /// Defaults to `5`.
    pub fn with_retries(mut self, retries: usize) -> Self {
        self.0.retries = Some(retries);
        self
    }

    /// Sets the number of retries to attempt for network operations.
    ///
    /// If `None`, the default retries is used.
    ///
    /// Defaults to `5`.
    pub fn with_maybe_retries(mut self, retries: Option<usize>) -> Self {
        self.0.retries = retries;
        self
    }

    /// Sets the Azure Storage configuration to use.
    pub fn with_azure(mut self, azure: AzureConfig) -> Self {
        self.0.azure = azure;
        self
    }

    /// Sets the Amazon S3 configuration to use.
    pub fn with_s3(mut self, s3: S3Config) -> Self {
        self.0.s3 = s3;
        self
    }

    /// Sets the Google Cloud Storage configuration to use.
    pub fn with_google(mut self, google: GoogleConfig) -> Self {
        self.0.google = google;
        self
    }

    /// Consumes the builder and returns the [`Config`].
    pub fn build(self) -> Config {
        Config(Arc::new(self.0))
    }
}

/// Configuration used in a cloud copy operation.
///
/// A [`Config`] is cheaply cloned.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config(Arc<ConfigInner>);

impl Config {
    /// Gets a [`ConfigBuilder`] for building a new [`Config`].
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Gets whether or not cache entries should be linked.
    ///
    /// If `link_to_cache` is `true`, then a downloaded file that is already
    /// present (and fresh) in the cache will be hard linked at the requested
    /// destination instead of copied.
    ///
    /// If the creation of the hard link fails (for example, the cache exists on
    /// a different file system than the destination path), then a copy to the
    /// destination will be made instead.
    ///
    /// Note that cache files are created read-only; if the destination is
    /// created as a hard link, it will also be read-only. It is not recommended
    /// to make the destination writable as writing to the destination path
    /// would corrupt the corresponding content entry in the cache.
    ///
    /// When `false`, a copy to the destination is always performed.
    pub fn link_to_cache(&self) -> bool {
        self.0.link_to_cache
    }

    /// Gets whether or not the destination should be overwritten.
    ///
    /// If `false` and the destination is a local file that already exists, the
    /// copy operation will fail.
    ///
    /// If `false` and the destination is a remote file, a network request will
    /// be made for the URL; if the request succeeds, the copy operation will
    /// fail.
    pub fn overwrite(&self) -> bool {
        self.0.overwrite
    }

    /// Gets the block size to use for file transfers.
    ///
    /// The default block size depends on the cloud storage service.
    pub fn block_size(&self) -> Option<u64> {
        self.0.block_size
    }

    /// Gets the parallelism supported for uploads and downloads.
    ///
    /// For uploads, this is the number of blocks that may be concurrently
    /// transferred for a single file.
    ///
    /// For downloads, this is the number of files that may be concurrently
    /// downloaded.
    ///
    /// Defaults to the host's available parallelism (or 1 if it cannot be
    /// determined).
    pub fn parallelism(&self) -> usize {
        self.0
            .parallelism
            .unwrap_or_else(|| available_parallelism().map(NonZero::get).unwrap_or(1))
    }

    /// Gets the number of retries to attempt for network operations.
    ///
    /// Defaults to `5`.
    pub fn retries(&self) -> usize {
        self.0.retries.unwrap_or(DEFAULT_RETRIES)
    }

    /// Gets the Azure Storage configuration.
    pub fn azure(&self) -> &AzureConfig {
        &self.0.azure
    }

    /// Gets the Amazon S3 configuration.
    pub fn s3(&self) -> &S3Config {
        &self.0.s3
    }

    /// Gets the Google Cloud Storage configuration.
    pub fn google(&self) -> &GoogleConfig {
        &self.0.google
    }

    /// Gets an iterator over the retry durations for network operations.
    ///
    /// Retries use an exponential power of 2 backoff, starting at 1 second with
    /// a maximum duration of 10 minutes.
    pub fn retry_durations<'a>(&self) -> impl Iterator<Item = Duration> + use<'a> {
        const INITIAL_DELAY_MILLIS: u64 = 1000;
        const BASE_FACTOR: f64 = 2.0;
        const MAX_DURATION: Duration = Duration::from_secs(600);

        ExponentialFactorBackoff::from_millis(INITIAL_DELAY_MILLIS, BASE_FACTOR)
            .max_duration(MAX_DURATION)
            .take(self.retries())
    }
}
