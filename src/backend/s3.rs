//! Implementation of the S3 storage backend.

use std::borrow::Cow;

use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use http_cache_stream_reqwest::Cache;
use http_cache_stream_reqwest::storage::DefaultCacheStorage;
use reqwest::Body;
use reqwest::Request;
use reqwest::Response;
use reqwest::StatusCode;
use reqwest::header;
use reqwest::header::HeaderValue;
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::broadcast;
use tracing::debug;
use url::Host;
use url::Url;

use crate::BLOCK_SIZE_THRESHOLD;
use crate::Config;
use crate::Error;
use crate::HttpClient;
use crate::ONE_MEBIBYTE;
use crate::Result;
use crate::S3AuthConfig;
use crate::S3Config;
use crate::TransferEvent;
use crate::USER_AGENT;
use crate::UrlExt as _;
use crate::backend::StorageBackend;
use crate::backend::Upload;
use crate::backend::auth::s3::RequestSigner;
use crate::backend::auth::s3::SignatureProvider;
use crate::backend::format_range_header;
use crate::sha256_hex_string;
use crate::streams::ByteStream;
use crate::streams::TransferStream;

/// The root domain for AWS.
const AWS_ROOT_DOMAIN: &str = "amazonaws.com";

/// The maximum number of parts in an upload.
const MAX_PARTS: u64 = 10000;

/// The minimum size of a part in bytes (5 MiB); applies to every part except
/// the last.
const MIN_PART_SIZE: u64 = 5 * ONE_MEBIBYTE;

/// The maximum size in bytes (5 GiB) for an upload part.
const MAX_PART_SIZE: u64 = MIN_PART_SIZE * 1024;

/// The maximum size of a file on S3 in bytes (5 TiB).
const MAX_FILE_SIZE: u64 = MAX_PART_SIZE * 1024;

/// The AWS date header name.
const AWS_DATE_HEADER: &str = "x-amz-date";

/// The AWS content SHA256 header name.
///
/// This is the SHA256 of each upload part for multipart uploads.
const AWS_CONTENT_SHA256_HEADER: &str = "x-amz-content-sha256";

/// The AWS content digest header name.
///
/// This is the content digest for the entire object.
pub(crate) const AWS_CONTENT_DIGEST_HEADER: &str = "x-amz-meta-content-digest";

/// Represents a S3-specific copy operation error.
#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    /// The specified S3 block size exceeds the maximum.
    #[error("S3 block size cannot exceed {MAX_PART_SIZE} bytes")]
    InvalidBlockSize,
    /// The source size exceeds the supported maximum size.
    #[error("the size of the source file exceeds the supported maximum of {MAX_FILE_SIZE} bytes")]
    MaximumSizeExceeded,
    /// Invalid URL with an `s3` scheme.
    #[error("invalid URL with `s3` scheme: the URL is not in a supported format")]
    InvalidScheme,
    /// A "path-style" URL is missing the bucket.
    #[error("URL is missing the bucket in the path")]
    MissingBucket,
    /// The S3 secret access key is invalid.
    #[error("invalid S3 secret access key")]
    InvalidSecretAccessKey,
    /// The response was missing an ETag header.
    #[error("response from server was missing an ETag header")]
    ResponseMissingETag,
    /// The bucket name in the URL was invalid.
    #[error("the bucket name specified in the URL is invalid")]
    InvalidBucketName,
    /// The configured S3 endpoint is invalid.
    #[error(
        "the configured S3 endpoint is invalid: it must be an `http` or `https` URL with a host"
    )]
    InvalidEndpoint,
    /// Unexpected response from server.
    #[error("unexpected {status} response from server: failed to deserialize response contents: {error}", status = .status.as_u16())]
    UnexpectedResponse {
        /// The response status code.
        status: reqwest::StatusCode,
        /// The deserialization error.
        error: serde_xml_rs::Error,
    },
}

/// Represents content in a list operation results.
#[derive(Debug, Deserialize)]
pub struct Content {
    /// The key of the S3 object.
    #[serde(rename = "Key")]
    pub key: String,
}

/// Represents results of a list operation.
#[derive(Debug, Deserialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListBucketResult {
    /// The contents of the results.
    #[serde(default, rename = "Contents")]
    pub contents: Vec<Content>,
    /// The next continuation token.
    #[serde(rename = "NextContinuationToken", default)]
    pub token: Option<String>,
}

/// Represents the result of initiating an upload.
#[derive(Default, Deserialize)]
#[serde(rename = "InitiateMultipartUploadResult")]
pub struct InitiateMultipartUploadResult {
    /// The upload identifier.
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

/// Represents a S3 signature provider.
pub struct S3SignatureProvider<'a> {
    /// The region for the request.
    region: &'a str,
    /// The S3 authentication configuration.
    auth: &'a S3AuthConfig,
}

impl SignatureProvider for S3SignatureProvider<'_> {
    fn algorithm(&self) -> &str {
        "AWS4-HMAC-SHA256"
    }

    fn secret_key_prefix(&self) -> &str {
        "AWS4"
    }

    fn request_type(&self) -> &str {
        "aws4_request"
    }

    fn region(&self) -> &str {
        self.region
    }

    fn service(&self) -> &str {
        "s3"
    }

    fn date_header_name(&self) -> &str {
        AWS_DATE_HEADER
    }

    fn content_hash_header_name(&self) -> &str {
        AWS_CONTENT_SHA256_HEADER
    }

    fn access_key_id(&self) -> &str {
        self.auth.access_key_id()
    }

    fn secret_access_key(&self) -> &str {
        self.auth.secret_access_key().expose_secret()
    }
}

/// Inserts the authentication header to the request.
///
/// Does nothing if the S3 configuration does not have authentication.
fn insert_authentication_header(
    config: &S3Config,
    date: DateTime<Utc>,
    request: &mut Request,
) -> Result<()> {
    let Some(auth) = config.auth() else {
        return Ok(());
    };

    let region = S3Url::parse(config, request.url())
        .expect("URL should be a supported S3 URL")
        .region;
    let signer = RequestSigner::new(S3SignatureProvider { region, auth });
    let auth = signer
        .sign(date, request)
        .ok_or(S3Error::InvalidSecretAccessKey)?;
    request.headers_mut().insert(
        header::AUTHORIZATION,
        HeaderValue::try_from(auth).expect("value should be valid"),
    );
    Ok(())
}

/// Determines if the given domain is a root domain for S3 requests.
fn is_root_domain(domain: &str) -> bool {
    domain.eq_ignore_ascii_case(AWS_ROOT_DOMAIN) || domain.eq_ignore_ascii_case("localhost")
}

/// Represents the parts of an `http` or `https` URL for an S3 object.
struct S3Url<'a> {
    /// The region of the bucket.
    region: &'a str,
    /// The name of the bucket.
    bucket: &'a str,
    /// The path to the object within the bucket, including the leading `/`.
    ///
    /// This is empty if the URL has no path to an object (i.e. a path-style
    /// URL that is only the bucket).
    path: &'a str,
    /// Whether or not the bucket is in the path of the URL (path-style) instead
    /// of the host of the URL (virtual host style).
    path_style: bool,
    /// Whether or not the URL is for the configured endpoint.
    custom_endpoint: bool,
}

impl<'a> S3Url<'a> {
    /// Parses the given URL.
    ///
    /// Returns `None` if the URL is not for AWS S3 or the configured endpoint.
    fn parse(config: &'a S3Config, url: &'a Url) -> Option<Self> {
        Self::parse_aws(url).or_else(|| Self::parse_endpoint(config, url))
    }

    /// Parses a URL for AWS S3 (or an emulator that uses the `localhost` root
    /// domain).
    fn parse_aws(url: &'a Url) -> Option<Self> {
        let domain = url.domain()?;

        if domain.starts_with("s3.") || domain.starts_with("S3.") {
            // Path-style URL of the form https://s3.<region>.amazonaws.com/<bucket>/<path>
            let (region, root) = domain[3..].split_once('.')?;
            if region.is_empty() || !is_root_domain(root) {
                return None;
            }

            Self::parse_path_style(url, region, false)
        } else {
            // Virtual host style URL of the form https://<bucket>.s3.<region>.amazonaws.com/<path>
            let mut parts = domain.splitn(4, '.');
            let (bucket, service, region, root) =
                (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
            if bucket.is_empty()
                || region.is_empty()
                || !service.eq_ignore_ascii_case("s3")
                || !is_root_domain(root)
            {
                return None;
            }

            Some(Self {
                region,
                bucket,
                path: url.path(),
                path_style: false,
                custom_endpoint: false,
            })
        }
    }

    /// Parses a URL for the configured endpoint.
    ///
    /// The URL must use the same scheme and port as the endpoint.
    fn parse_endpoint(config: &'a S3Config, url: &'a Url) -> Option<Self> {
        let endpoint = config.endpoint()?;
        if url.scheme() != endpoint.scheme()
            || url.port_or_known_default() != endpoint.port_or_known_default()
        {
            return None;
        }

        let host = url.host_str()?;
        let endpoint_host = endpoint.host_str()?;
        if host == endpoint_host {
            // Path-style URL of the form <endpoint>/<bucket>/<path>
            Self::parse_path_style(url, config.region(), true)
        } else {
            // Virtual host style URL of the form <scheme>://<bucket>.<endpoint host>/<path>
            //
            // A host that is an IP address cannot have a bucket subdomain
            if !matches!(endpoint.host(), Some(Host::Domain(_))) {
                return None;
            }

            let bucket = host.strip_suffix(endpoint_host)?.strip_suffix('.')?;
            if bucket.is_empty() {
                return None;
            }

            Some(Self {
                region: config.region(),
                bucket,
                path: url.path(),
                path_style: false,
                custom_endpoint: true,
            })
        }
    }

    /// Parses the bucket and path of a path-style URL.
    fn parse_path_style(url: &'a Url, region: &'a str, custom_endpoint: bool) -> Option<Self> {
        let bucket = url.path_segments()?.next()?;
        if bucket.is_empty() {
            return None;
        }

        Some(Self {
            region,
            bucket,
            path: url.path().strip_prefix('/')?.strip_prefix(bucket)?,
            path_style: true,
            custom_endpoint,
        })
    }
}

/// Extension trait for response.
trait ResponseExt {
    /// Converts an error response from S3 into an `Error`.
    async fn into_error(self) -> Error;
}

impl ResponseExt for Response {
    async fn into_error(self) -> Error {
        /// Represents an error response.
        #[derive(Default, Deserialize)]
        #[serde(rename = "Error")]
        struct ErrorResponse {
            /// The error message.
            #[serde(rename = "Message")]
            message: String,
        }

        let status = self.status();

        // Improve a 301 response which is likely due to using the wrong region
        if status == StatusCode::MOVED_PERMANENTLY {
            return Error::Server {
                status,
                message: "the AWS region being used may not be the correct region for the storage \
                          bucket"
                    .into(),
            };
        }

        let text: String = match self.text().await {
            Ok(text) => text,
            Err(e) => return e.into(),
        };

        if text.is_empty() {
            return Error::Server {
                status,
                message: text,
            };
        }

        let message = match serde_xml_rs::from_str::<ErrorResponse>(&text) {
            Ok(response) => response.message,
            Err(e) => {
                return S3Error::UnexpectedResponse { status, error: e }.into();
            }
        };

        Error::Server { status, message }
    }
}

/// Represents a completed part of an upload.
#[derive(Default, Clone, Serialize)]
#[serde(rename = "Part")]
pub struct S3UploadPart {
    /// The part number of the upload.
    #[serde(rename = "PartNumber")]
    number: u64,
    /// The ETag of the part.
    #[serde(rename = "ETag")]
    etag: String,
}

/// Represents an S3 file upload.
pub struct S3Upload {
    /// The configuration to use for the upload.
    config: Config,
    /// The HTTP client to use for uploading.
    client: HttpClient,
    /// The URL of the object being uploaded.
    url: Url,
    /// The identifier of this upload.
    id: String,
    /// The channel for sending progress updates.
    events: Option<broadcast::Sender<TransferEvent>>,
}

impl Upload for S3Upload {
    type Part = S3UploadPart;

    async fn put(&self, id: u64, block: u64, bytes: Bytes) -> Result<Option<Self::Part>> {
        // See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html

        debug!(
            "sending PUT request for block {block} of `{url}`",
            url = self.url.display()
        );

        let mut url = self.url.clone();

        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("partNumber", &format!("{number}", number = block + 1));
            pairs.append_pair("uploadId", &self.id);
        }

        let digest = sha256_hex_string(&bytes);
        let length = bytes.len();
        let body = Body::wrap_stream(TransferStream::new(
            ByteStream::new(bytes),
            id,
            block,
            0,
            self.events.clone(),
        ));

        let date = Utc::now();
        let mut request = self
            .client
            .put(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::CONTENT_LENGTH, length)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(AWS_DATE_HEADER, date.format("%Y%m%dT%H%M%SZ").to_string())
            .header(AWS_CONTENT_SHA256_HEADER, &digest)
            .body(body)
            .build()?;

        insert_authentication_header(self.config.s3(), date, &mut request)?;

        let response = self.client.execute(request).await?;
        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        let etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .ok_or(S3Error::ResponseMissingETag)?;

        Ok(Some(S3UploadPart {
            number: block + 1,
            etag: etag.to_string(),
        }))
    }

    async fn finalize(&self, parts: &[Self::Part]) -> Result<()> {
        // See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html

        /// Represents the request body for completing a multipart upload.
        #[derive(Serialize)]
        #[serde(rename = "CompleteMultipartUpload")]
        struct CompleteUpload<'a> {
            /// The parts of the upload.
            #[serde(rename = "Part")]
            parts: &'a [S3UploadPart],
        }

        debug!(
            "sending POST request to finalize upload of `{url}`",
            url = self.url.display()
        );

        let mut url = self.url.clone();

        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("uploadId", &self.id);
        }

        let body = serde_xml_rs::SerdeXml::new()
            .default_namespace("http://s3.amazonaws.com/doc/2006-03-01/")
            .to_string(&CompleteUpload { parts })
            .expect("should serialize");

        let date = Utc::now();
        let mut request = self
            .client
            .post(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::CONTENT_LENGTH, body.len())
            .header(header::CONTENT_TYPE, "application/xml")
            .header(AWS_DATE_HEADER, date.format("%Y%m%dT%H%M%SZ").to_string())
            .header(AWS_CONTENT_SHA256_HEADER, sha256_hex_string(&body))
            .body(body)
            .build()?;

        insert_authentication_header(self.config.s3(), date, &mut request)?;

        let response = self.client.execute(request).await?;
        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        Ok(())
    }
}

/// Represents the S3 storage backend.
pub struct S3StorageBackend {
    /// The config to use for transferring files.
    config: Config,
    /// The HTTP client to use for transferring files.
    client: HttpClient,
    /// The channel for sending transfer events.
    events: Option<broadcast::Sender<TransferEvent>>,
}

impl S3StorageBackend {
    /// Constructs a new S3 storage backend.
    pub fn new(
        config: Config,
        client: HttpClient,
        events: Option<broadcast::Sender<TransferEvent>>,
    ) -> Self {
        Self {
            config,
            client,
            events,
        }
    }
}

impl StorageBackend for S3StorageBackend {
    type Upload = S3Upload;

    fn config(&self) -> &Config {
        &self.config
    }

    fn cache(&self) -> Option<&Cache<DefaultCacheStorage>> {
        self.client.cache()
    }

    fn events(&self) -> &Option<broadcast::Sender<TransferEvent>> {
        &self.events
    }

    fn block_size(&self, file_size: u64) -> Result<u64> {
        /// The number of blocks to increment by in search of a block size
        const BLOCK_COUNT_INCREMENT: u64 = 50;

        // Return the block size if one was specified
        if let Some(size) = self.config.block_size() {
            if size > MAX_PART_SIZE {
                return Err(S3Error::InvalidBlockSize.into());
            }

            return Ok(size);
        }

        // Try to balance the number of blocks with the size of the blocks
        let mut num_blocks: u64 = BLOCK_COUNT_INCREMENT;
        while num_blocks < MAX_PARTS {
            let block_size = file_size.div_ceil(num_blocks).next_power_of_two();
            if block_size <= BLOCK_SIZE_THRESHOLD {
                return Ok(block_size.max(MIN_PART_SIZE));
            }

            num_blocks += BLOCK_COUNT_INCREMENT;
        }

        // Couldn't fit the number of blocks within the size threshold; fallback
        // to whatever will fit
        let block_size: u64 = file_size.div_ceil(MAX_PARTS);
        if block_size > MAX_PART_SIZE {
            return Err(S3Error::MaximumSizeExceeded.into());
        }

        Ok(block_size)
    }

    fn is_supported_url(config: &Config, url: &Url) -> bool {
        match url.scheme() {
            "s3" => true,
            "http" | "https" => S3Url::parse(config.s3(), url).is_some_and(|url| {
                // There must be at least one path segment for an object
                !url.path.is_empty()
            }),
            _ => false,
        }
    }

    fn rewrite_url<'a>(config: &Config, url: &'a Url) -> Result<Cow<'a, Url>> {
        match url.scheme() {
            "s3" => {
                let s3 = config.s3();
                let region = s3.region();
                let bucket = url.host_str().ok_or(S3Error::InvalidScheme)?;
                let path = url.path();

                if url.path() == "/" {
                    return Err(S3Error::InvalidScheme.into());
                }

                let (scheme, host, path_style) = if let Some(endpoint) = s3.endpoint() {
                    let scheme = endpoint.scheme();
                    let host = endpoint
                        .host_str()
                        .filter(|_| matches!(scheme, "http" | "https"));
                    let Some(host) = host else {
                        return Err(S3Error::InvalidEndpoint.into());
                    };

                    let host = match endpoint.port() {
                        Some(port) => format!("{host}:{port}"),
                        None => host.to_string(),
                    };

                    // A host that is an IP address cannot have a bucket subdomain
                    let path_style =
                        s3.path_style() || !matches!(endpoint.host(), Some(Host::Domain(_)));
                    (scheme, host, path_style)
                } else if let Some(port) = url.port() {
                    // If the S3 URL has a port, assume localhost
                    (
                        "http",
                        format!("s3.{region}.localhost:{port}"),
                        s3.path_style(),
                    )
                } else {
                    (
                        "https",
                        format!("s3.{region}.{AWS_ROOT_DOMAIN}"),
                        s3.path_style(),
                    )
                };

                let mut url_string = if path_style {
                    format!("{scheme}://{host}/{bucket}{path}")
                } else {
                    format!("{scheme}://{bucket}.{host}{path}")
                };

                if let Some(query) = url.query() {
                    url_string.push('?');
                    url_string.push_str(query);
                }

                if let Some(fragment) = url.fragment() {
                    url_string.push('#');
                    url_string.push_str(fragment);
                }

                url_string
                    .parse()
                    .map(Cow::Owned)
                    .map_err(|_| S3Error::InvalidScheme.into())
            }
            _ => Ok(Cow::Borrowed(url)),
        }
    }

    fn join_url<'a>(&self, mut url: Url, segments: impl Iterator<Item = &'a str>) -> Result<Url> {
        // Append on the segments
        {
            let mut existing = url.path_segments_mut().expect("url should have path");
            existing.pop_if_empty();
            existing.extend(segments);
        }

        Ok(url)
    }

    async fn head(&self, url: Url, must_exist: bool) -> Result<Response> {
        debug_assert!(
            Self::is_supported_url(&self.config, &url),
            "{url} is not a supported S3 URL",
            url = url.as_str()
        );

        debug!("sending HEAD request for `{url}`", url = url.display());

        let date = Utc::now();
        let mut request = self
            .client
            .head(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(AWS_DATE_HEADER, date.format("%Y%m%dT%H%M%SZ").to_string())
            .header(AWS_CONTENT_SHA256_HEADER, sha256_hex_string([]))
            .build()?;

        insert_authentication_header(self.config.s3(), date, &mut request)?;

        let response = self.client.execute(request).await?;
        if !response.status().is_success() {
            // If the resource isn't required to exist and it's a 404, return
            // the response.
            if !must_exist && response.status() == StatusCode::NOT_FOUND {
                return Ok(response);
            }

            return Err(response.into_error().await);
        }

        Ok(response)
    }

    async fn get(&self, url: Url) -> Result<Response> {
        debug_assert!(
            Self::is_supported_url(&self.config, &url),
            "{url} is not a supported S3 URL",
            url = url.as_str()
        );

        debug!("sending GET request for `{url}`", url = url.display());

        let date = Utc::now();
        let mut request = self
            .client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(AWS_DATE_HEADER, date.format("%Y%m%dT%H%M%SZ").to_string())
            .header(AWS_CONTENT_SHA256_HEADER, sha256_hex_string([]))
            .build()?;

        insert_authentication_header(self.config.s3(), date, &mut request)?;

        let response = self.client.execute(request).await?;
        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        Ok(response)
    }

    async fn get_range(
        &self,
        url: Url,
        etag: &str,
        start: u64,
        exclusive_end: Option<u64>,
    ) -> Result<Response> {
        debug_assert!(
            Self::is_supported_url(&self.config, &url),
            "{url} is not a supported S3 URL",
            url = url.as_str()
        );

        let range = format_range_header(start, exclusive_end);

        debug!(
            "sending GET request with range `{range}` for `{url}`",
            url = url.display(),
        );

        let date = Utc::now();

        let mut request = self
            .client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(AWS_DATE_HEADER, date.format("%Y%m%dT%H%M%SZ").to_string())
            .header(AWS_CONTENT_SHA256_HEADER, sha256_hex_string([]))
            .header(header::RANGE, range)
            .header(header::IF_MATCH, etag)
            .build()?;

        insert_authentication_header(self.config.s3(), date, &mut request)?;

        let response = self.client.execute(request).await?;
        let status = response.status();

        // Handle precondition failed as remote content modified
        if status == StatusCode::PRECONDITION_FAILED {
            return Err(Error::RemoteContentModified);
        }

        // Handle error response
        if !status.is_success() {
            return Err(response.into_error().await);
        }

        // We expect partial content, otherwise treat as remote content modified
        if status != StatusCode::PARTIAL_CONTENT {
            return Err(Error::RemoteContentModified);
        }

        Ok(response)
    }

    async fn walk(&self, mut url: Url, first_only: bool) -> Result<Vec<String>> {
        // See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html

        debug_assert!(
            Self::is_supported_url(&self.config, &url),
            "{url} is not a supported S3 URL",
            url = url.as_str()
        );

        debug!("walking `{url}` as a directory", url = url.display());

        let (bucket, mut prefix, path_style, custom_endpoint) = {
            let url =
                S3Url::parse(self.config.s3(), &url).expect("URL should be a supported S3 URL");
            (
                url.bucket.to_string(),
                url.path.strip_prefix('/').unwrap_or(url.path).to_string(),
                url.path_style,
                url.custom_endpoint,
            )
        };

        // The prefix should end with `/` to signify a directory.
        if !prefix.ends_with('/') {
            prefix.push('/');
        }

        if !path_style {
            url.set_path("/");
        } else if custom_endpoint {
            // The endpoint may not support virtual host style URLs (i.e. no
            // wildcard DNS), so keep the bucket in the path
            url.set_path(&format!("/{bucket}"));
        } else {
            // Format the request to use the virtual host style URL
            let domain = url.domain().expect("URL should have domain");
            url.set_host(Some(&format!("{bucket}.{domain}")))
                .map_err(|_| S3Error::InvalidBucketName)?;
            url.set_path("/");
        }

        {
            let mut pairs = url.query_pairs_mut();
            // Use version 2.0 of the API
            pairs.append_pair("list-type", "2");
            // Only return objects with this prefix
            pairs.append_pair("prefix", &prefix);

            // Return at most one key if we're only retrieving the first entry
            if first_only {
                pairs.append_pair("max-keys", "1");
            }
        }

        let date = Utc::now();
        let mut token = String::new();
        let mut paths = Vec::new();
        loop {
            let mut url = url.clone();
            if !token.is_empty() {
                url.query_pairs_mut()
                    .append_pair("continuation-token", &token);
            }

            // List the objects with the prefix
            let mut request = self
                .client
                .get(url)
                .header(header::USER_AGENT, USER_AGENT)
                .header(AWS_DATE_HEADER, date.format("%Y%m%dT%H%M%SZ").to_string())
                .header(AWS_CONTENT_SHA256_HEADER, sha256_hex_string([]))
                .build()?;

            insert_authentication_header(self.config.s3(), date, &mut request)?;

            let response = self.client.execute(request).await?;

            let status = response.status();
            if !status.is_success() {
                return Err(response.into_error().await);
            }

            let text = response.text().await?;
            let results: ListBucketResult = match serde_xml_rs::from_str(&text) {
                Ok(response) => response,
                Err(e) => {
                    return Err(S3Error::UnexpectedResponse { status, error: e }.into());
                }
            };

            // If there is only one result and the result is an empty path, then
            // the given URL was to a file and not a "directory"
            if paths.is_empty()
                && results.contents.len() == 1
                && results.token.is_none()
                && let Some("") = results.contents[0].key.strip_prefix(&prefix)
            {
                return Ok(paths);
            }

            paths.extend(results.contents.into_iter().map(|c| {
                let key = c.key.strip_prefix(&prefix).unwrap_or(&c.key);
                key.strip_prefix('/').unwrap_or(key).into()
            }));

            token = results.token.unwrap_or_default();
            if first_only || token.is_empty() {
                break;
            }
        }

        Ok(paths)
    }

    async fn new_upload(&self, url: Url, digest: Option<String>) -> Result<Self::Upload> {
        // See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html

        debug_assert!(
            Self::is_supported_url(&self.config, &url),
            "{url} is not a supported S3 URL",
            url = url.as_str()
        );

        debug!("sending POST request for `{url}`", url = url.display());

        let mut create = url.clone();
        create.query_pairs_mut().append_key_only("uploads");

        let date = Utc::now();
        let mut request = self
            .client
            .post(create)
            .header(header::USER_AGENT, USER_AGENT)
            // Servers that are stricter than AWS require the header even though
            // the request has no body
            .header(header::CONTENT_LENGTH, 0)
            .header(AWS_DATE_HEADER, date.format("%Y%m%dT%H%M%SZ").to_string())
            .header(AWS_CONTENT_SHA256_HEADER, sha256_hex_string([]))
            .build()?;

        if let Some(digest) = digest {
            request.headers_mut().insert(
                AWS_CONTENT_DIGEST_HEADER,
                digest
                    .try_into()
                    .expect("invalid content digest header value"),
            );
        }

        insert_authentication_header(self.config.s3(), date, &mut request)?;

        let response = self.client.execute(request).await?;
        let status = response.status();
        if !status.is_success() {
            return Err(response.into_error().await);
        }

        let text: String = match response.text().await {
            Ok(text) => text,
            Err(e) => return Err(e.into()),
        };

        let id = match serde_xml_rs::from_str::<InitiateMultipartUploadResult>(&text) {
            Ok(response) => response.upload_id,
            Err(e) => {
                return Err(S3Error::UnexpectedResponse { status, error: e }.into());
            }
        };

        Ok(S3Upload {
            config: self.config.clone(),
            client: self.client.clone(),
            url,
            id,
            events: self.events.clone(),
        })
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;

    use super::*;

    fn config(endpoint: &str) -> Config {
        Config::builder()
            .with_s3(S3Config::default().with_endpoint(endpoint.parse().unwrap()))
            .build()
    }

    fn is_supported(config: &Config, url: &str) -> bool {
        S3StorageBackend::is_supported_url(config, &url.parse().unwrap())
    }

    #[test]
    fn supports_aws_urls() {
        let config = Config::default();

        assert!(is_supported(&config, "s3://bucket/key"));
        assert!(is_supported(
            &config,
            "https://bucket.s3.us-east-1.amazonaws.com/key"
        ));
        assert!(is_supported(
            &config,
            "https://s3.us-east-1.amazonaws.com/bucket/key"
        ));
        assert!(is_supported(
            &config,
            "http://bucket.s3.us-east-1.localhost:4566/key"
        ));
        assert!(is_supported(
            &config,
            "http://s3.us-east-1.localhost:4566/bucket/key"
        ));

        // A path-style URL needs an object
        assert!(!is_supported(
            &config,
            "https://s3.us-east-1.amazonaws.com/bucket"
        ));

        // Not S3 without a configured endpoint
        assert!(!is_supported(&config, "http://minio.local:9000/bucket/key"));
        assert!(!is_supported(&config, "http://127.0.0.1:9000/bucket/key"));
    }

    #[test]
    fn parses_bucket_names_starting_with_s3() {
        let config = Config::default();

        let url: Url = "https://s3-data.s3.us-east-1.amazonaws.com/key"
            .parse()
            .unwrap();
        let parsed = S3Url::parse(config.s3(), &url).unwrap();
        assert_eq!(parsed.bucket, "s3-data");
        assert_eq!(parsed.region, "us-east-1");
        assert!(!parsed.path_style);
    }

    #[test]
    fn parses_endpoint_path_style_urls() {
        let config = config("http://minio.local:9000");

        let url: Url = "http://minio.local:9000/bucket/dir/key".parse().unwrap();
        let parsed = S3Url::parse(config.s3(), &url).unwrap();
        assert_eq!(parsed.region, "us-east-1");
        assert_eq!(parsed.bucket, "bucket");
        assert_eq!(parsed.path, "/dir/key");
        assert!(parsed.path_style);
        assert!(parsed.custom_endpoint);

        assert!(is_supported(&config, "http://minio.local:9000/bucket/key"));

        // A path-style URL needs an object
        assert!(!is_supported(&config, "http://minio.local:9000/bucket"));
        assert!(!is_supported(&config, "http://minio.local:9000/"));

        // The scheme and port must match the endpoint
        assert!(!is_supported(
            &config,
            "https://minio.local:9000/bucket/key"
        ));
        assert!(!is_supported(&config, "http://minio.local:9001/bucket/key"));
        assert!(!is_supported(&config, "http://minio.local/bucket/key"));
        assert!(!is_supported(&config, "http://other.local:9000/bucket/key"));
    }

    #[test]
    fn parses_endpoint_virtual_host_urls() {
        let config = config("http://minio.local:9000");

        let url: Url = "http://my.bucket.minio.local:9000/dir/key".parse().unwrap();
        let parsed = S3Url::parse(config.s3(), &url).unwrap();
        assert_eq!(parsed.region, "us-east-1");
        assert_eq!(parsed.bucket, "my.bucket");
        assert_eq!(parsed.path, "/dir/key");
        assert!(!parsed.path_style);
        assert!(parsed.custom_endpoint);

        assert!(!is_supported(&config, "http://bucket.other.local:9000/key"));
        assert!(!is_supported(&config, "http://bucket.minio.local:9001/key"));
    }

    #[test]
    fn endpoint_uses_configured_region() {
        let config = Config::builder()
            .with_s3(
                S3Config::default()
                    .with_region("my-region")
                    .with_endpoint("http://minio.local:9000".parse().unwrap()),
            )
            .build();

        let url: Url = "http://minio.local:9000/bucket/key".parse().unwrap();
        assert_eq!(S3Url::parse(config.s3(), &url).unwrap().region, "my-region");

        // AWS URLs still use the region from the URL
        let url: Url = "https://bucket.s3.us-west-2.amazonaws.com/key"
            .parse()
            .unwrap();
        let parsed = S3Url::parse(config.s3(), &url).unwrap();
        assert_eq!(parsed.region, "us-west-2");
        assert!(!parsed.custom_endpoint);
    }

    #[test]
    fn endpoint_with_ip_address_host() {
        let config = config("http://127.0.0.1:9000");
        assert!(is_supported(&config, "http://127.0.0.1:9000/bucket/key"));
        assert!(!is_supported(&config, "http://127.0.0.2:9000/bucket/key"));
        assert!(!is_supported(&config, "http://other.local:9000/bucket/key"));

        let config = self::config("http://[::1]:9000");
        assert!(is_supported(&config, "http://[::1]:9000/bucket/key"));
    }

    #[tokio::test]
    async fn new_upload_sends_content_length() {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        const RESPONSE_BODY: &str = "<InitiateMultipartUploadResult><UploadId>id\
                                     </UploadId></InitiateMultipartUploadResult>";

        // Accept a single connection and capture the headers of the request
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();

            let mut request = Vec::new();
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let mut buffer = [0; 1024];
                let read = stream.read(&mut buffer).await.unwrap();
                assert_ne!(read, 0, "connection closed before the request ended");
                request.extend_from_slice(&buffer[..read]);
            }

            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {len}\r\nconnection: \
                 close\r\n\r\n{RESPONSE_BODY}",
                len = RESPONSE_BODY.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap().to_ascii_lowercase()
        });

        let config = config(&format!("http://127.0.0.1:{port}"));
        let backend = S3StorageBackend::new(config, HttpClient::default(), None);
        backend
            .new_upload(
                format!("http://127.0.0.1:{port}/bucket/key")
                    .parse()
                    .unwrap(),
                None,
            )
            .await
            .unwrap();

        // Strict servers respond with a 411 if the request has no content length
        let request = server.await.unwrap();
        assert!(
            request.starts_with("post /bucket/key?uploads "),
            "{request}"
        );
        assert!(request.contains("\r\ncontent-length: 0\r\n"), "{request}");
    }

    #[test]
    fn endpoint_without_port() {
        let config = config("https://s3.example.com");
        assert!(is_supported(&config, "https://s3.example.com/bucket/key"));
        assert!(is_supported(
            &config,
            "https://s3.example.com:443/bucket/key"
        ));
        assert!(is_supported(&config, "https://bucket.s3.example.com/key"));
        assert!(!is_supported(&config, "http://s3.example.com/bucket/key"));
    }
}
