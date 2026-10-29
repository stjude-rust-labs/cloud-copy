//! Implementation of AWS Signature Version 4 authentication.
//!
//! This authentication scheme is also used by Google Cloud Storage via their
//! AWS compatibility layer.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Deref;

use chrono::DateTime;
use chrono::Utc;
use hmac::KeyInit;
use hmac::Mac;
use reqwest::Request;
use reqwest::header;
use sha2::Digest;
use sha2::Sha256;

/// The HMAC type used in authentication;
type Hmac = hmac::Hmac<Sha256>;

/// Helper for displaying canonical header names present in the request.
struct CanonicalHeaderNames<'a>(&'a Request, &'a [&'a str]);

impl fmt::Display for CanonicalHeaderNames<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for name in self.1 {
            // The host header is always present
            if *name == header::HOST.as_str() || self.0.headers().contains_key(*name) {
                if !first {
                    write!(f, ";")?;
                }

                first = false;
                write!(f, "{name}")?;
            }
        }

        Ok(())
    }
}

/// Helper for displaying canonical header values present in the request.
struct CanonicalHeaderValues<'a>(&'a Request, &'a [&'a str]);

impl fmt::Display for CanonicalHeaderValues<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for name in self.1 {
            if let Some(value) = self.0.headers().get(*name) {
                if !first {
                    writeln!(f)?;
                }

                write!(
                    f,
                    "{name}:{value}",
                    value = value.to_str().expect("header value should be a string")
                )?;
            } else if *name == header::HOST.as_str() {
                if !first {
                    writeln!(f)?;
                }

                let host = self.0.url().host_str().expect("should have host");

                if let Some(port) = self.0.url().port() {
                    write!(f, "host:{host}:{port}")?;
                } else {
                    write!(f, "host:{host}")?;
                }
            }

            first = false;
        }

        Ok(())
    }
}

/// Represents a provider of signature information for request signing.
pub trait SignatureProvider {
    /// Gets the name of the signature algorithm.
    fn algorithm(&self) -> &str;

    /// Gets the prefix for the secret key.
    fn secret_key_prefix(&self) -> &str;

    /// Gets the request type.
    fn request_type(&self) -> &str;

    /// Gets the name of the region for the request.
    fn region(&self) -> &str;

    /// Gets the name of the service for the request.
    fn service(&self) -> &str;

    /// Gets the canonical header names in alphabetical order.
    fn canonical_header_names(&self) -> &[&str];

    /// Get the content hash header name.
    fn content_hash_header_name(&self) -> &str;

    /// Gets the access key ID to sign with.
    fn access_key_id(&self) -> &str;

    /// Gets the secret access key to sign with.
    fn secret_access_key(&self) -> &str;
}

/// Implements a request signer.
///
/// The signer implements the AWS Signature Version 4 algorithm.
pub struct RequestSigner<P>(P);

impl<P> RequestSigner<P>
where
    P: SignatureProvider,
{
    /// Constructs a new signer with the given provider.
    pub fn new(provider: P) -> Self {
        Self(provider)
    }

    /// Signs the given request.
    ///
    /// Returns the signature that can be used as the value of an authentication
    /// header.
    ///
    /// Returns `None` if the provider's signing key wasn't valid.
    pub fn sign(&self, date: DateTime<Utc>, request: &Request) -> Option<String> {
        let scope = format!(
            "{date}/{region}/{service}/{request_type}",
            date = date.format("%Y%m%d"),
            region = self.0.region(),
            service = self.0.service(),
            request_type = self.0.request_type()
        );

        let canonical_request = self.create_canonical_request(request);
        let string_to_sign = self.create_string_to_sign(date, &scope, &canonical_request);
        let signing_key = self.derive_signing_key(date)?;

        let mut hmac = Hmac::new_from_slice(&signing_key).ok()?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());

        Some(format!(
            "{algorithm} \
             Credential={access_key_id}/{scope},SignedHeaders={canonical_header_names},\
             Signature={signature}",
            algorithm = self.0.algorithm(),
            access_key_id = self.0.access_key_id(),
            canonical_header_names = CanonicalHeaderNames(request, self.0.canonical_header_names()),
        ))
    }

    /// Creates a canonical request string used in authentication.
    ///
    /// # Panics
    ///
    /// Panics if the given request does not have the expected
    /// authentication-related headers.
    ///
    /// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    fn create_canonical_request(&self, request: &Request) -> String {
        let url = request.url();

        // Sort the query pairs by the encoded key
        let mut parameters: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for (k, v) in url.query_pairs() {
            parameters
                .entry(urlencoding::encode(&k).into_owned())
                .or_default()
                .push(urlencoding::encode(&v).into_owned());
        }

        let mut query_string = String::new();
        for (key, values) in parameters {
            for value in values {
                if !query_string.is_empty() {
                    query_string.push('&');
                }

                query_string.push_str(&key);
                query_string.push('=');
                query_string.push_str(&value);
            }
        }

        // Get the content hash header
        let content_hash = request
            .headers()
            .get(self.0.content_hash_header_name())
            .expect("request missing content hash header");

        // Format the canonical request
        format!(
            "\
{method}
{path}
{query_string}
{canonical_header_values}

{canonical_header_names}
{content_hash}",
            method = request.method(),
            path = url.path(),
            canonical_header_values =
                CanonicalHeaderValues(request, self.0.canonical_header_names()),
            canonical_header_names = CanonicalHeaderNames(request, self.0.canonical_header_names()),
            content_hash = content_hash
                .to_str()
                .expect("content hash should be a string")
                .trim(),
        )
    }

    /// Creates a string to sign for authentication.
    ///
    /// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    fn create_string_to_sign(
        &self,
        date: DateTime<Utc>,
        scope: &str,
        canonical_request: &str,
    ) -> String {
        let mut hash = Sha256::new();
        hash.update(canonical_request);
        let hash = hash.finalize();

        format!(
            "{algorithm}\n{date}\n{scope}\n{hash}",
            algorithm = self.0.algorithm(),
            date = date.format("%Y%m%dT%H%M%SZ"),
            hash = hex::encode(hash)
        )
    }

    /// Derives a signing key used in authentication.
    ///
    /// Returns `None` if the given secret access key is invalid.
    ///
    /// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    fn derive_signing_key<'a>(
        &self,
        date: DateTime<Utc>,
    ) -> Option<impl Deref<Target = [u8]> + use<'a, P>> {
        let mut hmac = Hmac::new_from_slice(
            format!(
                "{prefix}{secret_access_key}",
                prefix = self.0.secret_key_prefix(),
                secret_access_key = self.0.secret_access_key()
            )
            .as_bytes(),
        )
        .ok()?;
        hmac.update(format!("{date}", date = date.format("%Y%m%d")).as_bytes());
        let date_key = hmac.finalize().into_bytes();

        let mut hmac = Hmac::new_from_slice(&date_key).ok()?;
        hmac.update(self.0.region().as_bytes());
        let date_region_key = hmac.finalize().into_bytes();

        let mut hmac = Hmac::new_from_slice(&date_region_key).ok()?;
        hmac.update(self.0.service().as_bytes());
        let date_region_service_key = hmac.finalize().into_bytes();

        let mut hmac = Hmac::new_from_slice(&date_region_service_key).ok()?;
        hmac.update(self.0.request_type().as_bytes());
        Some(hmac.finalize().into_bytes())
    }
}
