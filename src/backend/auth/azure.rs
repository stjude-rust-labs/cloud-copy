//! Implementation of Azure Shared Key authentication.

use std::borrow::Cow;
use std::collections::BTreeMap;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use hmac::Mac;
use http_cache_stream_reqwest::semantics;
use reqwest::Method;
use reqwest::Request;
use reqwest::header;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderName;
use reqwest::header::HeaderValue;
use secrecy::ExposeSecret;
use sha2::Sha256;
use url::form_urlencoded;

use crate::AzureAuthConfig;

/// Determines if the given header is a Microsoft extension header.
fn is_microsoft_header(name: &HeaderName) -> bool {
    name.as_str().starts_with("x-ms-")
}

/// The HMAC type used in authentication;
type Hmac = hmac::Hmac<Sha256>;

/// Implements a request signer.
///
/// The signer implements Azure Shared Key authentication.
///
/// See: https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
pub struct RequestSigner<'a>(&'a AzureAuthConfig);

impl<'a> RequestSigner<'a> {
    pub fn new(config: &'a AzureAuthConfig) -> Self {
        Self(config)
    }

    /// Signs the given request.
    ///
    /// Returns `None` if the access key wasn't valid.
    pub fn sign(&self, request: &Request) -> Option<String> {
        // Calculate the canonical headers of the request
        let canonical_headers = self.canonical_headers(
            request
                .headers()
                .iter()
                .filter(|(k, _)| is_microsoft_header(k)),
        );

        // Calculate the canonical resource of the request
        let canonical_resource =
            self.canonical_resource(request.url().path(), request.url().query_pairs());

        // Calculate the string to sign
        let string_to_sign = self.string_to_sign(
            request.method(),
            &canonical_headers,
            &canonical_resource,
            |name| request.headers().get(name),
        );

        // Sign the string to produce the authorization header
        self.authorization_header(&string_to_sign)
    }

    /// Signs the given revalidation request.
    ///
    /// Returns `None` if the access key wasn't valid.
    pub fn sign_revalidation(
        &self,
        request: &dyn semantics::RequestLike,
        headers: &HeaderMap,
    ) -> Option<String> {
        let uri = request.uri();

        // Calculate the canonical headers of the request
        let canonical_headers = self.canonical_headers(
            request
                .headers()
                .iter()
                .filter(|(k, _)| is_microsoft_header(k)),
        );

        // Calculate the canonical resource of the request
        let canonical_resource = self.canonical_resource(
            uri.path(),
            form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()),
        );

        // Calculate the string to sign
        let string_to_sign = self.string_to_sign(
            request.method(),
            &canonical_headers,
            &canonical_resource,
            |name| headers.get(&name).or_else(|| request.headers().get(name)),
        );

        // Sign the string to produce the authorization header
        self.authorization_header(&string_to_sign)
    }

    /// Formats a string to sign for authentication.
    ///
    /// See: https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-signature-string
    ///
    /// # Panics
    ///
    /// Panics if any of the request headers cannot be represented as a string.
    fn string_to_sign<'b>(
        &self,
        method: &Method,
        canonical_headers: &str,
        canonical_resource: &str,
        headers: impl Fn(HeaderName) -> Option<&'b HeaderValue>,
    ) -> String {
        format!(
            "\
{method}
{content_encoding}
{content_language}
{content_length}
{content_md5}
{content_type}
{date}
{if_modified_since}
{if_match}
{if_none_match}
{if_unmodified_since}
{range}
{canonical_headers}{canonical_resource}",
            content_encoding = headers(header::CONTENT_ENCODING)
                .map(|v| v.to_str().expect("content-encoding should be a string"))
                .unwrap_or(""),
            content_language = headers(header::CONTENT_LANGUAGE)
                .map(|v| v.to_str().expect("content-language should be a string"))
                .unwrap_or(""),
            content_length = headers(header::CONTENT_LENGTH)
                .map(|v| v.to_str().expect("content-length should be a string"))
                .unwrap_or(""),
            content_md5 = headers(HeaderName::from_static("content-md5"))
                .map(|v| v.to_str().expect("content-md5 should be a string"))
                .unwrap_or(""),
            content_type = headers(header::CONTENT_TYPE)
                .map(|v| v.to_str().expect("content-type should be a string"))
                .unwrap_or(""),
            date = headers(header::DATE)
                .map(|v| v.to_str().expect("date should be a string"))
                .unwrap_or(""),
            if_modified_since = headers(header::IF_MODIFIED_SINCE)
                .map(|v| v.to_str().expect("if-modified-since should be a string"))
                .unwrap_or(""),
            if_match = headers(header::IF_MATCH)
                .map(|v| v.to_str().expect("if-match should be a string"))
                .unwrap_or(""),
            if_none_match = headers(header::IF_NONE_MATCH)
                .map(|v| v.to_str().expect("if-none-match should be a string"))
                .unwrap_or(""),
            if_unmodified_since = headers(header::IF_UNMODIFIED_SINCE)
                .map(|v| v.to_str().expect("if-unmodified-since should be a string"))
                .unwrap_or(""),
            range = headers(header::RANGE)
                .map(|v| v.to_str().expect("range should be a string"))
                .unwrap_or(""),
        )
    }

    /// Formats a canonical header string given the Microsoft extension headers.
    ///
    /// See: https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-canonicalized-headers-string
    ///
    /// # Panics
    ///
    /// Panics if any of the header values cannot be represented as a string or
    /// if there is a duplicate header value.
    fn canonical_headers<'b>(
        &self,
        microsoft_headers: impl Iterator<Item = (&'b HeaderName, &'b HeaderValue)>,
    ) -> String {
        let mut headers = BTreeMap::new();
        for (k, v) in microsoft_headers {
            if headers
                .insert(
                    k.as_str(),
                    v.to_str().expect("expected a string value").trim(),
                )
                .is_some()
            {
                panic!("duplicate header `{k}`", k = k.as_str());
            }
        }

        let mut canonical_headers = String::new();
        for (k, v) in headers {
            canonical_headers.push_str(k);
            canonical_headers.push(':');
            canonical_headers.push_str(v);
            canonical_headers.push('\n');
        }

        canonical_headers
    }

    /// Formats a canonical resource string.
    ///
    /// See: https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-canonicalized-resource-string
    ///
    /// # Panics
    ///
    /// Panics if there is a duplicate query parameter.
    fn canonical_resource<'b>(
        &self,
        path: &str,
        query_pairs: impl Iterator<Item = (Cow<'b, str>, Cow<'b, str>)>,
    ) -> String {
        let mut canonical_resource = String::new();

        canonical_resource.push('/');
        canonical_resource.push_str(self.0.account_name());
        canonical_resource.push_str(path);

        let mut parameters = BTreeMap::new();
        for (k, v) in query_pairs {
            if parameters.insert(k.clone(), v).is_some() {
                panic!("duplicate query parameter `{k}`");
            }
        }

        for (k, v) in parameters {
            canonical_resource.push('\n');
            canonical_resource.push_str(&k);
            canonical_resource.push(':');
            canonical_resource.push_str(&v);
        }

        canonical_resource
    }

    /// Signs the given string to format an `Authorization` header.
    ///
    /// See: https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#specifying-the-authorization-header
    ///
    /// Returns the value of the authorization header.
    ///
    /// Returns `None` if the access key wasn't valid.
    fn authorization_header(&self, string_to_sign: &str) -> Option<String> {
        let mut hmac = Hmac::new_from_slice(
            &BASE64_STANDARD
                .decode(self.0.access_key().expose_secret())
                .ok()?,
        )
        .ok()?;

        hmac.update(string_to_sign.as_bytes());

        let signature = BASE64_STANDARD.encode(hmac.finalize().into_bytes());

        Some(format!(
            "SharedKey {account_name}:{signature}",
            account_name = self.0.account_name()
        ))
    }
}
