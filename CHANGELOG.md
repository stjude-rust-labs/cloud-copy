# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

* Added `get_content_digest` function for retrieving content digests ([#27](https://github.com/stjude-rust-labs/cloud-copy/pull/27)).
* Added content digest metadata for uploads, defaulting to SHA-256 ([#27](https://github.com/stjude-rust-labs/cloud-copy/pull/27)).
* Added `--hash-algorithm` to `cloud-copy` CLI for specifying the algorithm to
  use for attaching content digest metadata to uploaded objects ([#27](https://github.com/stjude-rust-labs/cloud-copy/pull/27)).
* Added support for Azure Shared Key authentication ([#26](https://github.com/stjude-rust-labs/cloud-copy/pull/26)).

#### Changed

* `Config` now implements a builder pattern for setting configuration options ([#26](https://github.com/stjude-rust-labs/cloud-copy/pull/26)).

## 0.4.0 - 10-13-2025

#### Added

* Implemented a `walk` function that can be used to glob a cloud storage URL ([#24](https://github.com/stjude-rust-labs/cloud-copy/pull/24)).

#### Fixed

* Fixed Google Cloud Storage uploads not working ([#24](https://github.com/stjude-rust-labs/cloud-copy/pull/24)).
* Improved progress bar message ([#23](https://github.com/stjude-rust-labs/cloud-copy/pull/23)).

## 0.3.0 - 09-15-2025

#### Added

* Display an indeterminate progress bar on event stream lag ([#19](https://github.com/stjude-rust-labs/cloud-copy/pull/19)).

#### Fixed

* Ensure required auth options for CLI ([#18](https://github.com/stjude-rust-labs/cloud-copy/pull/18)).
* Only log message for an upload once the upload has been created ([#17](https://github.com/stjude-rust-labs/cloud-copy/pull/17)).

## 0.2.1 - 09-11-2025

#### Fixed

* Fixed progress showing for remote resource that already exists ([#15](https://github.com/stjude-rust-labs/cloud-copy/pull/15)).
* Fixed progress bar not being removed for failed transfers ([#14](https://github.com/stjude-rust-labs/cloud-copy/pull/14)).

## 0.2.0 - 09-10-2025

#### Added

* Implemented support for Windows ([#12](https://github.com/stjude-rust-labs/cloud-copy/pull/12)).
* Respect the `--overwrite` option for remote destinations ([#11](https://github.com/stjude-rust-labs/cloud-copy/pull/11)).
* Implemented an `--overwrite` option in the CLI ([#10](https://github.com/stjude-rust-labs/cloud-copy/pull/10)).
* Added a `rewrite_url` for changing cloud storage schemed URLs into HTTP URLs ([#9](https://github.com/stjude-rust-labs/cloud-copy/pull/9)).

#### Fixed

* Made upload progress of small files more consistent ([#11](https://github.com/stjude-rust-labs/cloud-copy/pull/11)).
* Fixed empty file uploads for Azure Storage ([#11](https://github.com/stjude-rust-labs/cloud-copy/pull/11)).

## 0.1.0 - 08-19-2025

#### Added

* Added `HttpClient` for allowing reuse of an HTTP client between copy
  operations ([#7](https://github.com/stjude-rust-labs/cloud-copy/pull/7)).
* Added support for linking files to the cache ([#6](https://github.com/stjude-rust-labs/cloud-copy/pull/6)).
* Added statistics to the CLI upon successful copy ([#5](https://github.com/stjude-rust-labs/cloud-copy/pull/5)).
* Added `--cache-dir` CLI option for download caching ([#5](https://github.com/stjude-rust-labs/cloud-copy/pull/5)).
* Added testing with emulators Azurite and localstack ([#4](https://github.com/stjude-rust-labs/cloud-copy/pull/4)).
* Added initial implementation of `cloud-copy` ([#1](https://github.com/stjude-rust-labs/cloud-copy/pull/1)).

#### Changed

* Reverted the default parallelism back to the system's available parallelism ([#5](https://github.com/stjude-rust-labs/cloud-copy/pull/5)).
* User agent now includes link to GitHub repository ([#5](https://github.com/stjude-rust-labs/cloud-copy/pull/5)).
* Changed the default parallelism to 4 times the available parallelism ([#2](https://github.com/stjude-rust-labs/cloud-copy/pull/2)).

#### Fixed

* Fixed incorrect transfer progress events ([#5](https://github.com/stjude-rust-labs/cloud-copy/pull/5)).
* Fixed an incorrect `range` header causing an extra byte in the response ([#5](https://github.com/stjude-rust-labs/cloud-copy/pull/5)).
* Fixed a panic when running `cloud-copy` ([#4](https://github.com/stjude-rust-labs/cloud-copy/pull/4)).
* Fixed remote content modification check with ranged downloads to use the
  `if-match` header ([#3](https://github.com/stjude-rust-labs/cloud-copy/pull/3)).
* Fixed graceful cancellation (i.e. SIGINT) to cancel operations without
  retrying and return a non-zero exit ([#3](https://github.com/stjude-rust-labs/cloud-copy/pull/3)).