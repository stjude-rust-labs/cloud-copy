# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

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