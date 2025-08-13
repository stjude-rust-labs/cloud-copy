# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

* Added testing with emulators Azurite and localstack ([#4](https://github.com/stjude-rust-labs/cloud-copy/pull/4)).
* Added initial implementation of `cloud-copy` ([#1](https://github.com/stjude-rust-labs/cloud-copy/pull/1)).

#### Changed

* Changed the default parallelism to 4 times the available parallelism ([#2](https://github.com/stjude-rust-labs/cloud-copy/pull/2)).

#### Fixed

* Fixed a panic when running `cloud-copy` ([#4](https://github.com/stjude-rust-labs/cloud-copy/pull/4)).
* Fixed remote content modification check with ranged downloads to use the
  `if-match` header ([#3](https://github.com/stjude-rust-labs/cloud-copy/pull/3)).
* Fixed graceful cancellation (i.e. SIGINT) to cancel operations without
  retrying and return a non-zero exit ([#3](https://github.com/stjude-rust-labs/cloud-copy/pull/3)).