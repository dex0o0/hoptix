# Hoptix

Hoptix is a simple downloader written in Rust. This project is currently a demo and under active development. The main goal is to provide a lightweight, extensible foundation for file download operations.

## Project status

- Status: Demo — actively under development.
- Note: APIs and behaviors may change in future releases (breaking changes may occur).

## Features (current and planned)

- Current: Basic implementation for downloading a file from a URL.
- Planned / Future:
  - Download queue management
  - Resume support for interrupted downloads
  - Rate limiting
  - Concurrent downloads (multiple connections)
  - Progress display and improved error reporting

## Requirements

- Rust toolchain (rustc and cargo)
- Network access for downloading files

## Local installation and usage

1. Clone the repository:
   git clone https://github.com/dex0o0/hoptix.git
2. Enter the project directory:
   cd hoptix
3. Build in release mode:
   cargo build --release
4. Run an example (arguments may vary depending on implementation):
   cargo run --release -- https://example.com/file.zip -o file.zip

Note: Adjust command-line parameters according to the project's CLI implementation.

## Development and tests

- Run tests:
  cargo test
- Format code:
  cargo fmt
- Lint:
  cargo clippy

Please make sure tests pass and run rustfmt and clippy before submitting changes.

## Collaboration

If you want to contribute to the project, follow these suggested steps:

1. Open an Issue describing the bug or feature request if one does not already exist.
2. Fork the repository.
3. Create a new branch for your work, for example:
   feat/short-description or fix/bug-description
4. Commit changes with clear, descriptive messages.
5. Before opening a Pull Request:
   - Ensure tests pass (cargo test)
   - Format code with cargo fmt and run clippy
6. Open a Pull Request from your branch and include in the description:
   - The related issue (if any)
   - A brief summary of the changes and the rationale
7. For major changes or architectural decisions, open an Issue or discussion first to get alignment before coding.

Recommended coding guidelines:

- Follow Rust coding conventions.
- Document public functions and modules using doc comments (///).
- Cover new functionality with unit tests.

## Reporting bugs and requesting features

To report a bug or request a feature, open a new Issue and include reproduction steps, error output (if any), and your Rust toolchain version.

## Project structure (overview)

- src/: source code
- Cargo.toml: package configuration and dependencies
- tests/: integration and unit tests (if present)

(Structure can be updated with more detail based on the repository contents.)

## License

Repository metadata shows the project license as MIT.

## Contact

Use Issues and Pull Requests for questions or collaboration.
