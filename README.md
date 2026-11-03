# rspm

[![GitHub License](https://img.shields.io/github/license/FL03/rspm?style=for-the-badge&logo=github)](LICENSE)

***

_**Warning: The library is currently in the early stages of development and is not yet ready for production use.**_

`rspm` is a Rust native Polymarket client supporting realtime streams, secure trading, and more.

## Features

`rspm` separates pure market primitives from network and SDK clients:

- `alloc,std`: enables SDK-independent types and helpers such as `BookSnapshot`,
  `MarketSnapshot`, `Side`, `ClobSide`, and `OrderType`.
- `sdk`: enables the optional `polymarket_client_sdk_v2` dependency.
- `clob`: enables authenticated HTTP and CLOB clients, their SDK bridges,
  bounded retry handling, and the direct SHA-256 dependency used for credential
  and protocol identities.
- `gamma`: enables the reqwest-based Gamma API without enabling the external SDK.
- `watch`: adds the CLOB WebSocket transport on top of `clob`.
- `serde`: enables serialization using [`serde`](https://serde.rs/).

The default profile enables `clob`, `gamma`, async support, ECDSA, and `std`.
The workspace deliberately contains one canonical production package, `rspm`.
Generated component placeholders are not public architecture and cannot ship;
future crate extraction requires a real dependency graph and an API-compatible
umbrella migration.

`WalletAddress` is a closed value object. It accepts exactly lowercase `0x`
plus 40 ASCII hexadecimal digits, normalizes hexadecimal casing, and applies
that same validation during deserialization.

RSPM is path-only while `publish = false`. From a workspace clone, consumers
that need only deterministic market primitives can use:

```toml
[dependencies.rspm]
default-features = false
features = ["alloc", "std"]
path = "crates/rspm"
version = "0.0.0"
```

A downstream workspace must adjust `path` relative to the `Cargo.toml` that
declares the dependency, for example `../clients/rspm/crates/rspm` when that is
where its checkout lives. The compatible `version` pin remains `0.0.0`.

### Native API and temporary SDK compatibility export

New code should use RSPM's canonical native surface, including
`rspm::{clob, auth, gamma, types, retry, watch, ...}`. These modules define the
RSPM contract and may intentionally differ from the upstream SDK.

For callers migrating away from a direct `polymarket_client_sdk_v2` dependency,
the `sdk` feature temporarily exposes the exact upstream crate as
`rspm::polymarket`. This is a public `extern crate` alias, not a wrapper, glob,
or prelude export. Upstream types and macros therefore retain exact identity:

```rust
use rspm::polymarket::{POLYGON, auth::Credentials};
use rspm::polymarket::types::{Address, address};

let wallet: Address = address!("0x1111111111111111111111111111111111111111");
let credentials = Credentials::default();
let _ = (POLYGON, wallet, credentials);
```

Do not introduce new SDK coupling through `rspm::polymarket`. Existing direct
SDK call sites may mechanically move to this alias as a staged migration while
RSPM-owned replacements are built.

The compatibility feature topology is:

| Features | SDK behavior |
| --- | --- |
| `alloc,std` | Pure market primitives only. The external SDK is not activated or exported. |
| `sdk` | Activates the optional SDK, `std` and its `alloc` closure, plus the exact `rspm::polymarket` crate export. |
| `clob` | Includes `sdk`, enables the upstream SDK's `clob` capability, and exposes RSPM's native `auth`, `clob`, and `retry` modules. |
| `clob,ws` | Adds the upstream CLOB WebSocket types and transport. `ws` alone does not enable RSPM's CLOB client. |
| `watch` | Includes `clob`, streaming, and `ws` for RSPM's native watcher. |
| `gamma` | Uses RSPM's reqwest-based Gamma client and does not activate the SDK. |
| `tracing` | Enables RSPM tracing and weak-forwards SDK tracing only when `sdk` is already active. |

### Authenticated raw-frame boundary

With `clob,ws`, `AuthenticatedUserRawFrame` exposes the exact received payload,
frame and transport sequences, socket generation and gap version, encoding and
schema gap, plus the process generation and receipt clocks. Those are transport
receipt facts. RSPM does not assign Axiom owner/session identity or derive an
application evidence key or payload digest from them. Consumers compose such
policy in their own shared contract layer.

Removing `rspm::polymarket` requires all of the following:

1. A repository-wide census finds no direct SDK dependencies outside RSPM.
2. The same census finds no remaining `rspm::polymarket` uses.
3. Native, RSPM-owned contracts replace every supported SDK-typed public escape.
4. The alias removal is scheduled and documented as a versioned breaking change.

These criteria are separate from removal of the vendored upstream SDK patch.
Patch removal follows `patches/polymarket_client_sdk_v2/AXIOM_PATCH.md`; retiring
that patch does not authorize removing the public alias, or the reverse. Until
the alias criteria are met, exact crate identity, feature gates, macros, and the
tracing forward remain compatibility contracts.

## Usage

Add the path-backed crate to your `Cargo.toml` from a workspace clone:

```toml
[dependencies.rspm]
features = []
path = "crates/rspm"
version = "0.0.0"
```

For a separate checkout, change `path` to the relative location of
`clients/rspm/crates/rspm` from the consuming manifest. RSPM is not a registry
dependency while publication remains disabled.

### Examples

#### _Example #1:_ Basic Usage

```rust
    extern crate rspm;

    fn main() -> anyhow::Result<()> {

        Ok(())
    }

```

## Getting Started

### Prerequisites

Ensure you have the latest version of Rust installed. You can install Rust using [rustup](https://rustup.rs/).

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

After installation, I always recommend ensuring that rustup is updated to the latest version:

```bash
rustup update
```

And to add the latest nightly toolchain, which is often useful for development:

```bash
rustup toolchain install nightly
```

Additionally, you may wish to install the `cargo-binstall` utility to streamline the installation of Rust binaries:

```bash
cargo install cargo-binstall
```

If necessary, add the `wasm32-*` target(s) if you plan to compile for WebAssembly:

```bash
rustup target add wasm32-unknown-unknown wasm32-p1 wasm32-p2
```

### Building from the source

Start by cloning the repository

```bash
git clone https://github.com/FL03/rspm.git -b main --depth 1
```

Then, navigate to the project directory:

```bash
cd rspm
```

Once you're in the project directory, you can build the project using `cargo`:

```bash
cargo build --workspace --release --all-features
```

Or, if you want to run the tests, you can use:

```bash
cargo test --workspace --release --all-features
```

### Upstream SDK patch

The workspace excludes `patches/polymarket_client_sdk_v2` from automatic
workspace membership and selects it exactly once as the direct path-backed
`workspace.dependencies` entry:

```toml
polymarket_client_sdk_v2 = { path = "patches/polymarket_client_sdk_v2", version = "0.7" }
```

This makes standalone clones use the same authenticated submission and
WebSocket lifecycle contracts as downstream consumers without a registry patch
table. See `patches/polymarket_client_sdk_v2/AXIOM_PATCH.md` for provenance and
removal criteria.

## Contributing

### Gates

Formatting, clippy, tests, doctests and the build run locally with
[Dagger](https://docs.dagger.io/install), from the shared `pzzld-org/dagger`
rust module that `dagger.toml` installs. They run over the default features and
`full,test-utils`. The module repository is private, so running them needs read
access to pzzld-org:

```bash
dagger check -j 1
```

`git config core.hooksPath .githooks` makes `.githooks/pre-push` run them before
every push. Pull requests no longer run them on GitHub Actions (pzzld-org/root
`CI.md`).

### Rust test placement

Keep private unit tests literally inline with the implementation in a
`#[cfg(test)] mod tests { ... }` block. Put public, black-box behavior in
`crates/rspm/tests/`. Never create a standalone test module below `src` and do
not use `#[path]` as a test-layout workaround. RSPM intentionally rejects every
source-side external-module `#[path]` override, rather than trying to infer why
it exists. The standalone source-layout contract and Axiom's mutation-tested
Shepherd gate enforce these boundaries.

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.
