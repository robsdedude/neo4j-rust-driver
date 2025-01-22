# Contributing

## Used Tools
### Pre-Commit
https://pre-commit.com/
Automatically runs some checks before committing, so you don't have to wait for the CI to complain.
```bash
pip install pre-commit
pre-commit install
```

To skip the checks, use `git commit --no-verify`.


### Cargo Public API
https://github.com/enselic/cargo-public-api
If you intentionally change the public API, you need to run:
```bash
UPDATE_EXPECT=1 cargo +stable test --workspace --features _internal_public_api public_api
```
**IMPORTANT:** make sure to run this with the latest stable toolchain have all the latest blanket impls included. 


### Cargo Check External Types
https://github.com/awslabs/cargo-check-external-types
```bash
cargo +nightly-2024-06-30 install --locked cargo-check-external-types@0.1.13
```
