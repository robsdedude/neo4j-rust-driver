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
UPDATE_EXPECT=1 cargo test --workspace --features _internal_public_api public_api
```
**IMPORTANT:** make sure to run this with the latest stable toolchain have all the latest blanket impls included. 


### Cargo Check External Types
https://github.com/awslabs/cargo-check-external-types
```bash
cargo install --locked cargo-check-external-types
```
