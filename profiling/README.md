# Profiling with DTRACE (MacOS / FreeBSD)

## Prerequisites:

- [DTrace](https://dtrace.org)
- [rustfilt](https://crates.io/crates/rustfilt)

```
cargo build -r
sudo profile/profile.d -c "./target/release/surreal" | rustfilt
```