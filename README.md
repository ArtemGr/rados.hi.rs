rados_hi - Opinionated high-level wrapper for librados (Ceph).

[![crate](https://img.shields.io/crates/v/rados_hi.svg)](https://crates.io/crates/rados_hi)
[![docs](https://docs.rs/rados_hi/badge.svg)](https://docs.rs/rados_hi/)

RADOS is a High Availability (with configurable replication and/or erasure coding) object store that is both a backbone of the Ceph distributed filesystem and a powerful distributed database of its own.

rados_hi is an experimental high-level wrapper around the low-level (so far) [ceph-rust](https://github.com/ceph/ceph-rust) bindings. It achieves good parallelization and composability by lifting RADOS AIO operations as [futures](https://github.com/alexcrichton/futures-rs).

```
```
[![big brother](https://ga-beacon.appspot.com/UA-83241762-2/README)](https://github.com/igrigorik/ga-beacon)
