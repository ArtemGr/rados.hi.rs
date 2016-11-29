rados_hi - Opinionated high-level wrapper for librados (Ceph).

[![crate](https://img.shields.io/crates/v/rados_hi.svg)](https://crates.io/crates/rados_hi)
[![docs](https://docs.rs/rados_hi/badge.svg)](https://docs.rs/rados_hi/)

RADOS is a High Availability (with configurable replication and/or erasure coding) object store that is both a backbone of the Ceph distributed filesystem and a powerful distributed database of its own.

rados_hi is an experimental high-level wrapper around the low-level (so far) [ceph-rust](https://github.com/ceph/ceph-rust) bindings. It achieves good parallelization and composability by lifting RADOS AIO operations as [futures](https://github.com/alexcrichton/futures-rs).

NB. This library diverges a bit from [futures](https://github.com/alexcrichton/futures-rs) "lazy evaluation" approach by starting
the AIO operations early, as soon as the `Future` is created.
You still need to "drive" the follow-up operations in a futures chain.
That is, in a `read and_then process and_then write` chain the `read` AIO operation will be scheduled with Ceph immediately but the follow-up
`process` and `write` futures will only be run once the chain is `poll`ed somehow.
This might change in the future, particularly as the explicit parallelism in `join_all` and similar adapters is better explored.

```
```
[![big brother](https://ga-beacon.appspot.com/UA-83241762-2/README)](https://github.com/igrigorik/ga-beacon)
