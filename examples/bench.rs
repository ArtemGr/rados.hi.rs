// Run me with "cargo run --release --example bench".
// To create the test pool: "ceph osd pool create test 10 replicated && ceph osd pool set test size 2".
// To remove it: "rados rmpool test test --yes-i-really-really-mean-it".

extern crate futures;
#[macro_use] extern crate gstuff;
extern crate libc;
extern crate rados_hi;
extern crate scoped_pool;
#[macro_use] extern crate timeit;

use futures::Future;
use rados_hi::*;
use scoped_pool::Pool;
use std::env::args;

// NB: Rust basic benchmarking seems useless for testing the multithreaded and AIO workloads, so we do it by hand.
fn ceph_bench (debug: bool) -> Result<(), String> {
  // To clear the test pool manually:
  // rados purge test --yes-i-really-really-mean-it

  let rad = try_s! (Rados::connect (&"/etc/ceph/ceph.conf", None));
  let ctx = try_s! (RadosCtx::new (&rad, "test"));

  let text = "If a man will begin with certainties, he shall end in doubts; \
    but if he will be content to begin with doubts he shall end in certainties. \
    Sir Francis Bacon (1561 - 1626)";

  // Measure Ceph throughput, as opposed to a single op latency, by doing writes in parallel.
  // Ceph is heavily bound by synchronous (journal) writes and durability, but the throughput should be okay.

  { let mut oid_num = 0u32;
    for &(outer_loops, inner_loops) in [(5, 128), (2, 1000)].iter() {
      let sec = timeit_loops! (outer_loops, {
        if debug {println! ("timeit_loops cycle...")}
        let mut futures = Vec::with_capacity (inner_loops);
        for _ in 0..inner_loops {
          oid_num += 1; if oid_num > 256 {oid_num = 1}
          let oid = format! ("write_full_{}", oid_num);
          if debug {println! ("Making a future for {}...", oid)}
          let c = try_s! (ctx.write_full (&oid, text.as_bytes()))
            .map (move |_f| {if debug {println! ("Thread {}, AIO completion for {}.", unsafe {libc::pthread_self()}, oid)}});
          futures.push (c);}
        // NB: The dropped futures don't have to finish their work, they are considered *cancelled*,
        // cf. https://aturon.github.io/blog/2016/09/07/futures-design/#cancellation.
        if debug {println! ("Joining...")}
        for f in futures {try_s! (f.wait());}});
      println! ("{} futures / {:.3} sec = {:.1} completions per second.", inner_loops, sec, inner_loops as f64 / sec);} }

  for &(threads, inner_loops) in [(1, 2), (16, 64), (64, 256)].iter() {
    let pool = Pool::new (threads);
    let mut ops = 0u32;
    let outer_loops = 10;
    let mut oid_num = 0u32;
    if debug {println! ("Test started...")}
    let mut loop_num = 0;
    let sec = timeit_loops! (outer_loops, {
      loop_num += 1;
      pool.scoped (|scope| {
        for _ in 0..inner_loops {
          oid_num += 1; if oid_num > 256 {oid_num = 1}
          let oid = format! ("write_full_{}", oid_num);
          let ctx = ctx.clone();
          scope.execute (move || {
            if debug {println! ("Loop {}, thread {}, writing to {}...", loop_num, unsafe {libc::pthread_self()}, oid)}
            ctx.write_full_bl (&oid, text.as_bytes()) .expect ("!write_full");});
          ops += 1;}})});
    assert_eq! (inner_loops, ops / outer_loops);
    println! ("Threads: {}; ops {} ({} inner / {:.3} sec); write_full per second: {:.1}.",
      threads, ops, inner_loops, sec, inner_loops as f64 / sec);}

  Ok(())}

fn main() {
  ceph_bench (args().any (|arg| arg == "--debug")) .unwrap();}