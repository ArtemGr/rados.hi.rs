// http://docs.ceph.com/docs/jewel/rados/api/librados/; http://docs.ceph.com/docs/jewel/rados/api/librados-intro/
extern crate ceph_rust;  // /usr/include/rados/librados.h
extern crate futures;
#[macro_use] extern crate gstuff;
extern crate libc;

use ceph_rust::rados;
use futures::{Async, Future, Poll};
use futures::task::Task;
use std::error::Error;
use std::ffi::CString;
use std::io::{self};
use std::mem::{transmute, swap};
use std::path::Path;
use std::ptr::{null, null_mut};
use std::sync::{Arc, Mutex};

/// A handle for interacting with a RADOS cluster.
///
/// It encapsulates all RADOS client configuration, including username, key for authentication, logging, and debugging.
/// Talking different clusters -- or to the same cluster with different users -- requires different cluster handles.
pub struct Rados (pub rados::rados_t);
impl Rados {
  pub fn connect (config: &AsRef<Path>, user: Option<&str>) -> Result<Arc<Rados>, String> {
    let mut cluster: rados::rados_t = null_mut();
    let user: Option<Result<CString, _>> = user.map (|id| CString::new (id));
    let user = match user {None => null(), Some (id) => try_s! (id).as_ptr()};
    let rc = unsafe {rados::rados_create (&mut cluster, user)};
    if rc != 0 {return ERR! ("Rados::connect] !rados_create: {}", rc)}
    let config_path: &Path = config.as_ref();
    let config_path_str: &str = match config_path.to_str() {Some (p) => p, None => return ERR! ("!str: {:?}", config_path)};
    let config_path_c = try_s! (CString::new (config_path_str));
    let rc = unsafe {rados::rados_conf_read_file (cluster, config_path_c.as_ptr())};
    if rc != 0 {return ERR! ("Rados::connect] !rados_conf_read_file ({:?}): {}", config_path, rc)}
    let rc = unsafe {rados::rados_connect (cluster)};
    if rc != 0 {return ERR! ("Rados::connect] !rados_connect: {}", rc)}
    Ok (Arc::new (Rados (cluster)))}
  /// Create a pool with default settings.
  ///
  /// The default owner is the admin user (auid 0). The default crush rule is rule 0.
  pub fn pool_create (&self, pool_name: &str) -> Result<(), String> {
    let pool_name = try_s! (CString::new (pool_name));
    let rc = unsafe {rados::rados_pool_create (self.0, pool_name.as_ptr())};
    if rc != 0 {return ERR! ("!rados_pool_create: {}", rc)}
    Ok(())}
  /// Delete a pool and all data inside it.
  ///
  /// The pool is removed from the cluster immediately, but the actual data is deleted in the background.
  pub fn pool_delete (&self, pool_name: &str) -> Result<(), String> {
    let pool_name = try_s! (CString::new (pool_name));
    let rc = unsafe {rados::rados_pool_delete (self.0, pool_name.as_ptr())};
    if rc != 0 {return ERR! ("!rados_pool_delete: {}", rc)}
    Ok(())}}
impl Drop for Rados {
  fn drop (&mut self) {
    unsafe {rados::rados_shutdown (self.0)}}}

/// RAII lock.
pub struct RadosLock {ctx: RadosCtx, oid: CString, name: CString, cookie: CString, unlocked: bool}
impl RadosLock {
  fn _unlock (&mut self) -> Result<(), String> {
    if self.unlocked {return Ok(())}
    let rc = unsafe {rados::rados_unlock (self.ctx.0.ctx, self.oid.as_ptr(), self.name.as_ptr(), self.cookie.as_ptr())};
    // "-ENOENT if the lock is not held by the specified (client, cookie) pair".
    if rc == 0 {self.unlocked = true; Ok(())} else {ERR! ("!rados_unlock: {}", rc)}}
  /// Using this method should prevent the compiler from dropping the lock earlier.
  pub fn unlock (mut self) -> Result<(), String> {self._unlock()}}
impl Drop for RadosLock {
  fn drop (&mut self) {if let Err (err) = self._unlock() {panic! ("RadosLock, drop] {}", err)}}}

#[allow(dead_code)]
struct RadosCtxImpl {  // The pImpl idiom.
  rad: Arc<Rados>,
  ctx: rados::rados_ioctx_t}
impl Drop for RadosCtxImpl {
  fn drop (&mut self) {
    unsafe {rados::rados_ioctx_destroy (self.ctx)}}}

/// An io context encapsulates a few settings for all I/O operations done on it.
///
/// * `pool` - set when the io context is created (see rados_ioctx_create()).
/// * snapshot context for writes (see `rados_ioctx_selfmanaged_snap_set_write_ctx`())
/// * snapshot id to read from (see `rados_ioctx_snap_set_read()`)
/// * object locator for all single-object operations (see `rados_ioctx_locator_set_key()`)
/// * namespace for all single-object operations (see `rados_ioctx_set_namespace()`).
///   Set to `LIBRADOS_ALL_NSPACES` before `rados_nobjects_list_open()` will list all objects in all namespaces.
///
/// Changing any of these settings is not thread-safe - librados users must synchronize any of these changes on their own,
/// or use separate io contexts for each thread.
#[derive (Clone)]
pub struct RadosCtx (Arc<RadosCtxImpl>);
unsafe impl Send for RadosCtx {}
unsafe impl Sync for RadosCtx {}
impl RadosCtx {
  /// Create an IO context.
  ///
  /// The IO context allows you to perform operations within a particular pool.
  ///
  /// IO context creation isn't cheap, especially on a slow network.
  /// We put it in an `Arc` in order to encourage reuse.
  pub fn new (rad: &Arc<Rados>, pool_name: &str) -> Result<RadosCtx, String> {
    let mut io: rados::rados_ioctx_t = null_mut();
    let pool_name = try_s! (CString::new (pool_name));
    let rc = unsafe {rados::rados_ioctx_create (rad.0, pool_name.as_ptr(), &mut io)};
    if rc != 0 {return ERR! ("RadosCtx::new] !rados_ioctx_create: {}", rc)}
    Ok (RadosCtx (Arc::new (RadosCtxImpl {rad: rad.clone(), ctx: io})))}

  pub fn write_bl (&self, oid: &str, bytes: &[u8]) -> Result<(), String> {
    let oid = try_s! (CString::new (oid));
    let rc = unsafe {rados::rados_write (self.0.ctx, oid.as_ptr(), bytes.as_ptr() as *const i8, bytes.len() as libc::size_t, 0)};
    if rc != 0 {return ERR! ("!rados_write: {}", rc)}
    Ok(())}

  /// Asychronously write an entire object.
  /// The object is filled with the provided data.
  /// If the object exists, it is atomically truncated and then written. Queues the write_full and returns.
  pub fn write_full (&self, oid: &str, bytes: &[u8]) -> Result<RadosWriteCompletion, String> {
    RadosWriteCompletion::write_full (self, oid, bytes)}

  pub fn write_full_bl (&self, oid: &str, bytes: &[u8]) -> Result<(), String> {
    let oid = try_s! (CString::new (oid));
    let rc = unsafe {rados::rados_write_full (self.0.ctx, oid.as_ptr(), bytes.as_ptr() as *const i8, bytes.len() as libc::size_t)};
    if rc != 0 {return ERR! ("!rados_write_full: {}", rc)}
    Ok(())}

  /// Asychronously read data from an object.
  ///
  /// The IO context determines the snapshot to read from, if any was set by rados_ioctx_snap_set_read().
  ///
  /// * `oid` - The name of the object to read from.
  /// * `len` - The number of bytes to read.
  /// * `off` - The offset to start reading from in the object.
  pub fn read (&self, oid: &str, len: usize, off: u64) -> Result<RadosReadCompletion, String> {
    RadosReadCompletion::read (self, oid, len, off)}

  /// Take an exclusive lock on an object.
  ///
  /// * `oid` - The name of the object.
  /// * `name` - The name of the lock.
  /// * `cookie` - User-defined identifier for this instance of the lock.
  /// * `desc` - User-defined lock description.
  /// * `dur_sec` - The duration of the lock. Set to 0 for infinite duration.
  /// * `flags` - Lock flags.
  ///
  /// Returns 0 on success, negative error code on failure:
  /// `-EBUSY` if the lock is already held by another (client, cookie) pair.
  /// `-EEXIST` if the lock is already held by the same (client, cookie) pair.
  pub fn lock_exclusive_bl (&self, oid: &str, name: &str, cookie: &str, desc: &str, dur_sec: u32, flags: u8) -> Result<RadosLock, io::Error> {
    let oid = try! (CString::new (oid));
    let name = try! (CString::new (name));
    let cookie = try! (CString::new (cookie));
    let desc = try! (CString::new (desc));
    let mut duration = libc::timeval {tv_sec: dur_sec as i64, tv_usec: 0};
    let rc = unsafe {rados::rados_lock_exclusive (self.0.ctx,
      oid.as_ptr(),
      name.as_ptr(),
      cookie.as_ptr(),
      desc.as_ptr(),
      if dur_sec == 0 {null_mut()} else {&mut duration},
      flags)};
    if rc == 0 {
      Ok (RadosLock {ctx: self.clone(), oid: oid, name: name, cookie: cookie, unlocked: false})
    } else {
      Err (io::Error::from_raw_os_error (-rc))}}}

/// Structure passed to the completion callback. Allocated on heap in order not to dangle around.
struct RadosWriteDugout {task: Mutex<Option<Task>>}

extern "C" fn rs_rados_write_complete (_pc: rados::rados_completion_t, dugout: *mut libc::c_void) {
  let dugout: &mut RadosWriteDugout = unsafe {transmute (dugout)};
  let lock = match dugout.task.lock() {
    Ok (lock) => lock,
    Err (err) => panic! ("rs_rados_write_complete] !lock: {}", err)};
  if let Some (ref task) = *lock {task.unpark()}}

pub struct RadosWriteCompletion {
  pub ctx: RadosCtx,
  pub pc: rados::rados_completion_t,
  dugout: Box<RadosWriteDugout>}
unsafe impl Send for RadosWriteCompletion {}
unsafe impl Sync for RadosWriteCompletion {}
impl RadosWriteCompletion {
  /// Asychronously write an entire object.
  /// The object is filled with the provided data.
  /// If the object exists, it is atomically truncated and then written. Queues the write_full and returns.
  pub fn write_full (ctx: &RadosCtx, oid: &str, bytes: &[u8]) -> Result<RadosWriteCompletion, String> {
    let mut pc: rados::rados_completion_t = null_mut();

    let dugout = Box::new (RadosWriteDugout {task: Mutex::new (None)});

    // Constructs a completion to use with asynchronous operations.
    //
    // The complete and safe callbacks correspond to operations being acked and committed, respectively.
    // The callbacks are called in order of receipt, so the safe callback may be triggered before the complete callback, and vice versa.
    // This is affected by journalling on the OSDs.
    //
    // Read operations only get a complete callback.
    //
    // cb_arg - application-defined data passed to the callback functions
    // cb_complete - the function to be called when the operation is in memory on all relpicas
    // cb_safe - the function to be called when the operation is on stable storage on all replicas
    // pc - where to store the completion
    let rc = unsafe {rados::rados_aio_create_completion (
      transmute (dugout.as_ref()),
      Some (rs_rados_write_complete),
      None,
      &mut pc)};
    if rc != 0 {return ERR! ("!rados_aio_create_completion: {}", rc)}

    let oid = try_s! (CString::new (oid));

    // Asychronously write an entire object.
    // The object is filled with the provided data.
    // If the object exists, it is atomically truncated and then written. Queues the write_full and returns.
    // 0 on success, -EROFS if the io context specifies a snap_seq other than LIBRADOS_SNAP_HEAD.
    let rc = unsafe {rados::rados_aio_write_full (ctx.0.ctx, oid.as_ptr() as *const i8, pc, bytes.as_ptr() as *const i8, bytes.len())};
    if rc != 0 {
      unsafe {rados::rados_aio_release (pc)};
      return ERR! ("!rados_aio_write_full: {}", rc)}

    Ok (RadosWriteCompletion {
      ctx: ctx.clone(),
      pc: pc,
      dugout: dugout})}

  /// Block until an operation completes.
  /// This means it is in memory on all replicas.
  pub fn wait_for_complete (&self) {
    unsafe {rados::rados_aio_wait_for_complete_and_cb (self.pc);}}

  /// Block until an operation is safe.
  /// This means it is on stable storage on all replicas.
  pub fn wait_for_safe (&self) {
    unsafe {rados::rados_aio_wait_for_safe_and_cb (self.pc)};}}
impl Drop for RadosWriteCompletion {
  fn drop (&mut self) {
    if self.pc != null_mut() {
      { let _lock = match self.dugout.task.lock() {
          Ok (lock) => lock,
          Err (err) => panic! ("RadosWriteCompletion::drop] !lock: {}", err)};

        // NB: We *must* either wait for the callback or prevent it from being fired, because we lended a `task` pointer to the callback.
        // Dropping before the callback fires or is cancelled will leave it with a dangling pointer.
        //
        // And by design the futures can be cancelled, cf. "https://aturon.github.io/blog/2016/09/07/futures-design/#cancellation".
        unsafe {rados::rados_aio_cancel (self.ctx.0.ctx, self.pc)}; }

      // Waiting for the [write] to complete seems safer,
      // but might be counterintuitive and misleading as it differs from the futures cancellable design.
      // NB: `rados_shutdown` hangs (!) if, relying on `rados_aio_cancel`, we skip this call.
      unsafe {rados::rados_aio_wait_for_complete_and_cb (self.pc);}

      let pc = self.pc;
      self.pc = null_mut();
      // Release a completion.
      // Call this when you no longer need the completion. It may not be freed immediately if the operation is not acked and committed.
      unsafe {rados::rados_aio_release (pc)};}}}
impl Future for RadosWriteCompletion {
  type Item = ();
  /// Rados errors returned with `std::io::Error::from_raw_os_error`.
  ///
  /// Use `is_not_found` to check for `ENOENT`.
  type Error = Box<Error>;
  fn poll (&mut self) -> Poll<(), Box<Error>> {
    // So to implement the `Future::poll` we should:
    // 1) If Ceph hasn't called back yet, use `futures::task::park()` to obtain a `Task`, then return `futures::Async::NotReady`.
    // 2) When Ceph calls back, if we've been `poll`ed and thus obtained a `Task` in (1), then invoke `Task::unpark()`.
    // Literature:
    // https://docs.rs/futures/0.1/futures/trait.Future.html#tymethod.poll
    // https://aturon.github.io/blog/2016/09/07/futures-design/
    // https://docs.rs/futures/0.1/futures/task/fn.park.html

    let mut lock = try_f! (self.dugout.task.lock());  // The lock should prevent the callback from coming earlier and failing to unpark us.
    *lock = Some (futures::task::park());  // Going to ping this task when the AIO operation completes.

    // Has an asynchronous operation completed?
    // This does not imply that the complete callback has finished.
    let complete = unsafe {rados::rados_aio_is_complete (self.pc)};
    if complete != 0 {
      let rc = unsafe {rados::rados_aio_get_return_value (self.pc)};
      if rc < 0 {return Err (Box::new (std::io::Error::from_raw_os_error (-rc)))}
      return Ok (Async::Ready (()))}

    Ok (Async::NotReady)}}

/// Structure passed to the completion callback. Allocated on heap in order not to dangle around.
struct RadosReadDugout {task: Mutex<Option<Task>>, buf: Vec<u8>}

extern "C" fn rs_rados_read_complete (_pc: rados::rados_completion_t, dugout: *mut libc::c_void) {
  let dugout: &mut RadosReadDugout = unsafe {transmute (dugout)};
  let lock = dugout.task.lock().expect ("rs_rados_read_complete] !lock");
  if let Some (ref task) = *lock {task.unpark()}}

pub struct RadosReadCompletion {
  pub ctx: RadosCtx,
  pub pc: rados::rados_completion_t,
  dugout: Box<RadosReadDugout>}
unsafe impl Send for RadosReadCompletion {}
unsafe impl Sync for RadosReadCompletion {}
impl RadosReadCompletion {
  /// Asychronously read data from an object.
  ///
  /// The IO context determines the snapshot to read from, if any was set by rados_ioctx_snap_set_read().
  ///
  /// * `oid` - The name of the object to read from.
  /// * `len` - The number of bytes to read.
  /// * `off` - The offset to start reading from in the object.
  pub fn read (ctx: &RadosCtx, oid: &str, len: usize, off: u64) -> Result<RadosReadCompletion, String> {
    let mut pc: rados::rados_completion_t = null_mut();
    let mut dugout = Box::new (RadosReadDugout {task: Mutex::new (None), buf: Vec::new()});
    let rc = unsafe {rados::rados_aio_create_completion (
      transmute (dugout.as_ref()),
      Some (rs_rados_read_complete),
      None,
      &mut pc)};
    if rc != 0 {return ERR! ("!rados_aio_create_completion: {}", rc)}

    let oid = try_s! (CString::new (oid));
    dugout.buf.reserve_exact (len);
    unsafe {dugout.buf.set_len (len)};
    let rc = unsafe {rados::rados_aio_read (ctx.0.ctx, oid.as_ptr() as *const i8, pc, dugout.buf.as_ptr() as *mut i8, len, off)};
    if rc != 0 {
      unsafe {rados::rados_aio_release (pc)};
      return ERR! ("!rados_aio_read: {}", rc)}

    Ok (RadosReadCompletion {
      ctx: ctx.clone(),
      pc: pc,
      dugout: dugout})}}
impl Drop for RadosReadCompletion {
  fn drop (&mut self) {
    if self.pc != null_mut() {
      // NB: We *must* either wait for the callback or prevent it from being fired, because we lended a `task` pointer to the callback.
      // Dropping before the callback fires or is cancelled will leave it with a dangling pointer.
      unsafe {rados::rados_aio_cancel (self.ctx.0.ctx, self.pc)};

      let pc = self.pc;
      self.pc = null_mut();
      unsafe {rados::rados_aio_release (pc)};}}}
impl Future for RadosReadCompletion {
  type Item = Vec<u8>;
  /// Rados errors returned with `std::io::Error::from_raw_os_error`.
  ///
  /// Use `is_not_found` to check for `ENOENT`.
  type Error = Box<Error>;
  fn poll (&mut self) -> Poll<Vec<u8>, Box<Error>> {
    { let mut lock = try_f! (self.dugout.task.lock());  // The lock should prevent the callback from coming earlier and failing to unpark us.
      *lock = Some (futures::task::park());  // Going to ping this task when the AIO operation completes.
      let complete = unsafe {rados::rados_aio_is_complete (self.pc)};
      if complete == 0 {return Ok (Async::NotReady)} }

    let rc = unsafe {rados::rados_aio_get_return_value (self.pc)};
    if rc < 0 {return Err (Box::new (std::io::Error::from_raw_os_error (-rc)))}

    let mut buf = Vec::new();
    swap (&mut buf, &mut self.dugout.buf);
    buf.resize (rc as usize, 0);

    Ok (Async::Ready (buf))}}

/// See if we have the ENOENT boxed there.
pub fn is_not_found (err: &Box<Error>) -> bool {
  if let Some (io) = err.downcast_ref::<std::io::Error>() {
    if io.kind() == std::io::ErrorKind::NotFound {
      return true}}
  false}
