
# Local IPC belongs to its service

```turtle folio:document
implementation:sync\/local-ipc a x0k:Implementation ;
    x0k:status "draft" ;
    x0k:summary "How a client on one machine reaches a service: the endpoint belongs to the service, and the transport is a Unix socket or a Windows named pipe." ;
    x0k:concerns "ipc", "storage", "supervision" ;
    x0k:cites architecture:core-and-edges ;
    x0k:motivatedBy intent:4ffaf896-91ae-4392-93b7-67d61edce838 ;
    folio:tangleCrate "crates/x0k-ipc-transport" ;
    folio:tangleRoot "src/local.rs" .
```

A storage client names the storage endpoint. Neither a daemon environment
override nor a failed connection can redirect it to another service or TCP.
The local module supplies byte streams; the service owns framing, deadlines,
authentication and admission. Its dependency closure excludes serialization,
daemon discovery and workspace-hack when built without default features.

The default legacy feature retains the existing daemon-facing exports and
behavior. Those callers can migrate independently. Their permissive discovery
and socket cleanup are not inherited by the explicit service API.

## One locator, one owner

The locator is an absolute path in a directory private to the current account.
Unix uses a filesystem socket. Windows hashes the canonical locator into a
local named-pipe name. A persistent adjacent lease file fences cooperating
listeners. It is never unlinked while it can fence anyone: only an owner
that holds what it serves exclusively by other means retires the socket and
lease once it stops. Unix recovery removes only a socket which
refuses connections, under that lease; regular files, symlinks and active
listeners are refused. Automatic unlink-on-drop is disabled so an old listener
cannot delete a newer endpoint. Stale Unix sockets are recovered on next bind.

The directory protects name creation on Unix. Windows additionally uses an
explicit account-only pipe DACL and rejects remote clients through interprocess.
Clients check the connected pipe's owner and DACL before sending credentials.
The listener also asks the kernel who connected (`SO_PEERCRED`, or
`getpeereid` on the BSDs and macOS) and drops a peer running as another
account, or one the platform cannot name, before it reads a byte.
The boundary is the operating-system account: mutually hostile processes under
one account require a different security model.

<a name="chunk-local-service-transport"></a><sub>[`src/local.rs`](../../crates/x0k-ipc-transport/src/local.rs) · `#local-service-transport`</sub>

```rust {#local-service-transport}
use std::{fs::{File, OpenOptions}, io, path::{Path, PathBuf}};
use interprocess::local_socket::{
    tokio::{Listener as SocketListener, Stream},
    traits::tokio::{Listener as _, Stream as _},
    ListenerOptions,
};
pub use interprocess::local_socket::tokio::Stream as Connection;

#[cfg(unix)]
#[path = "local_unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "local_windows.rs"]
mod platform;

/// Verify ownership and owner-only access on an already-open file.
pub fn check_private_file(file: &File) -> io::Result<()> {
    platform::check_file(file)
}

/// Set owner-only access on a caller-owned scratch/config path.
/// Call before writing secrets or publishing the endpoint.
pub fn make_private(path: &Path) -> io::Result<()> {
    platform::make_private(path)
}

#[derive(Clone, Debug)]
pub struct Endpoint { locator: PathBuf }

/// The error `source` raised on `path`, carrying the path in its message
/// and keeping its kind.
fn naming(path: &Path, doing: &str, source: io::Error) -> io::Error {
    io::Error::new(source.kind(), format!("{doing} {}: {source}", path.display()))
}

impl Endpoint {
    pub fn new(locator: impl Into<PathBuf>) -> io::Result<Self> {
        let locator = locator.into();
        if !locator.is_absolute() || locator.file_name().is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "absolute IPC locator required"));
        }
        let parent = locator.parent().ok_or_else(|| io::Error::other("IPC parent missing"))?;
        let parent = std::fs::canonicalize(parent)?;
        platform::check_directory(&parent)?;
        Ok(Self { locator: parent.join(locator.file_name().unwrap()) })
    }

    pub async fn connect(&self) -> io::Result<Connection> {
        let stream = Stream::connect(platform::name(&self.locator)?).await?;
        platform::check_connection(&stream)?;
        Ok(stream)
    }

    /// The absolute path this endpoint names; on Windows the pipe's name is
    /// derived from it.
    pub fn locator(&self) -> &Path { &self.locator }

    /// Take the endpoint. A refusal names the locator and keeps the
    /// operating system's error kind.
    pub async fn bind(&self) -> io::Result<Listener> {
        self.bind_unnamed().await.map_err(|e| naming(&self.locator, "cannot bind local socket", e))
    }

    async fn bind_unnamed(&self) -> io::Result<Listener> {
        platform::check_directory(self.locator.parent().unwrap())?;
        let lease_path = self.lease_path();
        if let Ok(meta) = std::fs::symlink_metadata(&lease_path) {
            if !meta.is_file() || meta.file_type().is_symlink() {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, "invalid IPC lease"));
            }
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        platform::lease_options(&mut options);
        let lease = options.open(&lease_path)?;
        platform::check_file(&lease)?;
        platform::check_lease(&lease)?;
        lease.try_lock().map_err(|_| io::Error::new(io::ErrorKind::AddrInUse, "IPC owner already active"))?;
        platform::recover(&self.locator).await?;
        let options = ListenerOptions::new().name(platform::name(&self.locator)?).reclaim_name(false);
        let inner = platform::listener(options)?;
        platform::after_bind(&self.locator)?;
        Ok(Listener { inner, _lease: lease })
    }

    fn lease_path(&self) -> PathBuf {
        let mut lease_path = self.locator.as_os_str().to_os_string();
        lease_path.push(".ipc-owner");
        PathBuf::from(lease_path)
    }

    /// Remove the socket and its lease after the listener has served. Only
    /// for an owner that holds what the endpoint serves exclusively by other
    /// means (a store's writer lease), so no other listener can be fencing
    /// on this lease; anything at either path but a socket and a plain file
    /// is left alone.
    pub fn retire(&self) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            if std::fs::symlink_metadata(&self.locator).is_ok_and(|meta| meta.file_type().is_socket()) {
                let _ = std::fs::remove_file(&self.locator);
            }
        }
        let lease_path = self.lease_path();
        if std::fs::symlink_metadata(&lease_path).is_ok_and(|meta| meta.file_type().is_file()) {
            let _ = std::fs::remove_file(&lease_path);
        }
    }
}

/// Listener drops before its ownership lease. The lease pathname persists.
pub struct Listener { inner: SocketListener, _lease: File }
impl Listener {
    /// The next connection from a peer running as this account. A peer the
    /// platform names as another account is dropped unanswered.
    pub async fn accept(&self) -> io::Result<Connection> {
        loop {
            let stream = self.inner.accept().await?;
            if platform::peer_admitted(&stream) { return Ok(stream); }
        }
    }
}

/// The process at the other end of `stream`, as the kernel names it: its
/// effective uid and, where the platform says, its pid. Never read from
/// the wire. `None` where the platform names no peer.
pub fn peer_of(stream: &Connection) -> Option<(u32, Option<u32>)> { platform::peer_of(stream) }

/// A bounded probe must not hang startup on an unresponsive listener.
#[cfg(unix)]
async fn probe_existing_socket(endpoint: &Path) -> io::Result<Connection> {
    tokio::time::timeout(std::time::Duration::from_millis(250),
        Stream::connect(platform::name(endpoint)?)).await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "IPC ownership probe timed out"))?
}
```

## An address named by what it serves

A service whose state lives in a file, such as a store's database, can be
reached at an address named by that file rather than placed beside it. A
socket placed beside its store inherits the length of the path to it, and a
Unix socket path holds at most 104 bytes on macOS and 108 on Linux: a deep
home directory or a nested temporary directory puts such a socket out of
reach, and the bind fails. The identity address is

    <runtime directory>/<16 hex digits>.<label>.sock

where the digits are the first eight bytes of the BLAKE3 hash of the
identity's canonical path, and the label names the service (`owner`, at most
16 lowercase letters or hyphens). The runtime directory is short and private
to the account: `$XDG_RUNTIME_DIR/x0k-ipc` on Linux when that is set, absolute
and at most 64 bytes long, and `/tmp/x0k-ipc-<uid>` otherwise and always on
macOS, whose `$TMPDIR` is itself long. The directory is the endpoints' alone,
not a general x0k runtime directory other tools may share and widen. Windows places the locator under the account's
temporary directory; its pipe name is a hash of the locator, so its length does
not matter there. The address therefore holds at most
`IDENTITY_ADDRESS_LIMIT` (103) bytes on Unix whatever the identity's path, and
an address that canonicalisation still lengthens past that is refused rather
than bound.

The runtime directory is created owner-only. One that already exists is used
only if it is a real directory (not a symlink) owned by this account with no
group or other access; anything else is refused by name, since another account
could otherwise pre-create it and watch or squat the endpoints in it. The
identity is hashed, not spelled out, so the directory's listing reveals no
paths.

Every refusal on this path, and every bind refusal, names the path it was
about and keeps the operating system's error kind.

<a name="chunk-identity-address"></a><sub>[`src/local.rs`](../../crates/x0k-ipc-transport/src/local.rs) · `#identity-address`</sub>

```rust {#identity-address}
/// Unix runtime directories longer than this are passed over for the fallback.
#[cfg(unix)]
const RUNTIME_DIR_LIMIT: usize = 64;
const LABEL_LIMIT: usize = 16;
/// The longest identity address on Unix: runtime directory, separator,
/// 16 hex digits, `.`, label, `.sock`.
pub const IDENTITY_ADDRESS_LIMIT: usize = 64 + 1 + 16 + 1 + LABEL_LIMIT + 5;

impl Endpoint {
    /// The endpoint serving `identity` (an existing file, typically a
    /// store's database) under the account's runtime directory.
    pub fn for_identity(label: &str, identity: &Path) -> io::Result<Self> {
        Self::for_identity_in(&platform::runtime_dir()?, label, identity)
    }

    /// The endpoint serving `identity` under `runtime`, created owner-only
    /// when absent and refused when it exists with any other owner or access.
    pub fn for_identity_in(runtime: &Path, label: &str, identity: &Path) -> io::Result<Self> {
        if label.is_empty() || label.len() > LABEL_LIMIT
            || !label.bytes().all(|b| b.is_ascii_lowercase() || b == b'-') {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("IPC label {label:?} must be 1 to {LABEL_LIMIT} lowercase letters or hyphens")));
        }
        if !runtime.is_absolute() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("IPC runtime directory {} must be absolute", runtime.display())));
        }
        #[cfg(unix)]
        if runtime.as_os_str().len() > RUNTIME_DIR_LIMIT {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!(
                "IPC runtime directory {} is longer than {RUNTIME_DIR_LIMIT} bytes", runtime.display())));
        }
        platform::ensure_private_directory(runtime)
            .map_err(|e| naming(runtime, "IPC runtime directory", e))?;
        let identity = std::fs::canonicalize(identity)
            .map_err(|e| naming(identity, "IPC identity", e))?;
        let digest = blake3::hash(identity.as_os_str().as_encoded_bytes());
        let digits: String = digest.as_bytes()[..8].iter().map(|b| format!("{b:02x}")).collect();
        let endpoint = Self::new(runtime.join(format!("{digits}.{label}.sock")))?;
        #[cfg(unix)]
        if endpoint.locator.as_os_str().len() > IDENTITY_ADDRESS_LIMIT {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!(
                "IPC address {} is longer than {IDENTITY_ADDRESS_LIMIT} bytes", endpoint.locator.display())));
        }
        Ok(endpoint)
    }
}
```

## Unix protection and recovery

The directory, config and lease must belong to the effective user and grant
no group or other access. The lease is opened without following symlinks and
must have exactly one link. Recovery compares device/inode before removing a
stale socket; any error except connection refusal is a refusal to recover.

<a name="chunk-unix-local-security"></a><sub>[`src/local_unix.rs`](../../crates/x0k-ipc-transport/src/local_unix.rs) · `#unix-local-security`</sub>

```rust {#unix-local-security file="src/local_unix.rs"}
use super::*;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use interprocess::local_socket::{GenericFilePath, Name, ToFsName};

fn check_metadata(meta: &std::fs::Metadata) -> io::Result<()> {
    // SAFETY: geteuid has no preconditions or pointers.
    if meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "IPC path must be owner-only"));
    }
    Ok(())
}
pub(super) fn check_file(file: &File) -> io::Result<()> { check_metadata(&file.metadata()?) }
pub(super) fn check_directory(path: &Path) -> io::Result<()> {
    let meta = std::fs::metadata(path)?;
    if !meta.is_dir() { return Err(io::Error::other("IPC parent is not a directory")); }
    check_metadata(&meta)
}
pub(super) fn make_private(path: &Path) -> io::Result<()> {
    let mode = if path.is_dir() { 0o700 } else { 0o600 };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}
pub(super) fn lease_options(options: &mut OpenOptions) {
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
}
pub(super) fn check_lease(file: &File) -> io::Result<()> {
    let meta = file.metadata()?;
    if !meta.is_file() || meta.nlink() != 1 { return Err(io::Error::other("invalid IPC lease links")); }
    Ok(())
}
pub(super) fn name(path: &Path) -> io::Result<Name<'_>> { path.to_fs_name::<GenericFilePath>() }
pub(super) fn check_connection(_: &Connection) -> io::Result<()> { Ok(()) }
fn effective_uid() -> u32 {
    // SAFETY: geteuid has no preconditions or pointers.
    unsafe { libc::geteuid() }
}
/// A peer is admitted only when the kernel names it as this account.
pub(super) fn peer_admitted(stream: &Connection) -> bool {
    use interprocess::local_socket::traits::StreamCommon;
    same_account(stream.peer_creds().ok().and_then(|creds| creds.euid()), effective_uid())
}
fn same_account(peer: Option<u32>, me: u32) -> bool { peer == Some(me) }
/// The peer's effective uid and pid, from its socket's credentials.
pub(super) fn peer_of(stream: &Connection) -> Option<(u32, Option<u32>)> {
    use interprocess::local_socket::traits::StreamCommon;
    let creds = stream.peer_creds().ok()?;
    Some((creds.euid()?, creds.pid().and_then(|pid| u32::try_from(pid).ok())))
}
pub(super) fn runtime_dir() -> io::Result<PathBuf> {
    Ok(runtime_dir_from(std::env::var_os("XDG_RUNTIME_DIR"), effective_uid()))
}
fn runtime_dir_from(xdg: Option<std::ffi::OsString>, uid: u32) -> PathBuf {
    #[cfg(not(target_vendor = "apple"))]
    if let Some(dir) = xdg.map(PathBuf::from).filter(|dir| dir.is_absolute()).map(|dir| dir.join("x0k-ipc"))
        .filter(|dir| dir.as_os_str().len() <= RUNTIME_DIR_LIMIT) {
        return dir;
    }
    #[cfg(target_vendor = "apple")]
    let _ = xdg;
    PathBuf::from(format!("/tmp/x0k-ipc-{uid}"))
}
pub(super) fn ensure_private_directory(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    match std::fs::DirBuilder::new().mode(0o700).create(path) {
        Err(e) if e.kind() != io::ErrorKind::AlreadyExists => return Err(e),
        _ => {}
    }
    let meta = std::fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "not a directory of this account's own"));
    }
    check_metadata(&meta)
}
pub(super) fn listener(options: ListenerOptions<'_>) -> io::Result<SocketListener> {
    options.create_tokio()
}
pub(super) fn after_bind(path: &Path) -> io::Result<()> { make_private(path) }
pub(super) async fn recover(path: &Path) -> io::Result<()> {
    let before = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    if !before.file_type().is_socket() {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, "IPC endpoint is not a socket"));
    }
    match super::probe_existing_socket(path).await {
        Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {},
        _ => return Err(io::Error::new(io::ErrorKind::AddrInUse, "IPC endpoint may be active")),
    }
    let after = std::fs::symlink_metadata(path)?;
    if before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(io::Error::new(io::ErrorKind::AddrInUse, "IPC endpoint changed"));
    }
    std::fs::remove_file(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_this_account_is_admitted() {
        assert!(same_account(Some(1000), 1000));
        assert!(!same_account(Some(1001), 1000), "another account is refused");
        assert!(!same_account(None, 1000), "a peer the platform cannot name is refused");
    }

    #[cfg(not(target_vendor = "apple"))]
    #[test]
    fn a_short_runtime_dir_is_used_and_a_long_one_falls_back() {
        assert_eq!(runtime_dir_from(Some("/run/user/1000".into()), 1000), PathBuf::from("/run/user/1000/x0k-ipc"));
        let long = format!("/{}", "r".repeat(70));
        assert_eq!(runtime_dir_from(Some(long.into()), 1000), PathBuf::from("/tmp/x0k-ipc-1000"));
        assert_eq!(runtime_dir_from(Some("relative".into()), 7), PathBuf::from("/tmp/x0k-ipc-7"));
        assert_eq!(runtime_dir_from(None, 7), PathBuf::from("/tmp/x0k-ipc-7"));
    }
}
```

## Windows account boundary

The pipe DACL names the process account SID explicitly. Default Windows
DACLs can admit other accounts, so filesystem locators and credential files
are checked through handles. Null DACLs, foreign owners, grants to other
accounts and unfamiliar ACE forms are refused. This deliberately requires
an account-only directory, including its inheritable permissions.

<a name="chunk-windows-local-security"></a><sub>[`src/local_windows.rs`](../../crates/x0k-ipc-transport/src/local_windows.rs) · `#windows-local-security`</sub>

```rust {#windows-local-security file="src/local_windows.rs"}
use super::*;
use std::{ffi::c_void, os::windows::{ffi::OsStrExt, fs::OpenOptionsExt,
    io::{AsHandle, AsRawHandle, FromRawHandle, OwnedHandle}}, ptr};
use interprocess::{
    local_socket::{GenericNamespaced, Name, ToNsName},
    os::windows::{local_socket::ListenerOptionsExt,
        security_descriptor::{SecurityDescriptor, AsSecurityDescriptor}},
};
use windows_sys::Win32::{
    Foundation::{LocalFree, HANDLE},
    Security::{*, Authorization::*},
    Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT},
    System::{Threading::{GetCurrentProcess, OpenProcessToken},
        SystemServices::{ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE}},
};

struct LocalAllocation(*mut c_void);
impl Drop for LocalAllocation {
    fn drop(&mut self) {
        // SAFETY: this wrapper owns an allocation returned by a LocalAlloc-family API.
        unsafe { LocalFree(self.0); }
    }
}

/// Pointer-aligned storage keeps TOKEN_USER and its trailing SID alive together.
struct User(Vec<usize>);
impl User {
    fn current() -> io::Result<Self> {
        let mut raw = ptr::null_mut();
        // SAFETY: valid output pointer, query-only access to the current process token.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut raw) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful OpenProcessToken transfers one owned handle.
        let token = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut length = 0;
        // SAFETY: null buffer with zero length asks Windows for the required size.
        unsafe { GetTokenInformation(token.as_raw_handle(), TokenUser, ptr::null_mut(), 0, &mut length); }
        if length < std::mem::size_of::<TOKEN_USER>() as u32 {
            return Err(io::Error::last_os_error());
        }
        let mut data = vec![0usize; (length as usize).div_ceil(std::mem::size_of::<usize>())];
        // SAFETY: aligned allocation is at least length bytes; token is alive.
        if unsafe { GetTokenInformation(token.as_raw_handle(), TokenUser,
            data.as_mut_ptr().cast(), length, &mut length) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(data))
    }
    fn sid(&self) -> PSID {
        // SAFETY: current() initialized this aligned TOKEN_USER and its trailing SID.
        unsafe { (*(self.0.as_ptr().cast::<TOKEN_USER>())).User.Sid }
    }
    fn text(&self) -> io::Result<String> {
        let mut raw = ptr::null_mut();
        // SAFETY: the SID is alive and raw is a valid output slot.
        if unsafe { ConvertSidToStringSidW(self.sid(), &mut raw) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let _allocation = LocalAllocation(raw.cast());
        // SAFETY: successful conversion returns a nul-terminated UTF-16 string.
        Ok(unsafe { widestring::U16CStr::from_ptr_str(raw) }.to_string_lossy())
    }
}
fn account_security_descriptor() -> io::Result<SecurityDescriptor> {
    let sid = User::current()?.text()?;
    let sddl = widestring::U16CString::from_str(format!("O:{sid}D:P(A;OICI;GA;;;{sid})"))
        .map_err(io::Error::other)?;
    SecurityDescriptor::deserialize(&sddl)
}
fn denied() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "IPC security must grant only the current account")
}
fn check_handle(handle: HANDLE) -> io::Result<()> {
    let user = User::current()?;
    let mut owner = ptr::null_mut();
    let mut acl = ptr::null_mut();
    let mut sd = ptr::null_mut();
    // SAFETY: handle is borrowed for this call, output pointers are valid.
    let status = unsafe { GetSecurityInfo(handle, SE_FILE_OBJECT,
        OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION, &mut owner,
        ptr::null_mut(), &mut acl, ptr::null_mut(), &mut sd) };
    if status != 0 { return Err(io::Error::from_raw_os_error(status as i32)); }
    let _allocation = LocalAllocation(sd);
    // SAFETY: owner and acl point inside the live Windows-produced descriptor.
    if owner.is_null() || acl.is_null() || unsafe { EqualSid(owner, user.sid()) } == 0 {
        return Err(denied());
    }
    // SAFETY: non-null ACL was returned by GetSecurityInfo.
    let count = unsafe { (*acl).AceCount };
    for index in 0..count {
        let mut ace = ptr::null_mut();
        // SAFETY: ACL is valid; GetAce checks index and returns an interior pointer.
        if unsafe { GetAce(acl, index as u32, &mut ace) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: GetAce succeeded, so at least ACE_HEADER is available.
        let kind = unsafe { (*ace.cast::<ACE_HEADER>()).AceType } as u32;
        if kind == ACCESS_DENIED_ACE_TYPE { continue; }
        if kind != ACCESS_ALLOWED_ACE_TYPE { return Err(denied()); }
        // SAFETY: a Windows-validated ACCESS_ALLOWED_ACE has a SID at SidStart.
        let sid = unsafe { ptr::addr_of!((*ace.cast::<ACCESS_ALLOWED_ACE>()).SidStart) }
            .cast_mut().cast();
        // SAFETY: both SIDs remain alive for the call.
        if unsafe { EqualSid(sid, user.sid()) } == 0 { return Err(denied()); }
    }
    Ok(())
}
pub(super) fn check_file(file: &File) -> io::Result<()> { check_handle(file.as_raw_handle()) }
pub(super) fn check_directory(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new().read(true).custom_flags(FILE_FLAG_BACKUP_SEMANTICS
        | FILE_FLAG_OPEN_REPARSE_POINT).open(path)?;
    if !file.metadata()?.is_dir() { return Err(io::Error::other("IPC parent is not a directory")); }
    check_file(&file)
}
pub(super) fn make_private(path: &Path) -> io::Result<()> {
    set_dacl(path, &account_security_descriptor()?)
}
fn set_dacl(path: &Path, descriptor: &SecurityDescriptor) -> io::Result<()> {
    let path = widestring::U16CString::from_os_str(path).map_err(io::Error::other)?;
    let mut present = 0;
    let mut defaulted = 0;
    let mut acl = ptr::null_mut();
    // SAFETY: descriptor remains alive, outputs are valid.
    if unsafe { GetSecurityDescriptorDacl(descriptor.as_sd().cast_mut(), &mut present,
        &mut acl, &mut defaulted) } == 0 { return Err(io::Error::last_os_error()); }
    // SAFETY: path and ACL remain alive. Protected DACL disables broader inherited grants.
    let status = unsafe { SetNamedSecurityInfoW(path.as_ptr(), SE_FILE_OBJECT,
        DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
        ptr::null_mut(), ptr::null_mut(), acl, ptr::null_mut()) };
    if status != 0 { return Err(io::Error::from_raw_os_error(status as i32)); }
    Ok(())
}
pub(super) fn lease_options(options: &mut OpenOptions) {
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}
pub(super) fn check_lease(file: &File) -> io::Result<()> {
    use std::os::windows::fs::MetadataExt;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.file_attributes() & 0x400 != 0 {
        return Err(io::Error::other("invalid IPC lease"));
    }
    Ok(())
}
pub(super) fn name(path: &Path) -> io::Result<Name<'static>> {
    let bytes: Vec<u8> = path.as_os_str().encode_wide().flat_map(u16::to_le_bytes).collect();
    format!("x0k-local-{}", blake3::hash(&bytes)).to_ns_name::<GenericNamespaced>()
}
pub(super) fn check_connection(stream: &Connection) -> io::Result<()> {
    match stream {
        Connection::NamedPipe(pipe) => check_handle(pipe.as_handle().as_raw_handle()),
    }
}
pub(super) fn listener(options: ListenerOptions<'_>) -> io::Result<SocketListener> {
    // interprocess enforces first-instance ownership and rejects remote clients.
    options.security_descriptor(account_security_descriptor()?).create_tokio()
}
pub(super) fn after_bind(_: &Path) -> io::Result<()> { Ok(()) }
pub(super) async fn recover(_: &Path) -> io::Result<()> { Ok(()) }
/// The pipe's DACL already admits only this account, and interprocess
/// rejects remote clients.
pub(super) fn peer_admitted(_: &Connection) -> bool { true }
/// A pipe names no uid.
pub(super) fn peer_of(_: &Connection) -> Option<(u32, Option<u32>)> { None }
pub(super) fn runtime_dir() -> io::Result<PathBuf> { Ok(std::env::temp_dir().join("x0k-runtime")) }
pub(super) fn ensure_private_directory(path: &Path) -> io::Result<()> {
    match std::fs::create_dir(path) {
        Ok(()) => make_private(path)?,
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    check_directory(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn broad_pipe_is_refused_before_credentials_and_cannot_be_replaced() {
        let dir = tempfile::tempdir().unwrap();
        make_private(dir.path()).unwrap();
        let path = dir.path().join("pipe");
        let broad = widestring::U16CString::from_str("D:P(A;;GA;;;WD)").unwrap();
        let _old = ListenerOptions::new().name(name(&path).unwrap())
            .security_descriptor(SecurityDescriptor::deserialize(&broad).unwrap())
            .create_tokio().unwrap();
        let endpoint = Endpoint::new(path).unwrap();
        assert!(matches!(endpoint.connect().await, Err(e) if e.kind() == io::ErrorKind::PermissionDenied));
        assert!(endpoint.bind().await.is_err());
    }

    #[test]
    fn config_grant_to_everyone_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        make_private(dir.path()).unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, b"scratch").unwrap();
        make_private(&path).unwrap();
        check_file(&File::open(&path).unwrap()).unwrap();
        let broad = widestring::U16CString::from_str("D:P(A;;GA;;;WD)").unwrap();
        set_dacl(&path, &SecurityDescriptor::deserialize(&broad).unwrap()).unwrap();
        assert_eq!(check_file(&File::open(&path).unwrap()).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied);
    }
}

```

## Exercise ownership through real streams

The proof uses the same endpoint on both platforms. Bytes are fragmented
deliberately; frame assembly remains the caller's responsibility. A second
owner cannot claim the endpoint, disconnect becomes EOF, and a new owner can
bind after the old listener closes. Unix additionally proves refusal of a
listener which does not participate in the lease protocol, stale recovery,
and preservation of ordinary files and symlinks.

The identity address is proved where the old placement failed: a store under
a 300-byte path inside the temporary directory (however deeply that is nested)
binds and answers, at an address no longer than `IDENTITY_ADDRESS_LIMIT`, the
same for the same store and different for another. A runtime directory open to
others, or a symlink standing in for one, is refused by name; an absent one is
created owner-only; and a bind the operating system refuses comes back naming
the address.

<a name="chunk-local-transport-proof"></a><sub>[`tests/explicit_local.rs`](../../crates/x0k-ipc-transport/tests/explicit_local.rs) · `#local-transport-proof`</sub>

```rust {#local-transport-proof file="tests/explicit_local.rs"}
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use x0k_ipc_transport::local::{Endpoint, IDENTITY_ADDRESS_LIMIT, make_private, check_private_file};

fn fixture() -> (tempfile::TempDir, Endpoint) {
    let dir = tempfile::tempdir().unwrap();
    make_private(dir.path()).unwrap();
    let endpoint = Endpoint::new(dir.path().join("service.sock")).unwrap();
    (dir, endpoint)
}

#[tokio::test]
async fn exclusive_owner_fragmented_stream_disconnect_and_restart() {
    let (_dir, endpoint) = fixture();
    let listener = endpoint.bind().await.unwrap();
    assert!(matches!(endpoint.bind().await, Err(e) if e.kind() == io::ErrorKind::AddrInUse));
    let mut client = endpoint.connect().await.unwrap();
    let mut server = listener.accept().await.unwrap();
    client.write_all(&[0, 0]).await.unwrap();
    client.write_all(&[0, 3, 7, 8, 9]).await.unwrap();
    assert_eq!(server.read_u32().await.unwrap(), 3);
    let mut body = [0; 3];
    server.read_exact(&mut body).await.unwrap();
    assert_eq!(body, [7, 8, 9]);
    drop(client);
    assert_eq!(server.read_u8().await.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    drop(server);
    drop(listener);
    let replacement = endpoint.bind().await.unwrap();
    let _connection = endpoint.connect().await.unwrap();
    let _accepted = replacement.accept().await.unwrap();
}

#[test]
fn explicit_locator_and_private_config() {
    assert!(Endpoint::new("relative.sock").is_err());
    let (dir, _) = fixture();
    let path = dir.path().join("config");
    std::fs::write(&path, b"private").unwrap();
    make_private(&path).unwrap();
    check_private_file(&std::fs::File::open(path).unwrap()).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unleased_live_socket_is_preserved_and_stale_socket_recovers() {
    let (dir, endpoint) = fixture();
    let path = dir.path().join("service.sock");
    let old = tokio::net::UnixListener::bind(&path).unwrap();
    assert!(matches!(endpoint.bind().await, Err(e) if e.kind() == io::ErrorKind::AddrInUse));
    let _client = tokio::net::UnixStream::connect(&path).await.unwrap();
    drop(old);
    let _recovered = endpoint.bind().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unsafe_paths_and_lease_aliases_are_refused() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let (dir, endpoint) = fixture();
    let path = dir.path().join("service.sock");
    std::fs::write(&path, b"not a socket").unwrap();
    assert!(endpoint.bind().await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"not a socket");
    std::fs::remove_file(&path).unwrap();
    symlink(dir.path().join("missing"), &path).unwrap();
    assert!(endpoint.bind().await.is_err());
    assert!(std::fs::symlink_metadata(&path).unwrap().file_type().is_symlink());
    std::fs::remove_file(&path).unwrap();
    let lease = dir.path().join("service.sock.ipc-owner");
    std::fs::hard_link(&lease, dir.path().join("lease-alias")).unwrap();
    assert!(endpoint.bind().await.is_err());
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Endpoint::new(path).is_err());
}

/// A store file under a path of at least `depth` bytes inside `root`.
fn deep_store(root: &std::path::Path, depth: usize) -> std::path::PathBuf {
    let mut dir = root.to_path_buf();
    while dir.as_os_str().len() < depth { dir.push("d".repeat(48)); }
    std::fs::create_dir_all(&dir).unwrap();
    let store = dir.join("store.sqlite");
    std::fs::write(&store, b"").unwrap();
    store
}

#[tokio::test]
async fn a_store_under_a_long_path_binds_at_a_bounded_address() {
    let root = tempfile::tempdir().unwrap();
    let store = deep_store(root.path(), 300);
    let endpoint = Endpoint::for_identity("owner", &store).unwrap();
    assert!(endpoint.locator().as_os_str().len() <= IDENTITY_ADDRESS_LIMIT, "{}", endpoint.locator().display());
    let listener = endpoint.bind().await.unwrap();
    let mut client = endpoint.connect().await.unwrap();
    let mut server = listener.accept().await.unwrap();
    client.write_all(&[42]).await.unwrap();
    assert_eq!(server.read_u8().await.unwrap(), 42, "a peer of this account is admitted");
    assert_eq!(Endpoint::for_identity("owner", &store).unwrap().locator(), endpoint.locator());
    let other = deep_store(root.path(), 400);
    assert_ne!(Endpoint::for_identity("owner", &other).unwrap().locator(), endpoint.locator());
    drop((client, server, listener));
    endpoint.retire();
    assert!(!endpoint.locator().exists(), "a retired endpoint leaves no socket behind");
}

#[cfg(unix)]
fn short_scratch() -> tempfile::TempDir {
    tempfile::Builder::new().prefix("x0k-ipc-").tempdir_in("/tmp").unwrap()
}

#[cfg(unix)]
#[test]
fn a_runtime_dir_open_to_others_is_refused_and_an_absent_one_is_made_private() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let scratch = short_scratch();
    let store = deep_store(scratch.path(), 0);
    let runtime = scratch.path().join("rt");
    Endpoint::for_identity_in(&runtime, "owner", &store).unwrap();
    assert_eq!(std::fs::metadata(&runtime).unwrap().permissions().mode() & 0o777, 0o700);

    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o755)).unwrap();
    let refused = Endpoint::for_identity_in(&runtime, "owner", &store).unwrap_err();
    assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied);
    assert!(refused.to_string().contains(&runtime.display().to_string()), "{refused}");

    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    let alias = scratch.path().join("alias");
    symlink(&runtime, &alias).unwrap();
    let refused = Endpoint::for_identity_in(&alias, "owner", &store).unwrap_err();
    assert!(refused.to_string().contains(&alias.display().to_string()), "{refused}");
    assert!(Endpoint::for_identity_in(&runtime, "Owner!", &store).is_err(), "a label is plain lowercase");
}

#[cfg(unix)]
#[tokio::test]
async fn a_refused_bind_names_its_address() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = short_scratch();
    let store = deep_store(scratch.path(), 0);
    let runtime = scratch.path().join("rt");
    let endpoint = Endpoint::for_identity_in(&runtime, "owner", &store).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o500)).unwrap();
    let refused = endpoint.bind().await.err().expect("a directory without write access refuses the bind");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied);
    assert!(refused.to_string().contains(&endpoint.locator().display().to_string()), "{refused}");
}
```
