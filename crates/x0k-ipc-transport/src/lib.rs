//! Local service IPC, with optional daemon discovery compatibility.
pub mod local;
#[cfg(feature = "legacy")]
mod legacy;
#[cfg(feature = "legacy")]
pub use legacy::*;
