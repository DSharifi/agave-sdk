#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod stub;

#[cfg(target_os = "linux")]
pub(crate) use linux::{EventHandle, EventSystem};
#[cfg(not(target_os = "linux"))]
pub(crate) use stub::{EventHandle, EventSystem};
