#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod stub;

#[cfg(target_os = "linux")]
pub(crate) use linux::{EventHandle, EventQueueError, EventSystem};
#[cfg(not(target_os = "linux"))]
pub(crate) use stub::{EventHandle, EventQueueError, EventSystem};
