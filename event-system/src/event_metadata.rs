#[cfg(target_os = "linux")]
use nix::unistd::{getpid, gettid};

/// Returns a timestamp in nanoseconds from Linux's `CLOCK_MONOTONIC`.
///
/// Event producers can include this value in their event payloads. Timestamps
/// are comparable across processes sharing the same clock (including its time
/// namespace) during the same boot. They are not Unix timestamps and do not
/// include time spent suspended.
///
/// Returns zero on non-Linux targets, where the event system is a no-op.
///
/// # Panics
///
/// Panics if the clock cannot be read or the timestamp does not fit in a `u64`.
#[inline]
pub fn monotonic_timestamp_ns() -> u64 {
    #[cfg(target_os = "linux")]
    {
        use {
            nix::time::{ClockId, clock_gettime},
            std::time::Duration,
        };

        let timestamp =
            clock_gettime(ClockId::CLOCK_MONOTONIC).expect("CLOCK_MONOTONIC must be available");
        Duration::from(timestamp)
            .as_nanos()
            .try_into()
            .expect("monotonic timestamp must fit in u64 nanoseconds")
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// Returns the caller's thread ID (see
/// [gettid(2)](https://man7.org/linux/man-pages/man2/gettid.2.html)
/// and pid of this process (see
/// [getpid(2)](https://pubs.opengroup.org/onlinepubs/9699919799/functions/getpid.html)).
///
/// The ids are cached so subsequent lookups on the same thread
/// perform no sys calls.
///
/// Returns zero for both ids on non-Linux targets, where the event system is a no-op.
///
/// #### Forking
/// The ids are cached per thread. That means calling this method after a `fork()`
/// will return the parent's value.
#[inline]
pub fn cached_thread_and_process_id() -> (i32, i32) {
    #[cfg(target_os = "linux")]
    thread_local! {
        static THREAD_AND_PROCESS_ID: (i32, i32) = (gettid().as_raw(), getpid().as_raw());
    }

    #[cfg(target_os = "linux")]
    {
        THREAD_AND_PROCESS_ID.with(|thread_and_process_id| *thread_and_process_id)
    }
    #[cfg(not(target_os = "linux"))]
    {
        (0, 0)
    }
}
