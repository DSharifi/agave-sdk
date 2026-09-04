use {
    crate::{Event, EventHandle, backend},
    std::path::Path,
    thiserror::Error,
};

/// Owns an event-system directory and creates typed event streams within it.
#[derive(Clone)]
pub struct EventSystem {
    backend: backend::EventSystem,
}

impl EventSystem {
    /// Creates an event system at `event_system_directory`.
    ///
    /// The directory must not already exist and relative paths are
    /// resolved when this method is called.
    pub fn create(
        event_system_directory: impl AsRef<Path>,
    ) -> Result<Self, CreateEventSystemError> {
        Ok(Self {
            backend: backend::EventSystem::create(event_system_directory)?,
        })
    }

    /// Creates a stream named `event_stream_name` for event type `E`.
    pub fn create_event_handle<E: Event>(
        &self,
        event_stream_name: &str,
        event_stream_config: EventStreamConfig,
    ) -> Result<EventHandle<E>, CreateEventHandleError> {
        self.backend
            .create_event_handle::<E>(event_stream_name, event_stream_config)
            .map(EventHandle::new)
    }
}

impl std::fmt::Debug for EventSystem {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.backend.fmt(formatter)
    }
}

/// Capacity and participant limits for an event stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventStreamConfig {
    /// Number of events retained in each producer queue.
    pub capacity: usize,
    /// Maximum number of concurrent producers.
    pub producer_slots: usize,
    /// Maximum number of concurrent consumers.
    pub consumer_slots: usize,
}

#[derive(Debug, Error)]
#[error("failed to create the event-system directory")]
pub struct CreateEventSystemError(#[from] std::io::Error);

/// An error reported by the platform event-queue implementation.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct EventQueueError(#[source] pub(crate) Box<dyn std::error::Error + Send + Sync + 'static>);

#[derive(Debug, Error)]
pub enum CreateEventHandleError {
    #[error("event stream name `{0}` is invalid")]
    InvalidEventStreamName(String),
    #[error("failed to serialize the event-stream schema")]
    FailedToSerializeSchema(#[source] wincode::WriteError),
    #[error("failed to create the event-stream files")]
    FileSystem(#[from] std::io::Error),
    #[error("failed to create the event-stream queue")]
    Queue(#[source] EventQueueError),
}
