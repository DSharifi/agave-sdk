use {
    crate::{Event, event_handle::EventHandle, queue_file},
    shaq::broadcast::{Broadcast, BroadcastConfig},
    std::{
        fs::{OpenOptions, create_dir, rename},
        io::Write,
        path::{Path, PathBuf, absolute},
        sync::Arc,
    },
    thiserror::Error,
};

// Layout of the event-system directory:
//
// event-system-directory/
// ├── tmp/
// │   └── .transaction-events-<timestamp>.tmp/
// │       ├── queue
// │       └── schema
// └── event-streams/
//     ├── shred-events/
//     │   ├── queue
//     │   └── schema
//     └── slot-events/
//         ├── queue
//         └── schema
//
const EVENT_QUEUE_FILE_NAME: &str = "queue";
const EVENT_SCHEMA_FILE_NAME: &str = "schema";
const EVENT_STAGING_DIRECTORY_NAME: &str = "tmp";
const EVENT_STREAMS_DIRECTORY_NAME: &str = "event-streams";

/// Owns an event-system directory and creates typed event streams within it.
#[derive(Debug, Clone)]
pub struct EventSystem {
    event_system_directory: Arc<Path>,
}

impl EventSystem {
    /// Creates an event system at `event_system_directory`.
    ///
    /// The directory must not already exist. Relative paths are resolved when
    /// this method is called.
    pub fn create(
        event_system_directory: impl AsRef<Path>,
    ) -> Result<Self, CreateEventSystemError> {
        let event_system_directory = absolute(event_system_directory)?;
        create_dir(&event_system_directory)?;
        create_dir(event_system_directory.join(EVENT_STAGING_DIRECTORY_NAME))?;
        create_dir(event_system_directory.join(EVENT_STREAMS_DIRECTORY_NAME))?;

        Ok(Self {
            event_system_directory: event_system_directory.into(),
        })
    }

    /// Creates a stream named `event_stream_name` for event type `E`.
    pub fn create_event_handle<E: Event>(
        &self,
        event_stream_name: &str,
        event_stream_config: EventStreamConfig,
    ) -> Result<EventHandle<E>, CreateEventHandleError> {
        let event_stream_directory =
            self.event_stream_directory(event_stream_name)
                .ok_or_else(|| {
                    CreateEventHandleError::InvalidEventStreamName(event_stream_name.to_owned())
                })?;
        let broadcast_config = BroadcastConfig {
            capacity: event_stream_config.capacity,
            producer_slots: event_stream_config.producer_slots,
            consumer_slots: event_stream_config.consumer_slots,
        };

        let staging_directory = self
            .event_system_directory
            .join(EVENT_STAGING_DIRECTORY_NAME);
        let mut temporary_event_stream_directory = tempfile::Builder::new()
            .prefix(&format!(".{event_stream_name}-"))
            .suffix(".tmp")
            .disable_cleanup(false)
            .tempdir_in(staging_directory)?;

        let encoded_schema = wincode::serialize(&E::schema())
            .map_err(CreateEventHandleError::FailedToSerializeSchema)?;
        let mut schema_file = OpenOptions::new().write(true).create_new(true).open(
            temporary_event_stream_directory
                .path()
                .join(EVENT_SCHEMA_FILE_NAME),
        )?;
        schema_file.write_all(&encoded_schema)?;

        let queue_file = queue_file::create_anonymous_file()?;
        // SAFETY:
        // - memfd_create returned a new anonymous file, so this call uniquely
        //   initializes it.
        // - the file is sealed against resizing before it is published below.
        // - Event's safety contract guarantees the QueueCell representation
        //   requirements expected by shaq.
        let broadcast =
            unsafe { Broadcast::<E::QueueCell>::create(&queue_file, broadcast_config) }?;
        queue_file::seal(&queue_file)?;
        queue_file::publish_anonymous_file(
            &queue_file,
            &temporary_event_stream_directory
                .path()
                .join(EVENT_QUEUE_FILE_NAME),
        )?;

        // Publishing the directory atomically prevents readers from observing
        // a queue without its schema, or vice versa.
        rename(
            temporary_event_stream_directory.path(),
            event_stream_directory,
        )?;

        temporary_event_stream_directory.disable_cleanup(true);

        Ok(EventHandle::new(broadcast, Arc::new(queue_file)))
    }

    fn event_stream_directory(&self, event_stream_name: &str) -> Option<PathBuf> {
        let event_stream_name = Path::new(event_stream_name);
        if event_stream_name.file_name() != Some(event_stream_name.as_os_str()) {
            return None;
        }

        Some(
            self.event_system_directory
                .join(EVENT_STREAMS_DIRECTORY_NAME)
                .join(event_stream_name),
        )
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

#[derive(Debug, Error)]
pub enum CreateEventHandleError {
    #[error("event stream name `{0}` is invalid")]
    InvalidEventStreamName(String),
    #[error("failed to serialize the event-stream schema")]
    FailedToSerializeSchema(#[source] wincode::WriteError),
    #[error("failed to create the event-stream files")]
    FileSystem(#[from] std::io::Error),
    #[error("failed to create the event-stream queue")]
    Queue(#[from] shaq::error::Error),
}

#[cfg(test)]
mod tests {
    use super::EventSystem;

    #[test]
    fn event_system_stores_an_absolute_path() {
        let temporary_directory = tempfile::TempDir::new_in(".").unwrap();
        let event_system_directory = std::path::PathBuf::from(
            temporary_directory
                .path()
                .file_name()
                .expect("temporary directory has a file name"),
        )
        .join("event-system");
        assert!(event_system_directory.is_relative());

        let event_system = EventSystem::create(event_system_directory).unwrap();

        assert!(event_system.event_system_directory.is_absolute());
    }
}
