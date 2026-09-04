use {
    crate::{
        Event,
        event_system::{
            CreateEventHandleError, CreateEventSystemError, EventQueueError, EventStreamConfig,
        },
    },
    shaq::broadcast::{Broadcast, BroadcastConfig},
    std::{
        fs::{File, OpenOptions, create_dir, rename},
        io::Write,
        path::{Path, PathBuf, absolute},
        sync::Arc,
    },
};

mod queue_file;

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

#[derive(Debug, Clone)]
pub(crate) struct EventSystem {
    event_system_directory: Arc<Path>,
}

impl EventSystem {
    pub(crate) fn create(
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

    pub(crate) fn create_event_handle<E: Event>(
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
        let broadcast = unsafe { Broadcast::<E::QueueCell>::create(&queue_file, broadcast_config) }
            .map_err(|error| CreateEventHandleError::Queue(EventQueueError(Box::new(error))))?;
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

pub(crate) struct EventHandle<E: Event> {
    pub(crate) broadcast: Broadcast<E::QueueCell>,
    pub(crate) queue_file: Arc<File>,
}

impl<E: Event> EventHandle<E> {
    fn new(broadcast: Broadcast<E::QueueCell>, queue_file: Arc<File>) -> Self {
        Self {
            broadcast,
            queue_file,
        }
    }
}

impl<E: Event> Clone for EventHandle<E> {
    fn clone(&self) -> Self {
        Self {
            broadcast: self.broadcast.clone(),
            queue_file: Arc::clone(&self.queue_file),
        }
    }
}

impl<E: Event> std::fmt::Debug for EventHandle<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventHandle")
            .field("broadcast", &self.broadcast)
            .finish()
    }
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
