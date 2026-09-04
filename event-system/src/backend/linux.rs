use {
    crate::{
        Event,
        event_system::{
            CreateEventHandleError, CreateEventSystemError,
            EventQueueError as PublicEventQueueError, EventStreamConfig,
        },
    },
    shaq::broadcast::{Broadcast, BroadcastConfig},
    std::{
        ffi::CString,
        fs::{File, OpenOptions, create_dir},
        io::{self, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::{ffi::OsStrExt, fs::symlink},
        },
        path::{Path, PathBuf, absolute},
        sync::Arc,
    },
};

// Layout of the event-system directory:
//
// event-system-directory/
// ├── tmp/
// │   └── transaction-events<random>/
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

// Seals required by shaq's safety contract to prevent the file from resizing.
const REQUIRED_SEALS: libc::c_int = libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;
const ANONYMOUS_FILE_NAME: *const libc::c_char = c"agave-event-stream".as_ptr();

pub(crate) type EventQueueError = shaq::error::Error;

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
            .prefix(event_stream_name)
            // cleans up the staging directory if we fail creating the event
            // in subsequent steps.
            .disable_cleanup(false)
            .tempdir_in(staging_directory)?;

        let schema_file_path = temporary_event_stream_directory
            .path()
            .join(EVENT_SCHEMA_FILE_NAME);
        let mut schema_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(schema_file_path)?;
        let encoded_schema = wincode::serialize(&E::schema())
            .map_err(CreateEventHandleError::FailedToSerializeSchema)?;
        schema_file.write_all(&encoded_schema)?;

        let check_libc_result = |result: i32| {
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        };

        // SAFETY: ANONYMOUS_FILE_NAME points to a valid static C string.
        let anonymous_file_file_descriptor = unsafe {
            libc::memfd_create(
                ANONYMOUS_FILE_NAME,
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        check_libc_result(anonymous_file_file_descriptor)?;

        // SAFETY: memfd_create returned a new owned file descriptor.
        let queue_file = unsafe { File::from_raw_fd(anonymous_file_file_descriptor) };
        // SAFETY:
        // - memfd_create returned a new anonymous file, so this call uniquely
        //   initializes it.
        // - the file is sealed against resizing before it is published below.
        // - E::QueCell guarantees Broadcast::create's T type requirements.
        let broadcast = unsafe { Broadcast::create(&queue_file, broadcast_config) }
            .map_err(|error| CreateEventHandleError::Queue(PublicEventQueueError(error)))?;

        let queue_file_descriptor = queue_file.as_raw_fd();
        // SAFETY: queue_file owns a valid descriptor and F_ADD_SEALS accepts this bitmask.
        let seal_result =
            unsafe { libc::fcntl(queue_file_descriptor, libc::F_ADD_SEALS, REQUIRED_SEALS) };
        check_libc_result(seal_result)?;

        let queue_file_path = temporary_event_stream_directory
            .path()
            .join(EVENT_QUEUE_FILE_NAME);
        let process_id = std::process::id();
        let proc_fd_path = format!("/proc/{process_id}/fd/{queue_file_descriptor}");
        symlink(proc_fd_path, queue_file_path)?;

        // Publishing the directory atomically prevents readers from observing
        // a queue without its schema, or vice versa.
        let temp_event_directory = CString::new(
            temporary_event_stream_directory
                .path()
                .as_os_str()
                .as_bytes(),
        )
        .expect("temporary_event_stream_directory is valid C string");
        let target_event_directory = CString::new(event_stream_directory.as_os_str().as_bytes())
            .expect("target_event_directory is valid C string");

        // SAFETY: source and destination are valid, NUL-terminated path strings.
        let rename_result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                temp_event_directory.as_ptr(),
                libc::AT_FDCWD,
                target_event_directory.as_ptr(),
                // error if an event with same name already exists
                libc::RENAME_NOREPLACE,
            )
        };
        check_libc_result(rename_result)?;

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
    use {
        super::{
            EVENT_QUEUE_FILE_NAME, EVENT_SCHEMA_FILE_NAME, EVENT_STAGING_DIRECTORY_NAME,
            EVENT_STREAMS_DIRECTORY_NAME, EventSystem,
        },
        crate::{CreateEventHandleError, EventStreamConfig, event},
        std::{
            assert_matches,
            fs::OpenOptions,
            io::{self, ErrorKind},
            os::fd::AsRawFd,
        },
        tempfile::TempDir,
        wincode_dynamic::RootSchema,
    };

    #[event]
    struct TestEvent {
        value: u64,
    }

    const TEST_CONFIG: EventStreamConfig = EventStreamConfig {
        capacity: 2,
        producer_slots: 1,
        consumer_slots: 1,
    };

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

    #[test]
    fn invalid_event_stream_name_does_not_create_a_staging_directory() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::create(&event_system_directory).unwrap();

        assert_matches!(
            event_system.create_event_handle::<TestEvent>("nested/stream", TEST_CONFIG),
            Err(CreateEventHandleError::InvalidEventStreamName(_))
        );
        assert_eq!(
            std::fs::read_dir(event_system_directory.join(EVENT_STAGING_DIRECTORY_NAME))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn failed_queue_creation_removes_its_staging_directory() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::create(&event_system_directory).unwrap();
        let invalid_config = EventStreamConfig {
            capacity: 0,
            ..TEST_CONFIG
        };

        assert_matches!(
            event_system.create_event_handle::<TestEvent>("test-events", invalid_config),
            Err(CreateEventHandleError::Queue(_))
        );
        assert_eq!(
            std::fs::read_dir(event_system_directory.join(EVENT_STAGING_DIRECTORY_NAME))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn failed_event_stream_publication_removes_its_staging_directory() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::create(&event_system_directory).unwrap();
        let _event_handle = event_system
            .create_event_handle::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();
        assert_matches!(
            event_system.create_event_handle::<TestEvent>("test-events", TEST_CONFIG),
            Err(CreateEventHandleError::FileSystem(error))
                if error.kind() == ErrorKind::AlreadyExists
        );
        assert_eq!(
            std::fs::read_dir(event_system_directory.join(EVENT_STAGING_DIRECTORY_NAME))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn event_stream_does_not_replace_an_existing_empty_directory() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::create(&event_system_directory).unwrap();
        let event_stream_directory = event_system_directory
            .join(EVENT_STREAMS_DIRECTORY_NAME)
            .join("test-events");
        std::fs::create_dir(&event_stream_directory).unwrap();

        assert_matches!(
            event_system.create_event_handle::<TestEvent>("test-events", TEST_CONFIG),
            Err(CreateEventHandleError::FileSystem(error))
                if error.kind() == ErrorKind::AlreadyExists
        );
        assert_eq!(
            std::fs::read_dir(event_stream_directory).unwrap().count(),
            0
        );
    }

    #[test]
    fn event_stream_publishes_its_schema() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::create(&event_system_directory).unwrap();
        let _event_handle = event_system
            .create_event_handle::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        let event_stream_directory = event_system_directory
            .join(EVENT_STREAMS_DIRECTORY_NAME)
            .join("test-events");
        let encoded_schema =
            std::fs::read(event_stream_directory.join(EVENT_SCHEMA_FILE_NAME)).unwrap();
        let schema = wincode::deserialize::<RootSchema>(&encoded_schema).unwrap();

        assert_matches!(schema, RootSchema::Struct(_));
    }

    #[test]
    fn event_stream_publishes_a_queue_symlink() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::create(&event_system_directory).unwrap();
        let _event_handle = event_system
            .create_event_handle::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        let event_stream_directory = event_system_directory
            .join(EVENT_STREAMS_DIRECTORY_NAME)
            .join("test-events");
        let queue_path = event_stream_directory.join(EVENT_QUEUE_FILE_NAME);

        assert!(queue_path.is_file());
        assert!(
            std::fs::symlink_metadata(queue_path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn published_queue_cannot_be_resized() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::create(&event_system_directory).unwrap();
        let _event_handle = event_system
            .create_event_handle::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        let event_stream_directory = event_system_directory
            .join(EVENT_STREAMS_DIRECTORY_NAME)
            .join("test-events");
        let queue_path = event_stream_directory.join(EVENT_QUEUE_FILE_NAME);
        let queue_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(queue_path)
            .unwrap();
        let queue_size = queue_file.metadata().unwrap().len();
        assert_eq!(
            queue_file.set_len(0).unwrap_err().raw_os_error(),
            Some(libc::EPERM)
        );
        assert_eq!(
            queue_file
                .set_len(queue_size.saturating_add(1))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EPERM)
        );
    }

    #[test]
    fn published_queue_seals_cannot_be_changed() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::create(&event_system_directory).unwrap();
        let _event_handle = event_system
            .create_event_handle::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        let queue_path = event_system_directory
            .join(EVENT_STREAMS_DIRECTORY_NAME)
            .join("test-events")
            .join(EVENT_QUEUE_FILE_NAME);
        let queue_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(queue_path)
            .unwrap();
        let queue_file_descriptor = queue_file.as_raw_fd();

        // SAFETY: queue_file owns a valid descriptor and F_ADD_SEALS accepts this seal.
        let seal_result = unsafe {
            libc::fcntl(
                queue_file_descriptor,
                libc::F_ADD_SEALS,
                libc::F_SEAL_FUTURE_WRITE,
            )
        };
        let seal_error = io::Error::last_os_error();
        assert_eq!(seal_result, -1);
        assert_eq!(seal_error.raw_os_error(), Some(libc::EPERM));
    }

    #[test]
    fn successful_event_stream_publication_leaves_no_staging_directory() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::create(&event_system_directory).unwrap();
        let _event_handle = event_system
            .create_event_handle::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        assert_eq!(
            std::fs::read_dir(event_system_directory.join(EVENT_STAGING_DIRECTORY_NAME))
                .unwrap()
                .count(),
            0
        );
    }
}
