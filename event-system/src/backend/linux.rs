use {
    crate::{
        Event,
        event_system::{
            CreateEventSystemError, CreateStreamError, EventQueueError as PublicEventQueueError,
            StreamConfig,
        },
    },
    shaq::broadcast::{Broadcast, BroadcastConfig},
    std::{
        fs::{File, OpenOptions, create_dir, create_dir_all, remove_dir_all},
        io::{self, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::fs::symlink,
        },
        path::{Path, PathBuf},
        sync::Arc,
    },
};

// Layout of the event-system directory:
//
// event-system-directory/
// ├── tmp/
// │   └── transaction-events/
// │       ├── queue-<id>
// │       └── schema
// └── event-streams/
//     ├── shred-events/
//     │   ├── queue-<id>
//     │   └── schema
//     └── slot-events/
//         ├── queue-<id>
//         └── schema
//
const QUEUE_FILE_NAME: &str = "queue";
const SCHEMA_FILE_NAME: &str = "schema";

const STAGING_DIRECTORY_NAME: &str = "tmp";
const STREAMS_DIRECTORY_NAME: &str = "event-streams";

// Seals required by shaq's safety contract to prevent the file from resizing.
const REQUIRED_SEALS: libc::c_int = libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;
const ANONYMOUS_FILE_NAME: *const libc::c_char = c"agave-event-stream".as_ptr();

pub(crate) type EventQueueError = shaq::error::Error;

#[derive(Debug, Clone)]
pub(crate) struct EventSystem {
    event_system_directory: Arc<Path>,
}

impl EventSystem {
    /// Creates an event system directory in the given path, `event_system_directory`.
    ///
    /// ### Note:
    /// - If the directory path already exists, it must be empty.
    /// - This functions creates the given directory and any missing parents.
    /// - The given path is canonicalized.
    pub(crate) fn new(
        event_system_directory: impl AsRef<Path>,
    ) -> Result<Self, CreateEventSystemError> {
        create_dir_all(&event_system_directory)?;
        let event_system_directory = event_system_directory.as_ref().canonicalize()?;
        create_dir(event_system_directory.join(STAGING_DIRECTORY_NAME))?;
        create_dir(event_system_directory.join(STREAMS_DIRECTORY_NAME))?;

        Ok(Self {
            event_system_directory: event_system_directory.into(),
        })
    }

    /// Creates a stream named `stream_name` for event type `E`
    /// and returns its [`ProducerFactory`].
    pub(crate) fn create_stream<E: Event>(
        &self,
        stream_name: &str,
        stream_config: StreamConfig,
    ) -> Result<ProducerFactory<E>, CreateStreamError> {
        if is_invalid_event_stream_name(stream_name) {
            return Err(CreateStreamError::InvalidStreamName(
                stream_name.to_string(),
            ));
        }

        let event_stream_directory = self
            .event_system_directory
            .join(STREAMS_DIRECTORY_NAME)
            .join(stream_name);

        let staging_directory = self
            .event_system_directory
            .join(STAGING_DIRECTORY_NAME)
            .join(stream_name);
        let temporary_event_stream_directory = StagingDirectory::new(staging_directory)?;

        let schema_file_path = temporary_event_stream_directory
            .path()
            .join(SCHEMA_FILE_NAME);
        let mut schema_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(schema_file_path)?;
        let encoded_schema =
            wincode::serialize(&E::schema()).map_err(CreateStreamError::FailedToSerializeSchema)?;
        schema_file.write_all(&encoded_schema)?;

        let queue_identifier = getrandom::u64()
            .map_err(io::Error::from)
            .map_err(CreateStreamError::OsRngFailure)?;
        let producer_factory = create_queue(stream_config, queue_identifier)?;

        let queue_file_name = format!("{QUEUE_FILE_NAME}-{queue_identifier}");
        let queue_file_path = temporary_event_stream_directory
            .path()
            .join(queue_file_name);
        let process_id = std::process::id();
        let queue_fd = producer_factory.queue_file.as_raw_fd();
        let proc_fd_path = format!("/proc/{process_id}/fd/{queue_fd}");
        symlink(proc_fd_path, queue_file_path)?;

        temporary_event_stream_directory.publish(&event_stream_directory)?;

        Ok(producer_factory)
    }
}

/// Returns whether the stream name is invalid as a single file name.
fn is_invalid_event_stream_name(stream_name: &str) -> bool {
    let is_current_or_parent_directory = matches!(stream_name, "." | "..");
    let contains_forbidden_characters = stream_name.contains(['/', '\0']);

    stream_name.is_empty() || is_current_or_parent_directory || contains_forbidden_characters
}

/// Creates and seals a queue, keeping its backing file alive in the returned factory.
fn create_queue<E: Event>(
    stream_config: StreamConfig,
    queue_identifier: u64,
) -> Result<ProducerFactory<E>, CreateStreamError> {
    let broadcast_config = BroadcastConfig {
        capacity: stream_config.capacity,
        producer_slots: stream_config.producer_slots,
        consumer_slots: stream_config.consumer_slots,
    };

    let check_libc_result = |result: i32| {
        if result == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    };

    // SAFETY: ANONYMOUS_FILE_NAME points to a valid static C string.
    let queue_fd = unsafe {
        libc::memfd_create(
            ANONYMOUS_FILE_NAME,
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    check_libc_result(queue_fd)?;

    // SAFETY: memfd_create returned a new owned file descriptor.
    let queue_file = unsafe { File::from_raw_fd(queue_fd) };

    // SAFETY:
    // - memfd_create returned a new anonymous file, so this call uniquely
    //   initializes it.
    // - the file is sealed against resizing below.
    // - E::QueueCell guarantees Broadcast::create's T type requirements.
    let broadcast = unsafe {
        Broadcast::create_with_identifier(&queue_file, broadcast_config, queue_identifier)
    }
    .map_err(|error| CreateStreamError::Queue(PublicEventQueueError(error)))?;

    // SAFETY: queue_file owns a valid descriptor and F_ADD_SEALS accepts this bitmask.
    let seal_result = unsafe { libc::fcntl(queue_fd, libc::F_ADD_SEALS, REQUIRED_SEALS) };
    check_libc_result(seal_result)?;

    Ok(ProducerFactory::new(broadcast, Arc::new(queue_file)))
}

pub(crate) struct ProducerFactory<E: Event> {
    pub(crate) broadcast: Broadcast<E::QueueCell>,
    pub(crate) queue_file: Arc<File>,
}

impl<E: Event> ProducerFactory<E> {
    fn new(broadcast: Broadcast<E::QueueCell>, queue_file: Arc<File>) -> Self {
        Self {
            broadcast,
            queue_file,
        }
    }
}

impl<E: Event> Clone for ProducerFactory<E> {
    fn clone(&self) -> Self {
        Self {
            broadcast: self.broadcast.clone(),
            queue_file: Arc::clone(&self.queue_file),
        }
    }
}

impl<E: Event> std::fmt::Debug for ProducerFactory<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerFactory")
            .field("broadcast", &self.broadcast)
            .finish_non_exhaustive()
    }
}

struct StagingDirectory {
    path: PathBuf,
    cleanup: bool,
}

impl StagingDirectory {
    fn new(path: PathBuf) -> io::Result<Self> {
        create_dir(&path)?;
        Ok(Self {
            path,
            cleanup: true,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn publish(mut self, destination: &Path) -> io::Result<()> {
        // Publishing the directory atomically prevents readers from observing
        // a queue without its schema, or vice versa.
        //
        // Published streams are nonempty, so rename cannot replace them.
        std::fs::rename(&self.path, destination)?;
        self.cleanup = false;
        Ok(())
    }
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = remove_dir_all(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            EventSystem, QUEUE_FILE_NAME, SCHEMA_FILE_NAME, STAGING_DIRECTORY_NAME,
            STREAMS_DIRECTORY_NAME, StagingDirectory,
        },
        crate::{CreateStreamError, StreamConfig, event},
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

    const TEST_CONFIG: StreamConfig = StreamConfig {
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

        let event_system = EventSystem::new(event_system_directory).unwrap();

        assert!(event_system.event_system_directory.is_absolute());
    }

    #[test]
    fn invalid_stream_name_does_not_create_a_staging_directory() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::new(&event_system_directory).unwrap();

        assert_matches!(
            event_system.create_stream::<TestEvent>("nested/stream", TEST_CONFIG),
            Err(CreateStreamError::InvalidStreamName(_))
        );
        assert_eq!(
            std::fs::read_dir(event_system_directory.join(STAGING_DIRECTORY_NAME))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn missing_staging_directory_prevents_publication() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        std::fs::remove_dir(event_system_directory.join(STAGING_DIRECTORY_NAME)).unwrap();

        assert_matches!(
            event_system.create_stream::<TestEvent>("test-events", TEST_CONFIG),
            Err(CreateStreamError::FileSystem(error))
                if error.kind() == ErrorKind::NotFound
        );
        assert_eq!(
            std::fs::read_dir(event_system_directory.join(STREAMS_DIRECTORY_NAME))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn create_stream_rejects_an_occupied_staging_name() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        std::fs::create_dir(
            event_system_directory
                .join(STAGING_DIRECTORY_NAME)
                .join("test-events"),
        )
        .unwrap();

        assert_matches!(
            event_system.create_stream::<TestEvent>("test-events", TEST_CONFIG),
            Err(CreateStreamError::FileSystem(error))
                if error.kind() == ErrorKind::AlreadyExists
        );
    }

    #[test]
    fn staging_directory_drop_removes_its_contents() {
        let temporary_directory = TempDir::new().unwrap();
        let path = temporary_directory.path().join("staging");
        let staging_directory = StagingDirectory::new(path.clone()).unwrap();
        std::fs::write(path.join("file"), b"contents").unwrap();

        drop(staging_directory);

        assert!(!path.exists());
    }

    #[test]
    fn published_staging_directory_preserves_its_contents() {
        let temporary_directory = TempDir::new().unwrap();
        let path = temporary_directory.path().join("staging");
        let destination = temporary_directory.path().join("published");
        let staging_directory = StagingDirectory::new(path.clone()).unwrap();
        std::fs::write(path.join("file"), b"contents").unwrap();

        staging_directory.publish(&destination).unwrap();

        assert!(!path.exists());
        assert_eq!(
            std::fs::read(destination.join("file")).unwrap(),
            b"contents"
        );
    }

    #[test]
    fn staging_directory_creation_preserves_existing_contents() {
        let temporary_directory = TempDir::new().unwrap();
        let path = temporary_directory.path().join("staging");
        std::fs::create_dir(&path).unwrap();
        let file_path = path.join("file");
        std::fs::write(&file_path, b"contents").unwrap();

        drop(StagingDirectory::new(path));

        assert_eq!(std::fs::read(file_path).unwrap(), b"contents");
    }

    #[test]
    fn failed_queue_creation_removes_its_staging_directory() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        let invalid_config = StreamConfig {
            capacity: 0,
            ..TEST_CONFIG
        };

        assert_matches!(
            event_system.create_stream::<TestEvent>("test-events", invalid_config),
            Err(CreateStreamError::Queue(_))
        );
        assert_eq!(
            std::fs::read_dir(event_system_directory.join(STAGING_DIRECTORY_NAME))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn failed_event_stream_publication_removes_its_staging_directory() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        let _producer_factory = event_system
            .create_stream::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();
        assert_matches!(
            event_system.create_stream::<TestEvent>("test-events", TEST_CONFIG),
            Err(CreateStreamError::FileSystem(_))
        );
        assert_eq!(
            std::fs::read_dir(event_system_directory.join(STAGING_DIRECTORY_NAME))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn event_stream_can_replace_an_existing_empty_directory() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        let event_stream_directory = event_system_directory
            .join(STREAMS_DIRECTORY_NAME)
            .join("test-events");
        std::fs::create_dir(&event_stream_directory).unwrap();

        let _producer_factory = event_system
            .create_stream::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();
    }

    #[test]
    fn event_stream_publishes_its_schema() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        let _producer_factory = event_system
            .create_stream::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        let event_stream_directory = event_system_directory
            .join(STREAMS_DIRECTORY_NAME)
            .join("test-events");
        let encoded_schema = std::fs::read(event_stream_directory.join(SCHEMA_FILE_NAME)).unwrap();
        let schema = wincode::deserialize::<RootSchema>(&encoded_schema).unwrap();

        assert_matches!(schema, RootSchema::Struct(_));
    }

    #[test]
    fn event_stream_publishes_its_queue_identifier_in_the_filename() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        let producer_factory = event_system
            .create_stream::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        let event_stream_directory = event_system_directory
            .join(STREAMS_DIRECTORY_NAME)
            .join("test-events");

        let mut file_names = std::fs::read_dir(event_stream_directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        file_names.sort();

        let queue_identifier = producer_factory.broadcast.queue_identifier();
        assert_eq!(
            file_names,
            vec![
                format!("{QUEUE_FILE_NAME}-{queue_identifier}"),
                SCHEMA_FILE_NAME.to_owned(),
            ]
        );
    }

    #[test]
    fn event_stream_publishes_a_queue_symlink() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        let producer_factory = event_system
            .create_stream::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        let event_stream_directory = event_system_directory
            .join(STREAMS_DIRECTORY_NAME)
            .join("test-events");
        let queue_identifier = producer_factory.broadcast.queue_identifier();
        let queue_path =
            event_stream_directory.join(format!("{QUEUE_FILE_NAME}-{queue_identifier}"));

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
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        let producer_factory = event_system
            .create_stream::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        let event_stream_directory = event_system_directory
            .join(STREAMS_DIRECTORY_NAME)
            .join("test-events");
        let queue_identifier = producer_factory.broadcast.queue_identifier();
        let queue_path =
            event_stream_directory.join(format!("{QUEUE_FILE_NAME}-{queue_identifier}"));
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
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        let producer_factory = event_system
            .create_stream::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        let queue_identifier = producer_factory.broadcast.queue_identifier();
        let queue_path = event_system_directory
            .join(STREAMS_DIRECTORY_NAME)
            .join("test-events")
            .join(format!("{QUEUE_FILE_NAME}-{queue_identifier}"));
        let queue_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(queue_path)
            .unwrap();
        let queue_fd = queue_file.as_raw_fd();

        // SAFETY: queue_file owns a valid descriptor and F_ADD_SEALS accepts this seal.
        let seal_result =
            unsafe { libc::fcntl(queue_fd, libc::F_ADD_SEALS, libc::F_SEAL_FUTURE_WRITE) };
        let seal_error = io::Error::last_os_error();
        assert_eq!(seal_result, -1);
        assert_eq!(seal_error.raw_os_error(), Some(libc::EPERM));
    }

    #[test]
    fn successful_event_stream_publication_leaves_no_staging_directory() {
        let temporary_directory = TempDir::new().unwrap();
        let event_system_directory = temporary_directory.path().join("event-system");
        let event_system = EventSystem::new(&event_system_directory).unwrap();
        let _producer_factory = event_system
            .create_stream::<TestEvent>("test-events", TEST_CONFIG)
            .unwrap();

        assert_eq!(
            std::fs::read_dir(event_system_directory.join(STAGING_DIRECTORY_NAME))
                .unwrap()
                .count(),
            0
        );
    }
}
