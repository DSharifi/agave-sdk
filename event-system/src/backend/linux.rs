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
        fs::{File, OpenOptions, create_dir, remove_dir_all},
        io::{self, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::{ffi::OsStrExt, fs::symlink},
        },
        path::{Path, PathBuf, absolute},
        sync::Arc,
    },
    uuid::Uuid,
};

// Layout of the event-system directory:
//
// event-system-directory/
// ├── tmp/
// │   └── transaction-events<random>/
// │       ├── queue
// │       ├── queue-identity
// │       └── schema
// └── event-streams/
//     ├── shred-events/
//     │   ├── queue
//     │   ├── queue-identity
//     │   └── schema
//     └── slot-events/
//         ├── queue
//         ├── queue-identity
//         └── schema
//
const EVENT_QUEUE_FILE_NAME: &str = "queue";
const EVENT_QUEUE_IDENTITY_FILE_NAME: &str = "queue-identity";
const EVENT_SCHEMA_FILE_NAME: &str = "schema";
const EVENT_STAGING_DIRECTORY_NAME: &str = "tmp";
const EVENT_STREAMS_DIRECTORY_NAME: &str = "event-streams";

// Seals required by shaq's safety contract to prevent the file from resizing.
const REQUIRED_SEALS: libc::c_int = libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;
const ANONYMOUS_FILE_NAME_PREFIX: &str = "agave-event-stream";

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

        // file descriptors can be reused.
        // By attaching a random identity to each event stream we guard
        // against race conditions where one stream is halfway closed
        // but the symlinked queue file points to a new file with a file
        // descriptor that is reused.
        //
        //
        // Example Scenario we guard for:
        // 1. subscriber follows A/queue and obtains "/proc/123/fd/42"
        // 2. subscriber pauses
        //
        // 3. publisher removes A’s directory and closes 42
        // 4. publisher creates B, which receives 42
        //
        // 5. subscriber resumes and opens 42 - receiving B instead of A
        //
        //
        // Solution:
        // 1. publisher stores the same random UUID in A/queue-identity and
        //    A's memfd name.
        // 2. subscriber opens A's directory and reads the expected UUID and
        //    schema relative to that open directory.
        // 3. subscriber opens queue relative to the same directory. During
        //    this call, the kernel may follow the symlink, pause, and then
        //    resolve /proc/123/fd/42 after the publisher has reused 42 for B.
        // 4. subscriber reads /proc/self/fd/<its-opened-fd> to get the memfd
        //    name of the file it actually opened and extracts its UUID.
        // 5. if the UUID differs from the expected UUID, it closes the file
        //    and retries discovery. In this example, B's UUID differs from A's,
        //    so the subscriber rejects B before joining the queue.
        // 6. if the UUID matches, it maps and joins that same open file.
        //    Reusing the publisher's fd cannot change the subscriber's open
        //    file, so the match remains valid after the check.
        //
        // If any required file is missing, the subscriber retries discovery.
        let queue_identity = Uuid::new_v4();
        let anonymous_file_name = CString::new(format!(
            "{ANONYMOUS_FILE_NAME_PREFIX}-{queue_identity}"
        ))
        .expect(
            "<<ANONYMOUS_FILE_NAME_PREFIX>>-<<random_queue_file_identity>> contains no NUL bytes",
        );

        // SAFETY: anonymous_file_name is a valid C string.
        let anonymous_file_file_descriptor = unsafe {
            libc::memfd_create(
                anonymous_file_name.as_ptr(),
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

        // A subscriber can finish following the symlink after teardown has
        // closed and reused the descriptor. The random ID in the memfd name
        // lets it reject that race without relying on inode numbers, which can
        // also be reused (see the attachment check in the crate docs).
        let queue_identity_file_path = temporary_event_stream_directory
            .path()
            .join(EVENT_QUEUE_IDENTITY_FILE_NAME);
        std::fs::write(queue_identity_file_path, format!("{queue_identity}\n"))?;

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

        Ok(EventHandle::new(
            broadcast,
            queue_file,
            event_stream_directory,
        ))
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

// Shared publication lifetime. Future producer handles must also retain this
// Arc: a shaq producer keeps the mapping alive, but not the published fd.
struct EventStream {
    directory: PathBuf,
    // Retain the published descriptor until after Drop removes the directory.
    _queue_file: File,
}

impl Drop for EventStream {
    fn drop(&mut self) {
        // File fields are dropped after this method returns. If cleanup fails,
        // subscribers can still reject a reused descriptor via queue-identity.
        let _ = remove_dir_all(&self.directory);
    }
}

pub(crate) struct EventHandle<E: Event> {
    pub(crate) broadcast: Broadcast<E::QueueCell>,
    stream: Arc<EventStream>,
}

impl<E: Event> EventHandle<E> {
    fn new(broadcast: Broadcast<E::QueueCell>, queue_file: File, directory: PathBuf) -> Self {
        Self {
            broadcast,
            stream: Arc::new(EventStream {
                directory,
                _queue_file: queue_file,
            }),
        }
    }
}

impl<E: Event> Clone for EventHandle<E> {
    fn clone(&self) -> Self {
        Self {
            broadcast: self.broadcast.clone(),
            stream: Arc::clone(&self.stream),
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
            EVENT_QUEUE_FILE_NAME, EVENT_QUEUE_IDENTITY_FILE_NAME, EVENT_SCHEMA_FILE_NAME,
            EVENT_STAGING_DIRECTORY_NAME, EVENT_STREAMS_DIRECTORY_NAME, EventSystem,
        },
        crate::{CreateEventHandleError, Event, EventStreamConfig, event},
        shaq::broadcast::{Broadcast, ProducerId},
        std::{
            assert_matches,
            fs::{File, OpenOptions},
            io::{self, ErrorKind},
            os::fd::{AsRawFd, FromRawFd},
            path::PathBuf,
            process::Command,
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

    #[test]
    fn last_handle_unpublishes_before_descriptor_reuse() {
        const CHILD_ENV: &str = "AGAVE_EVENT_FD_REUSE_TEST";
        if std::env::var_os(CHILD_ENV).is_none() {
            // Isolate fd allocation from the other parallel tests so reuse is
            // deterministic, without overwriting a descriptor they might own.
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "backend::linux::tests::last_handle_unpublishes_before_descriptor_reuse",
                    "--nocapture",
                ])
                .env(CHILD_ENV, "1")
                .output()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            return;
        }

        let temporary_directory = TempDir::new().unwrap();
        let directory = temporary_directory.path().join("event-system");
        let system = EventSystem::create(&directory).unwrap();
        let stream_path = directory.join(EVENT_STREAMS_DIRECTORY_NAME).join("a");
        let queue_path = stream_path.join(EVENT_QUEUE_FILE_NAME);
        let handle = system
            .create_event_handle::<TestEvent>("a", TEST_CONFIG)
            .unwrap();
        let clone = handle.clone();

        // Keep the original directory open, as a subscriber must do when
        // reading the schema, identity and queue across multiple syscalls.
        let stream_directory = File::open(&stream_path).unwrap();
        let pinned_path = PathBuf::from(format!("/proc/self/fd/{}", stream_directory.as_raw_fd()));
        let expected_identity =
            std::fs::read_to_string(pinned_path.join(EVENT_QUEUE_IDENTITY_FILE_NAME)).unwrap();
        let proc_path = std::fs::read_link(&queue_path).unwrap();
        let original_fd: libc::c_int = proc_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let queue = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&queue_path)
            .unwrap();
        assert_eq!(queue_identity(&queue), expected_identity);
        // SAFETY: this is TestEvent's initialized, sealed byte-array queue.
        let joined = unsafe { Broadcast::<<TestEvent as Event>::QueueCell>::join(&queue) }.unwrap();
        let mut consumer = joined.consumer().unwrap();
        let mut producer = handle.broadcast.producer(ProducerId::new(1)).unwrap();

        drop(handle);
        assert!(stream_path.exists());
        assert_eq!(
            queue_identity(&File::open(&queue_path).unwrap()),
            expected_identity
        );

        let other_handle = system
            .create_event_handle::<TestEvent>("b", TEST_CONFIG)
            .unwrap();
        let other_queue = File::open(
            directory
                .join(EVENT_STREAMS_DIRECTORY_NAME)
                .join("b")
                .join(EVENT_QUEUE_FILE_NAME),
        )
        .unwrap();
        drop(clone);
        assert!(!stream_path.exists());

        // SAFETY: other_queue is valid. F_DUPFD_CLOEXEC allocates a new fd
        // starting at original_fd without overwriting any existing descriptor.
        let reused_fd =
            unsafe { libc::fcntl(other_queue.as_raw_fd(), libc::F_DUPFD_CLOEXEC, original_fd) };
        assert_eq!(reused_fd, original_fd);
        // SAFETY: fcntl returned a new owned descriptor.
        let reused_file = unsafe { File::from_raw_fd(reused_fd) };
        assert_eq!(queue_identity(&reused_file), queue_identity(&other_queue));
        assert_eq!(
            File::open(&queue_path).unwrap_err().kind(),
            ErrorKind::NotFound
        );

        // Simulate a subscriber that resolved the symlink before teardown but
        // opened its /proc target after fd reuse: the saved identity rejects B.
        let stale_queue = File::open(proc_path).unwrap();
        assert_ne!(queue_identity(&stale_queue), expected_identity);
        assert_eq!(queue_identity(&queue), expected_identity);
        producer.try_write(42u64.to_le_bytes()).unwrap();
        assert_eq!(consumer.try_read(), Some(42u64.to_le_bytes()));

        let replacement = system
            .create_event_handle::<TestEvent>("a", TEST_CONFIG)
            .unwrap();
        assert_ne!(
            queue_identity(&File::open(&queue_path).unwrap()),
            expected_identity,
        );
        assert_eq!(
            File::open(pinned_path.join(EVENT_QUEUE_FILE_NAME))
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound,
        );
        drop((replacement, other_handle));
    }

    fn queue_identity(queue: &File) -> String {
        // Inspect the subscriber's opened file, not the publisher's fd path.
        let target = std::fs::read_link(format!("/proc/self/fd/{}", queue.as_raw_fd())).unwrap();
        let identity = target
            .to_str()
            .unwrap()
            .strip_prefix("/memfd:agave-event-stream-")
            .unwrap()
            .strip_suffix(" (deleted)")
            .unwrap();
        assert_eq!(identity.len(), 32);
        u128::from_str_radix(identity, 16).unwrap();
        format!("{identity}\n")
    }
}
