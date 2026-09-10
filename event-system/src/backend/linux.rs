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
        super::{REQUIRED_SEALS, StagingDirectory, create_queue},
        crate::{StreamConfig, event},
        std::{io, os::fd::AsRawFd},
        tempfile::TempDir,
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
    fn staging_directory_publish_preserves_contents() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("staging");
        let destination = directory.path().join("published");

        let staging = StagingDirectory::new(path.clone()).unwrap();
        std::fs::write(staging.path().join("file"), b"published contents").unwrap();
        staging.publish(&destination).unwrap();
        assert!(!path.exists());
        assert_eq!(
            std::fs::read(destination.join("file")).unwrap(),
            b"published contents"
        );
    }

    #[test]
    fn unpublished_staging_directory_removes_contents_on_drop() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("staging");
        let staging = StagingDirectory::new(path.clone()).unwrap();
        std::fs::write(staging.path().join("file"), b"unpublished contents").unwrap();
        drop(staging);
        assert!(!path.exists());
    }

    #[test]
    fn queue_file_is_sealed_against_resizing_and_additional_seals() {
        let producer_factory = create_queue::<TestEvent>(TEST_CONFIG, 0).unwrap();
        let queue_file = &producer_factory.queue_file;
        let queue_fd = queue_file.as_raw_fd();
        // SAFETY: queue_file owns a valid descriptor and F_GET_SEALS takes no extra argument.
        let seals = unsafe { libc::fcntl(queue_fd, libc::F_GET_SEALS) };
        assert_ne!(seals, -1);
        assert_eq!(seals & REQUIRED_SEALS, REQUIRED_SEALS,);

        let queue_size = queue_file.metadata().unwrap().len();
        assert_eq!(
            queue_file.set_len(0).unwrap_err().raw_os_error(),
            Some(libc::EPERM),
        );
        assert_eq!(
            queue_file
                .set_len(queue_size.saturating_add(1))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EPERM)
        );

        // SAFETY: queue_file owns a valid descriptor and F_ADD_SEALS accepts this seal.
        let seal_result =
            unsafe { libc::fcntl(queue_fd, libc::F_ADD_SEALS, libc::F_SEAL_FUTURE_WRITE) };
        let seal_error = io::Error::last_os_error();
        assert_eq!(seal_result, -1);
        assert_eq!(seal_error.raw_os_error(), Some(libc::EPERM));
    }
}
