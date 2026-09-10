#![cfg(target_os = "linux")]

use {
    agave_event_system::{CreateStreamError, EventSystem, StreamConfig, event},
    rstest::rstest,
    std::{assert_matches, io::ErrorKind},
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

struct TestContext {
    event_system: EventSystem,
    _directory: TempDir,
}

impl TestContext {
    fn new_event_system() -> Self {
        let directory = TempDir::new().unwrap();
        let event_system = EventSystem::new(directory.path().join("event-system")).unwrap();

        Self {
            event_system,
            _directory: directory,
        }
    }
}

#[test]
fn create_event_system_fails_when_path_is_a_file() {
    const EXISTING_CONTENTS: &[u8] = b"existing file contents are preserved";

    let event_system_directory = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(&event_system_directory, EXISTING_CONTENTS).unwrap();

    assert_matches!(EventSystem::new(&event_system_directory), Err(_));
    assert_eq!(
        std::fs::read(&event_system_directory).unwrap(),
        EXISTING_CONTENTS
    );
}

#[test]
fn create_event_system_fails_when_directory_is_reused() {
    let directory = TempDir::new().unwrap();
    let path = directory.path();

    let _event_system = EventSystem::new(path).unwrap();

    let event_system_with_reused_path_result = EventSystem::new(path);
    assert_matches!(event_system_with_reused_path_result, Err(_));
}

#[rstest]
#[case::empty("")]
#[case::embedded_nul("stream\0name")]
#[case::current_directory(".")]
#[case::parent_directory("..")]
#[case::nested_path("nested/stream")]
#[case::absolute_path("/absolute")]
#[case::trailing_slash("trailing/")]
#[case::double_trailing_slash("trailing//")]
#[case::trailing_dot("trailing/.")]
fn create_stream_rejects_invalid_stream_names(#[case] invalid_name: &str) {
    let test_context = TestContext::new_event_system();

    assert_matches!(
        test_context.event_system.create_stream::<TestEvent>(invalid_name, TEST_CONFIG),
        Err(CreateStreamError::InvalidStreamName(name)) if name == invalid_name
    );
}

#[test]
fn create_stream_reserves_names_only_after_success() {
    let test_context = TestContext::new_event_system();
    const REUSED_STREAM_NAME: &str = "reused-stream-name";

    let invalid_config = StreamConfig {
        capacity: 0,
        ..TEST_CONFIG
    };

    assert_matches!(
        test_context
            .event_system
            .create_stream::<TestEvent>(REUSED_STREAM_NAME, invalid_config),
        Err(CreateStreamError::Queue(_))
    );
    let _producer_factory = test_context
        .event_system
        .create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG)
        .expect("test-events is unused stream name as it failed above");

    assert_matches!(
        test_context.event_system.create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG),
        Err(CreateStreamError::FileSystem(error))
            if matches!(
                error.kind(),
                // Linux permits EEXIST or ENOTEMPTY for a nonempty destination.
                ErrorKind::AlreadyExists | ErrorKind::DirectoryNotEmpty
            ),
            "creation of the same stream name must now fail, since it succeeded above."
    );
}
