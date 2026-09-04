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

#[test]
fn create_event_system_fails_when_path_is_a_file() {
    let temporary_directory = TempDir::new().unwrap();
    let event_system_directory = temporary_directory.path().join("event-system");
    let contents = b"existing file contents";
    std::fs::write(&event_system_directory, contents).unwrap();

    assert_matches!(EventSystem::new(&event_system_directory), Err(_));
    assert_eq!(std::fs::read(&event_system_directory).unwrap(), contents);
}

#[test]
fn create_event_system_fails_when_directory_is_reused() {
    let temporary_directory = TempDir::new().unwrap();
    let event_system_directory = temporary_directory.path().join("event-system");

    let _event_system = EventSystem::new(&event_system_directory).unwrap();

    assert!(EventSystem::new(event_system_directory).is_err());
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
    let temporary_directory = TempDir::new().unwrap();
    let event_system_directory = temporary_directory.path().join("event-system");
    let event_system = EventSystem::new(&event_system_directory).unwrap();

    assert_matches!(
        event_system.create_stream::<TestEvent>(invalid_name, TEST_CONFIG),
        Err(CreateStreamError::InvalidStreamName(name)) if name == invalid_name
    );
}

#[test]
fn create_stream_rejects_invalid_config() {
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
}

#[test]
fn create_stream_rejects_duplicate_stream_names() {
    let temporary_directory = TempDir::new().unwrap();
    let event_system_directory = temporary_directory.path().join("event-system");
    let event_system = EventSystem::new(&event_system_directory).unwrap();
    let _producer_factory = event_system
        .create_stream::<TestEvent>("test-events", TEST_CONFIG)
        .unwrap();
    assert_matches!(
        event_system.create_stream::<TestEvent>("test-events", TEST_CONFIG),
        Err(CreateStreamError::FileSystem(error))
            if matches!(
                error.kind(),
                // Linux permits EEXIST or ENOTEMPTY for a nonempty destination.
                ErrorKind::AlreadyExists | ErrorKind::DirectoryNotEmpty
            )
    );
}
