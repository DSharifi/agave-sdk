#![cfg(target_os = "linux")]

use {
    agave_event_system::{CreateEventHandleError, EventStreamConfig, EventSystem, event},
    std::{assert_matches, fs::OpenOptions},
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
fn create_fails_when_directory_is_reused() {
    let temporary_directory = TempDir::new().unwrap();
    let event_system_directory = temporary_directory.path().join("event-system");

    let _event_system = EventSystem::create(&event_system_directory).unwrap();

    assert!(EventSystem::create(event_system_directory).is_err());
}

#[test]
fn invalid_event_stream_names_are_rejected() {
    let temporary_directory = TempDir::new().unwrap();
    let event_system_directory = temporary_directory.path().join("event-system");
    let event_system = EventSystem::create(&event_system_directory).unwrap();

    for invalid_name in [
        "",
        ".",
        "..",
        "nested/stream",
        "/absolute",
        "trailing/",
        "trailing//",
        "trailing/.",
    ] {
        assert_matches!(
            event_system.create_event_handle::<TestEvent>(invalid_name, TEST_CONFIG),
            Err(CreateEventHandleError::InvalidEventStreamName(name)) if name == invalid_name
        );
    }
    assert_eq!(
        std::fs::read_dir(event_system_directory.join("tmp"))
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
        std::fs::read_dir(event_system_directory.join("tmp"))
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
        Err(CreateEventHandleError::FileSystem(_))
    );
    assert_eq!(
        std::fs::read_dir(event_system_directory.join("tmp"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn event_stream_is_published_with_its_schema_and_a_sealed_queue() {
    let temporary_directory = TempDir::new().unwrap();
    let event_system_directory = temporary_directory.path().join("event-system");
    let event_system = EventSystem::create(&event_system_directory).unwrap();
    let _event_handle = event_system
        .create_event_handle::<TestEvent>("test-events", TEST_CONFIG)
        .unwrap();

    let event_stream_directory = event_system_directory
        .join("event-streams")
        .join("test-events");
    let encoded_schema = std::fs::read(event_stream_directory.join("schema")).unwrap();
    let schema = wincode::deserialize::<RootSchema>(&encoded_schema).unwrap();
    let queue_path = event_stream_directory.join("queue");
    assert_matches!(schema, RootSchema::Struct(_));
    assert!(queue_path.is_file());
    assert!(
        std::fs::symlink_metadata(&queue_path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read_dir(event_system_directory.join("tmp"))
            .unwrap()
            .count(),
        0
    );

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
