#![cfg(target_os = "linux")]

use {
    agave_event_system::{CreateEventHandleError, EventStreamConfig, EventSystem, event},
    std::{assert_matches, io::ErrorKind},
    tempfile::TempDir,
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
}

#[test]
fn invalid_event_stream_config_is_rejected() {
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
}

#[test]
fn duplicate_event_stream_names_are_rejected() {
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
}
