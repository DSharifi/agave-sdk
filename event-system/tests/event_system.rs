use agave_event_system::{EventStreamConfig, EventSystem, event};

#[event]
struct TestEvent {
    value: u64,
}

#[test]
fn event_system_is_a_no_op() {
    let event_system_directory = std::env::temp_dir().join(format!(
        "agave-event-system-stub-test-{}",
        std::process::id()
    ));
    assert!(!event_system_directory.exists());

    let event_system = EventSystem::create(&event_system_directory).unwrap();
    let event_handle = event_system
        .create_event_handle::<TestEvent>(
            "../an-invalid-stream-name",
            EventStreamConfig {
                capacity: 0,
                producer_slots: 0,
                consumer_slots: 0,
            },
        )
        .unwrap();
    let _cloned_event_handle = event_handle.clone();

    assert!(!event_system_directory.exists());
}
