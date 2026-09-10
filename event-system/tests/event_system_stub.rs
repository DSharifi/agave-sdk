#![cfg(not(target_os = "linux"))]

use agave_event_system::{EventSystem, StreamConfig, event};

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

    let event_system = EventSystem::new(&event_system_directory).unwrap();
    let producer_factory = event_system
        .create_stream::<TestEvent>(
            "../an-invalid-stream-name",
            StreamConfig {
                capacity: 0,
                producer_slots: 0,
                consumer_slots: 0,
            },
        )
        .unwrap();
    let _cloned_producer_factory = producer_factory.clone();

    assert!(!event_system_directory.exists());
}
