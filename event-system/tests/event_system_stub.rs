#![cfg(not(target_os = "linux"))]

use agave_event_system::{EventSystem, StreamConfig, event, stream_name};

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
    let publisher_factory = event_system
        .create_stream::<TestEvent>(
            stream_name!("test-stream"),
            StreamConfig {
                capacity: 0,
                publisher_slots: 0,
                subscriber_slots: 0,
            },
        )
        .unwrap();
    let _cloned_publisher_factory = publisher_factory.clone();

    assert!(!event_system_directory.exists());
}
