use {
    agave_event_system::{Event, event},
    rstest::rstest,
    std::mem::size_of,
};

#[event]
enum StaticEnumEvent {
    Event1 { value: u64 },
}

#[event]
struct StaticEvent {
    value: u64,
}

#[event(max_serialized_size = 64)]
struct DynamicEvent {
    values: Vec<u8>,
}

#[event(max_serialized_size = 64)]
enum DynamicEnumEvent {
    Event1 { values: Vec<u8> },
}

#[test]
fn event_macro_sizes_static_struct_queue_cell_from_the_schema() {
    assert_eq!(
        size_of::<<StaticEvent as Event>::QueueCell>(),
        size_of::<u64>()
    );
}

#[test]
fn event_macro_sizes_static_enum_queue_cell_from_the_schema() {
    let tag_size = size_of::<u32>();
    let payload_size = size_of::<u64>();
    let expected_size = tag_size.checked_add(payload_size).unwrap();

    assert_eq!(
        size_of::<<StaticEnumEvent as Event>::QueueCell>(),
        expected_size
    );
}

#[test]
fn event_macro_uses_the_declared_dynamic_size_bound_for_struct() {
    assert_eq!(size_of::<<DynamicEvent as Event>::QueueCell>(), 64);
}

#[test]
fn event_macro_uses_the_declared_dynamic_size_bound_for_enum() {
    assert_eq!(size_of::<<DynamicEnumEvent as Event>::QueueCell>(), 64);
}

#[rstest]
#[case::static_struct(StaticEvent { value: 1 })]
#[case::static_enum(StaticEnumEvent::Event1 { value: 1 })]
#[case::dynamic_struct(DynamicEvent { values: vec![1, 2, 3] })]
#[case::dynamic_enum(DynamicEnumEvent::Event1 { values: vec![1, 2, 3] })]
fn event_macro_implements_event_trait(#[case] event: impl Event) {
    // drop so `event` is not dead code.
    // clippy give false positive about it when it's underscored.
    drop(event)
}
