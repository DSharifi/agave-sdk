use agave_event_system::{Event, event};

#[event]
struct StaticEvent {
    value: u64,
}

#[event(max_serialized_size = 64)]
struct DynamicEvent {
    values: Vec<u8>,
}

#[test]
fn event_macro_sizes_static_queue_cells_from_the_schema() {
    assert_eq!(
        std::mem::size_of::<<StaticEvent as Event>::QueueCell>(),
        std::mem::size_of::<u64>()
    );
}

#[test]
fn event_macro_uses_the_declared_dynamic_size_bound() {
    assert_eq!(
        std::mem::size_of::<<DynamicEvent as Event>::QueueCell>(),
        64
    );
}
