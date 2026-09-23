#![cfg(target_os = "linux")]

mod common;

use {
    crate::common::{TEST_EVENT, TestContextBuilder},
    agave_event_system::subscriber::RecvTimeoutError,
    rstest::rstest,
    std::{
        assert_matches,
        time::{Duration, Instant},
    },
};

#[rstest]
fn recv_timeout_errors_when_no_message_is_emitted(
    #[values(Duration::ZERO, Duration::from_micros(10))] timeout: Duration,
) {
    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let (_producer_factory, mut subscriber) = test_context.create_stream_with_subscriber();

    let wait_start = Instant::now();
    let recv_result = subscriber.recv_timeout(timeout).map(|_message| ());
    let waited = wait_start.elapsed();

    assert_matches!(recv_result, Err(RecvTimeoutError));
    assert!(
        waited >= timeout,
        "recv_timeout returned after {waited:?}, before the {timeout:?} timeout"
    );
}

#[test]
fn recv_timeout_returns_already_emitted_message() {
    const LONG_DURATION: Duration = Duration::from_secs(10);

    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let (producer_factory, mut subscriber) = test_context.create_stream_with_subscriber();
    let mut producer = producer_factory.try_create_producer().unwrap();
    producer.emit_event(&TEST_EVENT).unwrap();

    let received_event = subscriber
        .recv_timeout(LONG_DURATION)
        .expect("existing message is already in the stream")
        .decode()
        .unwrap();

    assert_eq!(received_event, TEST_EVENT);
}
