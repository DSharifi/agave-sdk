#[cfg(target_os = "linux")]
use {
    crate::common::{
        TEST_CONFIG, TEST_EVENT, TEST_STREAM_NAME, TestContext, TestContextBuilder, TestEvent,
        assert_is_empty, assert_received,
    },
    agave_event_system::{
        StreamConfig, stream_name,
        subscriber::{self, Subscriber, Typed},
    },
};
use {
    agave_event_system::stream_policy::{ParseStreamFilterError, StreamPolicy},
    rstest::rstest,
    std::assert_matches,
};

mod common;

#[cfg(target_os = "linux")]
fn available_stream_names(test_context: &TestContext) -> Vec<String> {
    let mut stream_names: Vec<String> =
        subscriber::StreamExplorer::new(test_context.event_system_path())
            .available_streams()
            .map(|stream| stream.stream_name().as_str().to_string())
            .collect();
    stream_names.sort();
    stream_names
}

#[cfg(target_os = "linux")]
fn connect(test_context: &TestContext, stream_name: &str) -> Subscriber<Typed<TestEvent>> {
    subscriber::StreamExplorer::new(test_context.event_system_path())
        .available_streams()
        .find(|stream| stream.stream_name().as_str() == stream_name)
        .expect("stream should exist")
        .try_connect_typed::<TestEvent>()
        .unwrap()
}

#[cfg(target_os = "linux")]
#[test]
fn disabled_stream_is_not_published() {
    let test_context = TestContextBuilder::new().build();
    let publisher_factory = test_context
        .event_system
        .create_stream::<TestEvent>(TEST_STREAM_NAME, TEST_CONFIG)
        .unwrap();
    let mut publisher = publisher_factory.try_create_publisher().unwrap();

    publisher.publish(&TEST_EVENT).unwrap();
    publisher.publish_batch(&[TEST_EVENT, TEST_EVENT]).unwrap();

    assert!(available_stream_names(&test_context).is_empty());
    let streams_directory = test_context.event_system_path().join("event-streams");
    assert_eq!(std::fs::read_dir(streams_directory).unwrap().count(), 0);
}

#[cfg(target_os = "linux")]
#[test]
fn toggling_stream_policy_for_live_event_system() {
    let test_context = TestContextBuilder::new().build();

    let create_publisher = |stream_name| {
        test_context
            .event_system
            .create_stream(stream_name, TEST_CONFIG)
            .unwrap()
            .try_create_publisher()
            .unwrap()
    };

    let mut publishers = [
        create_publisher(stream_name!("network.packets")),
        create_publisher(stream_name!("network.drops")),
        create_publisher(stream_name!("block-production.transaction")),
    ];
    let mut publish_on_all_publishers = move || {
        for publisher in publishers.iter_mut() {
            publisher.publish(&TEST_EVENT).unwrap();
        }
    };

    publish_on_all_publishers();
    assert!(
        available_stream_names(&test_context).is_empty(),
        "all streams are disabled by the default policy"
    );

    test_context
        .event_system
        .set_stream_policy("on".parse().unwrap());
    assert_eq!(
        available_stream_names(&test_context),
        [
            "block-production.transaction",
            "network.drops",
            "network.packets"
        ]
    );

    let mut network_packets_subscriber = connect(&test_context, "network.packets");
    let mut network_drops_subscriber = connect(&test_context, "network.drops");
    let mut block_production_transaction_subscriber =
        connect(&test_context, "block-production.transaction");

    // events published while the streams were disabled are not retained
    assert_is_empty([
        &mut network_packets_subscriber,
        &mut network_drops_subscriber,
        &mut block_production_transaction_subscriber,
    ]);

    publish_on_all_publishers();
    assert_received([
        &mut network_packets_subscriber,
        &mut network_drops_subscriber,
        &mut block_production_transaction_subscriber,
    ]);

    test_context.event_system.set_stream_policy(
        "off, network.=on, network.packets=off, block-production=on"
            .parse()
            .unwrap(),
    );
    assert_eq!(
        available_stream_names(&test_context),
        ["block-production.transaction", "network.drops"]
    );

    publish_on_all_publishers();
    assert_is_empty([&mut network_packets_subscriber]);
    assert_received([
        &mut network_drops_subscriber,
        &mut block_production_transaction_subscriber,
    ]);
}

#[cfg(target_os = "linux")]
#[test]
fn reenabled_stream_is_a_new_stream() {
    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let publisher_factory = test_context
        .event_system
        .create_stream::<TestEvent>(TEST_STREAM_NAME, TEST_CONFIG)
        .unwrap();
    let mut publisher = publisher_factory.try_create_publisher().unwrap();
    let mut first_stream_subscriber = connect(&test_context, TEST_STREAM_NAME.as_str());

    test_context
        .event_system
        .set_stream_policy("off".parse().unwrap());
    assert!(available_stream_names(&test_context).is_empty());

    test_context
        .event_system
        .set_stream_policy("on".parse().unwrap());
    let mut second_stream_subscriber = connect(&test_context, TEST_STREAM_NAME.as_str());

    publisher.publish(&TEST_EVENT).unwrap();
    assert_is_empty([&mut first_stream_subscriber]);
    assert_received([&mut second_stream_subscriber]);
}

#[cfg(target_os = "linux")]
#[rstest]
fn publisher_slots_are_reserved_while_disabled(#[values(1, 2)] publisher_slots: usize) {
    let test_context = TestContextBuilder::new().build();
    let stream_config = StreamConfig {
        publisher_slots,
        ..TEST_CONFIG
    };
    let publisher_factory = test_context
        .event_system
        .create_stream::<TestEvent>(TEST_STREAM_NAME, stream_config)
        .unwrap();

    let mut publishers: Vec<_> = (0..publisher_slots)
        .map(|_| publisher_factory.try_create_publisher().unwrap())
        .collect();
    assert_matches!(publisher_factory.try_create_publisher(), None);

    test_context
        .event_system
        .set_stream_policy("on".parse().unwrap());
    let mut subscriber = connect(&test_context, TEST_STREAM_NAME.as_str());

    for publisher in publishers.iter_mut() {
        publisher.publish(&TEST_EVENT).unwrap();
        assert_received([&mut subscriber]);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn publisher_slots_stay_retired_across_stream_policy_changes() {
    let test_context = TestContextBuilder::new().build();
    let publisher_factory = test_context
        .event_system
        .create_stream::<TestEvent>(TEST_STREAM_NAME, TEST_CONFIG)
        .unwrap();

    drop(publisher_factory.try_create_publisher().unwrap());
    assert_matches!(publisher_factory.try_create_publisher(), None);

    test_context
        .event_system
        .set_stream_policy("on".parse().unwrap());
    assert_matches!(publisher_factory.try_create_publisher(), None);
}

#[rstest]
#[case::white_space(" ")]
#[case::tabbed("\t")]
#[case::empty("")]
#[case::default(StreamPolicy::default())]
fn policies_matches_off(#[case] stream_policy: StreamPolicy) {
    let off_policy: StreamPolicy = "off".parse().unwrap();
    assert_eq!(stream_policy, off_policy);
}

#[test]
fn last_rule_wins() {
    let policy: StreamPolicy = "off, network.=on, on, network.=off".parse().unwrap();
    let expected: StreamPolicy = "on, network.=off".parse().unwrap();

    assert_eq!(policy, expected);
}

#[rstest]
#[case::overlapping_prefixes("network.=on,network.repair=off")]
#[case::case_sensitive_prefixes("Network.=on,network.=off")]
fn accepts_distinct_prefixes(#[case] value: &str) {
    assert_matches!(value.parse::<StreamPolicy>(), Ok(_));
}

#[rstest]
#[case::empty_policy("", "off")]
#[case::only_whitespace(" \t\r\n ", "off")]
#[case::bare_prefix("banking_stage", "banking_stage=on")]
#[case::trimmed_bare_prefix(" \t network. \n", "network.=on")]
#[case::multiple_bare_prefixes("network.,banking_stage", "network.=on,banking_stage=on")]
#[case::shorthand_after_default("off,network.", "off,network.=on")]
#[case::default_after_shorthand("network.,on", "on,network.=on")]
#[case::default_between_shorthands(
    "network.,off,banking_stage",
    "off,network.=on,banking_stage=on"
)]
#[case::shorthand_between_rules(
    "off,network.,network.repair=off",
    "off,network.=on,network.repair=off"
)]
#[case::keyword_prefix("off,off=", "off,off=on")]
#[case::uppercase_keyword_prefix("ON=", "ON=on")]
#[case::bare_prefix_overrides_rule("banking_stage=off,banking_stage", "banking_stage=on")]
#[case::empty_value("banking_stage=", "banking_stage=on")]
#[case::blank_value("banking_stage= \t ", "banking_stage=on")]
#[case::uppercase_default_on("ON", "on")]
#[case::mixed_case_default_on("oN", "on")]
#[case::uppercase_default_off("OFF", "off")]
#[case::mixed_case_default_off("oFf", "off")]
#[case::uppercase("ON,banking_stage=OFF", "on,banking_stage=off")]
#[case::mixed_case("oFf,banking_stage=oN", "off,banking_stage=on")]
#[case::whitespace(
    " \tON\n, banking_stage \t=\nOFF , network. = on ",
    "on,banking_stage=off,network.=on"
)]
#[case::default_after_prefix("network.=on,on", "on,network.=on")]
#[case::default_between_prefixes(
    "network.=on,off,network.repair=off",
    "off,network.=on,network.repair=off"
)]
#[case::reordered_equal_length_prefixes("net=on,rpc=off", "rpc=off,net=on")]
#[case::reordered_case_sensitive_prefixes("net=on,Net=off", "Net=off,net=on")]
#[case::reordered_mixed_length_prefixes(
    "network.repair=off,rpc=off,net=on,network.=on",
    "network.=on,net=on,rpc=off,network.repair=off"
)]
fn normalizes_policy(#[case] policy_format_1: &str, #[case] policy_format_2: &str) {
    assert_ne!(
        policy_format_1, policy_format_2,
        "sanity check failed. these strings can not be identical for the test"
    );

    let policy_1: StreamPolicy = policy_format_1.parse().unwrap();
    let policy_2: StreamPolicy = policy_format_2.parse().unwrap();

    assert_eq!(
        policy_1, policy_2,
        "policies must be the same as `StreamPolicy::parse` should normalize the input strings."
    );
}

#[rstest]
#[case::only_separator(",", "off")]
#[case::only_empty_entries(" , ,\t,\n, ", "off")]
#[case::trailing_separator("on,", "on")]
#[case::leading_separator(",off", "off")]
#[case::consecutive_separators("on,,banking_stage=off", "on,banking_stage=off")]
#[case::blank_directive("on, \t\n ,banking_stage=off", "on,banking_stage=off")]
#[case::trailing_blank_directive("network.=on, \t", "network.=on")]
fn ignores_empty_directives(#[case] value: &str, #[case] expected: &str) {
    assert_eq!(
        value.parse::<StreamPolicy>().unwrap(),
        expected.parse::<StreamPolicy>().unwrap()
    );
}

#[rstest]
#[case::empty_prefix_on("=on", "on")]
#[case::empty_prefix_off("=off", "off")]
#[case::empty_prefix_and_value("=", "on")]
#[case::blank_prefix(" \t =on", "on")]
#[case::blank_prefix_and_value(" \t = \n", "on")]
#[case::mixed_case_rule(" = oFf ", "off")]
fn empty_prefix_sets_default_rule(#[case] directive: &str, #[case] default_rule: &str) {
    for (policy, expected) in [
        (directive.to_string(), default_rule.to_string()),
        (
            format!("on,network.=on,{directive},network.repair=off"),
            format!("{default_rule},network.=on,network.repair=off"),
        ),
    ] {
        assert_eq!(
            policy.parse::<StreamPolicy>().unwrap(),
            expected.parse::<StreamPolicy>().unwrap()
        );
    }
}

#[rstest]
#[case::unknown_value("invalid")]
#[case::unsupported_value("allow")]
#[case::extra_assignment_in_value("on=off")]
fn rejects_invalid_rule_values(
    #[case] invalid_rule_value: &str,
    #[values("network.", "")] prefix: &str,
) {
    for policy_string in [
        format!("{prefix}={invalid_rule_value}"),
        format!("on,network.=on,{prefix}={invalid_rule_value},network.repair=off"),
        format!("{prefix}={invalid_rule_value},{prefix}=on"),
    ] {
        assert_eq!(
            policy_string.parse::<StreamPolicy>(),
            Err(ParseStreamFilterError::InvalidStreamRuleValue(
                invalid_rule_value.to_string()
            ))
        );
    }
}
