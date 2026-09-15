use {
    agave_event_system::stream_policy::{ParseStreamFilterError, StreamPolicy},
    rstest::rstest,
    std::assert_matches,
};

#[rstest]
#[case::white_space(" ")]
#[case::tabbed("\t")]
#[case::empty("")]
#[case::default(StreamPolicy::default())]
fn policies_matches_off(#[case] stream_policy: StreamPolicy) {
    let off_policy: StreamPolicy = "off".parse().unwrap();
    assert_eq!(stream_policy, off_policy);
}

#[rstest]
#[case::adjacent("")]
#[case::separated_by_prefix("banking_stage=on,")]
fn rejects_duplicate_defaults(
    #[case] intervening_rules: &str,
    #[values("on", "off")] first: &str,
    #[values("on", "off", " ON ", " oFf ")] second: &str,
) {
    let result = format!("{first},{intervening_rules}{second}").parse::<StreamPolicy>();
    assert_eq!(
        result,
        Err(ParseStreamFilterError::DuplicateDefaultRule {
            default_rule_1: first.to_string(),
            default_rule_2: second.trim().to_ascii_lowercase(),
        })
    );
}

#[rstest]
#[case::adjacent("")]
#[case::separated_by_other_rules("on,network.=off,")]
fn rejects_duplicate_prefixes(
    #[case] intervening_rules: &str,
    #[values("on", "off")] first: &str,
    #[values("on", "off", "")] second: &str,
) {
    let result = format!("banking_stage={first},{intervening_rules}banking_stage={second}")
        .parse::<StreamPolicy>();
    assert_eq!(
        result,
        Err(ParseStreamFilterError::DuplicatePrefixRule {
            prefix: "banking_stage".to_string(),
            rule_1: first.to_string(),
            rule_2: if second.is_empty() { "on" } else { second }.to_string(),
        })
    );
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
#[case::only_separator(",")]
#[case::only_empty_entries(" , ,\t,\n, ")]
#[case::trailing_separator("on,")]
#[case::leading_separator(",off")]
#[case::consecutive_separators("on,,banking_stage=off")]
#[case::blank_directive("on, \t\n ,banking_stage=off")]
#[case::trailing_blank_directive("network.=on, \t")]
fn rejects_empty_directives(#[case] value: &str) {
    assert_eq!(
        value.parse::<StreamPolicy>(),
        Err(ParseStreamFilterError::EmptyDirective)
    );
}

#[rstest]
#[case::empty_prefix_on("=on")]
#[case::empty_prefix_off("=off")]
#[case::empty_prefix_and_value("=")]
#[case::blank_prefix(" \t =on")]
fn rejects_empty_prefixes(#[case] directive_with_no_prefix: &str) {
    let policy_with_invalid_directive =
        format!("on,network.=on,{directive_with_no_prefix},network.repair=off");

    for stream_policy_string in [
        directive_with_no_prefix.to_string(),
        policy_with_invalid_directive,
    ] {
        assert_eq!(
            stream_policy_string.parse::<StreamPolicy>(),
            Err(ParseStreamFilterError::InvalidPrefixRule(
                directive_with_no_prefix.trim().to_string()
            ))
        );
    }
}

#[rstest]
#[case::unknown_value("invalid")]
#[case::unsupported_value("allow")]
#[case::extra_assignment_in_value("on=off")]
fn rejects_invalid_prefix_rule_values(#[case] invalid_rule_value: &str) {
    for policy_string in [
        format!("network.={invalid_rule_value}"),
        format!("on,network.=on,network.={invalid_rule_value},network.repair=off"),
    ] {
        assert_eq!(
            policy_string.parse::<StreamPolicy>(),
            Err(ParseStreamFilterError::InvalidStreamRuleValue(
                invalid_rule_value.to_string()
            ))
        );
    }
}
