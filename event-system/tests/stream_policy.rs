use {
    agave_event_system::stream_policy::{ParseStreamFilterError, StreamPolicy},
    rstest::rstest,
    std::assert_matches,
};

#[rstest]
#[case::empty_policy("")]
#[case::default_allow("allow")]
#[case::default_deny("deny")]
#[case::prefix_allow_without_default("solana_core=allow")]
#[case::prefix_deny_without_default("solana_core::replay_stage=deny")]
#[case::multiple_prefixes_without_default("network.=allow,network.repair=deny")]
#[case::default_before_prefixes("deny,network.=allow,network.repair=deny")]
#[case::default_between_prefixes("network.=allow,allow,network.repair=deny")]
#[case::default_after_prefixes("network.=allow,network.repair=deny,deny")]
fn accepts_valid_policy(#[case] value: &str) {
    assert_matches!(value.parse::<StreamPolicy>(), Ok(_));
}

#[rstest]
fn rejects_duplicate_defaults(
    #[values("allow", "deny")] first: &str,
    #[values("allow", "deny")] second: &str,
) {
    let result = format!("{first},banking_stage=allow,{second}").parse::<StreamPolicy>();
    assert_matches!(
        result,
        Err(ParseStreamFilterError::DuplicateDefaultRule {
            default_rule_1,
            default_rule_2,
        }) if default_rule_1 == first && default_rule_2 == second
    );
}

#[rstest]
#[case::adjacent("")]
#[case::separated_by_other_rules("allow,network.=deny,")]
fn rejects_duplicate_prefixes(
    #[case] intervening_rules: &str,
    #[values("allow", "deny")] first: &str,
    #[values("allow", "deny")] second: &str,
) {
    let result = format!("banking_stage={first},{intervening_rules}banking_stage={second}")
        .parse::<StreamPolicy>();
    assert_matches!(
        result,
        Err(ParseStreamFilterError::DuplicatePrefixRule(prefix)) if prefix == "banking_stage"
    );
}

#[rstest]
#[case::only_separator(",")]
#[case::trailing_separator("allow,")]
#[case::leading_separator(",deny")]
#[case::consecutive_separators("allow,,banking_stage=deny")]
fn rejects_empty_entries(#[case] value: &str) {
    assert_matches!(
        value.parse::<StreamPolicy>(),
        Err(ParseStreamFilterError::EmptyRule)
    );
}

#[rstest]
#[case::missing_assignment("banking_stage", "banking_stage")]
#[case::unknown_bare_rule("invalid", "invalid")]
#[case::empty_prefix_allow("=allow", "=allow")]
#[case::empty_prefix_deny("=deny", "=deny")]
#[case::missing_assignment_between_rules("allow,banking_stage,network.=deny", "banking_stage")]
#[case::missing_assignment_between_spaced_rules(
    "allow, banking_stage, network.=deny",
    " banking_stage"
)]
fn rejects_malformed_prefix_rules(#[case] value: &str, #[case] invalid_rule: &str) {
    assert_matches!(
        value.parse::<StreamPolicy>(),
        Err(ParseStreamFilterError::InvalidPrefixRule(invalid)) if invalid == invalid_rule
    );
}

#[rstest]
#[case::empty_value("")]
#[case::unknown_value("invalid")]
#[case::uppercase_value("ALLOW")]
#[case::extra_assignment("allow=deny")]
fn rejects_invalid_rule_values(#[case] rule: &str) {
    assert_matches!(
        format!("banking_stage={rule}").parse::<StreamPolicy>(),
        Err(ParseStreamFilterError::InvalidStreamRuleValue(invalid)) if invalid == rule
    );
}
