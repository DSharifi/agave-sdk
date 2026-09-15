use std::{collections::HashMap, str::FromStr};

const FILTER_VALUES_DELIMITER: &str = ",";
const PREFIX_RULE_ASSIGNMENT: &str = "=";

const ALLOW: &str = "allow";
const DENY: &str = "deny";

/// Configures which stream in an [`EventSystem`](crate::EventSystem) can emit events.
///
/// A rule can either be "allow" or "deny" which respectively allow and disable a stream
/// from having events being emitted by producers.
///
/// ## Denied streams
///
/// Events emitted to denied streams by [`Producer`](crate::producer::Producer) are dropped
/// meaning the corresponding [`StreamSubscriber`](crate::subscriber::StreamSubscriber)s will receive none
/// of the events that are emitted on that stream while it is denied.
///
/// Events emitted while the stream is denied are dropped and not re-transmitted automatically by [`agave_event_system`](crate).
///
/// # Creating a [`StreamPolicy`]
///
/// StreamPolicies can be created with [`str::parse`].
///
/// ## String Format
///
/// The expected format is a comma separated list of entries, where each entry must be:
/// - a bare `allow` or `deny`, which sets the default rule.
/// - `<prefix>=allow` or `<prefix>=deny` sets a rule for a the prefix. A prefix can not be empty.
///
/// Duplicate entries are not permitted.
///
///
/// # Rule precedence
///
/// A stream's assigned rule is determined by the following precedence order:
/// 1. the longest prefix rule in te ruleset that is a prefix of stream name.
/// 2. the default policy in the ruleset
/// 3. deny
///
///
/// # Examples
///
/// This policy allows `network.gossip`, denies `network.repair` and
/// `network.repair.requests`, and denies names outside the `network.` prefix:
///
/// ```
/// use agave_event_system::stream_policy::StreamPolicy;
/// # use agave_event_system::stream_policy::ParseStreamFilterError;
///
/// let policy: StreamPolicy = "deny,network.=allow,network.repair=deny".parse()?;
/// # Ok::<(), ParseStreamFilterError>(())
/// ```
#[derive(Debug)]
pub struct StreamPolicy {
    /// Rule to apply when no prefix matches the stream name.
    #[expect(
        dead_code,
        reason = "will be used in a follow up when policy is used on linux backend"
    )]
    pub(crate) default_rule: StreamRule,
    #[expect(
        dead_code,
        reason = "will be used in a follow up when policy is used on linux backend"
    )]
    /// Rules indexed by literal prefix; the longest matching prefix wins.
    pub(crate) prefix_rules: HashMap<String, StreamRule>,
}

impl StreamPolicy {
    #[expect(
        dead_code,
        reason = "will be used in a follow up when policy is used on linux backend"
    )]
    pub(crate) fn stream_rule(&self, stream_name: &str) -> StreamRule {
        let best_match = self
            .prefix_rules
            .iter()
            .filter(|(prefix, _)| stream_name.starts_with(*prefix))
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(_, stream_rule)| *stream_rule);

        best_match.unwrap_or(self.default_rule)
    }
}

impl FromStr for StreamPolicy {
    type Err = ParseStreamFilterError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() {
            return Ok(Self {
                default_rule: StreamRule::default(),
                prefix_rules: HashMap::new(),
            });
        }

        let mut default_rule: Option<StreamRule> = None;
        let mut prefix_rules = HashMap::new();
        for rule_entry in input.split(FILTER_VALUES_DELIMITER) {
            if rule_entry.is_empty() {
                return Err(ParseStreamFilterError::EmptyRule);
            }

            // the entry is a default rule
            if let Ok(parsed_rule) = rule_entry.parse::<StreamRule>() {
                if let Some(previous_default_rule) = default_rule.replace(parsed_rule) {
                    return Err(ParseStreamFilterError::DuplicateDefaultRule {
                        default_rule_1: previous_default_rule.to_string(),
                        default_rule_2: parsed_rule.to_string(),
                    });
                }
                continue;
            }

            // Both a missing assignment and an empty prefix are invalid.
            let Some((prefix, rule_value)) = rule_entry
                .split_once(PREFIX_RULE_ASSIGNMENT)
                .filter(|(prefix, _)| !prefix.is_empty())
            else {
                return Err(ParseStreamFilterError::InvalidPrefixRule(
                    rule_entry.to_string(),
                ));
            };

            if prefix_rules
                .insert(prefix.to_string(), rule_value.parse()?)
                .is_some()
            {
                return Err(ParseStreamFilterError::DuplicatePrefixRule(
                    prefix.to_string(),
                ));
            }
        }

        Ok(Self {
            default_rule: default_rule.unwrap_or_default(),
            prefix_rules,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum StreamRule {
    Allow,
    #[default]
    Deny,
}

impl FromStr for StreamRule {
    type Err = ParseStreamFilterError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            ALLOW => Ok(Self::Allow),
            DENY => Ok(Self::Deny),
            invalid_rule => Err(ParseStreamFilterError::InvalidStreamRuleValue(
                invalid_rule.to_string(),
            )),
        }
    }
}

impl std::fmt::Display for StreamRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamRule::Allow => f.write_str(ALLOW),
            StreamRule::Deny => f.write_str(DENY),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseStreamFilterError {
    #[error(
        "invalid prefix rule. Expected a non-empty prefix followed by {PREFIX_RULE_ASSIGNMENT} \
         and a rule, got {0}"
    )]
    InvalidPrefixRule(String),
    #[error("invalid stream rule value. Must be either {ALLOW} or {DENY}, got {0}")]
    InvalidStreamRuleValue(String),
    #[error("got more than one rule for prefix {0}")]
    DuplicatePrefixRule(String),
    #[error("got more than one default value. {default_rule_1} and {default_rule_2}")]
    DuplicateDefaultRule {
        default_rule_1: String,
        default_rule_2: String,
    },
    #[error("got an empty stream rule \"{FILTER_VALUES_DELIMITER}{FILTER_VALUES_DELIMITER}\"")]
    EmptyRule,
}
