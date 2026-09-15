use {crate::stream_name::StreamName, std::str::FromStr};

const DIRECTIVE_DELIMITER: &str = ",";
const PREFIX_RULE_ASSIGNMENT: &str = "=";

const ON: &str = "on";
const OFF: &str = "off";

/// Configures which streams in an [`EventSystem`](crate::EventSystem) can emit events.
///
/// # Enabling events
///
/// Events are disabled on all streams by default. An empty or whitespace only
/// policy string is equivalent to `off` and [`StreamPolicy::default()`].
///
/// Use [`str::parse`] to create a policy with the following syntax:
///
/// ```text
/// on | off | prefix[=[rule]]
/// ```
///
/// A policy is a comma-separated list of directives in these forms. Brackets
/// mark optional parts. A bare `on` or `off` sets the default rule. Any other
/// bare directive enables events for a stream name prefix. A prefix can also
/// have an explicit rule (`<prefix>=<rule>`). For example:
///
/// ```text
/// off,network.=on,network.repair=off
/// ```
///
/// The **prefix** selects streams whose names start with the given text.
/// For example, `network.=on` enables events for `network.gossip`,
/// `network.repair`, and `network.repair.requests`. Prefixes must be non-empty
/// and are matched case-sensitively.
///
/// The **rule** controls whether matching streams can emit events:
///
/// - `on` enables events.
/// - `off` disables events.
///
/// Both `=` and the rule are optional: `network.` and `network.=` have the
/// same effect as `network.=on`. This shorthand can appear anywhere in the list.
/// To target a prefix named `on` or `off`, use `on=on` or `off=on`.
///
/// Rule names ignore ASCII case: `off`, `OFF`, and `oFf` have the same effect.
/// The examples below use lowercase names.
///
/// For a non-empty policy, parsing returns a [`ParseStreamFilterError`] if any
/// directive is empty, malformed, or repeated. This includes leading or trailing
/// commas and consecutive commas.
///
/// # Rule precedence
///
/// The longest matching prefix determines the rule for a stream. Streams with
/// no matching prefix use the default rule, which is `off` unless a directive
/// changes it. The default directive may appear anywhere in the list.
///
/// A policy may contain at most one default directive and one directive per
/// prefix. Repeating either is an error, even if the rule values agree.
/// For example, `on,on` and `network.=on,network.=off` are both invalid.
/// Different prefixes may overlap; the longest matching prefix takes precedence.
///
/// # Examples
///
/// Example policies:
///
/// - `on` enables events on all streams.
/// - `off` disables events on all streams.
/// - `network.=on` enables events on streams starting with `network.`.
/// - `network.` and `network.=` also enable events on streams starting with `network.`.
/// - `on,network.repair=off` enables events except on streams starting with
///   `network.repair`.
///
/// This policy enables `network.gossip`, disables `network.repair` and
/// `network.repair.requests`, and disables streams outside the `network.` prefix:
///
/// ```
/// use agave_event_system::stream_policy::StreamPolicy;
/// # use agave_event_system::stream_policy::ParseStreamFilterError;
///
/// let policy: StreamPolicy = "off,network.=on,network.repair=off".parse()?;
/// # Ok::<(), ParseStreamFilterError>(())
/// ```
///
/// # Disabled streams
///
/// Events emitted by a [`Producer`](crate::producer::Producer) on a disabled
/// stream are dropped. Its [`StreamSubscriber`](crate::subscriber::StreamSubscriber)s
/// do not receive those events, and enabling the stream later does not replay them.
#[derive(Debug, PartialEq, Eq, Default)]
pub struct StreamPolicy {
    /// Rule to apply when no prefix matches the stream name.
    pub(crate) default_rule: StreamRule,
    /// Prefix rules sorted by increasing prefix length, then lexicographically.
    pub(crate) prefix_rules_sorted_by_length: Box<[PrefixStreamRule]>,
}

impl StreamPolicy {
    #[expect(
        dead_code,
        reason = "will be used in a follow up when policy is used on linux backend"
    )]
    pub(crate) fn stream_rule(&self, stream_name: &StreamName) -> StreamRule {
        let best_match = self
            .prefix_rules_sorted_by_length
            .iter()
            .rev()
            .find(|PrefixStreamRule { prefix, .. }| stream_name.as_str().starts_with(prefix))
            .map(|PrefixStreamRule { stream_state, .. }| stream_state)
            .copied();

        best_match.unwrap_or(self.default_rule)
    }
}

impl FromStr for StreamPolicy {
    type Err = ParseStreamFilterError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.trim().is_empty() {
            return Ok(Self::default());
        }

        let mut prefix_rules: Vec<PrefixStreamRule> = vec![];
        let mut default_rule = None;

        for directive in input
            .split(DIRECTIVE_DELIMITER)
            // white space around directives are allowed
            .map(str::trim)
        {
            if directive.is_empty() {
                return Err(ParseStreamFilterError::EmptyDirective);
            }

            if let Ok(parsed_rule) = directive.parse::<StreamRule>() {
                if let Some(previous_default_rule) = default_rule.replace(parsed_rule) {
                    return Err(ParseStreamFilterError::DuplicateDefaultRule {
                        default_rule_1: previous_default_rule.to_string(),
                        default_rule_2: parsed_rule.to_string(),
                    });
                }
                continue;
            }

            let prefix_rule = directive.parse::<PrefixStreamRule>()?;

            if let Some(previous_rule) = prefix_rules
                .iter()
                // error only if the duplicate prefix has a different rule
                .find(|rule| rule.prefix == prefix_rule.prefix)
            {
                return Err(ParseStreamFilterError::DuplicatePrefixRule {
                    prefix: prefix_rule.prefix,
                    rule_1: previous_rule.stream_state.to_string(),
                    rule_2: prefix_rule.stream_state.to_string(),
                });
            }

            prefix_rules.push(prefix_rule);
        }

        prefix_rules.sort_by(|left, right| {
            left.prefix
                .len()
                .cmp(&right.prefix.len())
                // sort lexicographic as tie breaker so we have same structure
                // for equality checks of `StreamPolicy::eq` implementation.
                .then_with(|| left.prefix.cmp(&right.prefix))
        });

        Ok(Self {
            default_rule: default_rule.unwrap_or_default(),
            prefix_rules_sorted_by_length: prefix_rules.into_boxed_slice(),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PrefixStreamRule {
    prefix: String,
    stream_state: StreamRule,
}

impl FromStr for PrefixStreamRule {
    type Err = ParseStreamFilterError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (prefix, rule_value) = input
            .split_once(PREFIX_RULE_ASSIGNMENT)
            .unwrap_or((input, ""));
        let prefix = prefix.trim();
        if prefix.is_empty() {
            return Err(ParseStreamFilterError::InvalidPrefixRule(input.to_string()));
        }

        let rule_value = rule_value.trim();
        let stream_state = if rule_value.is_empty() {
            StreamRule::On
        } else {
            rule_value.parse()?
        };

        Ok(Self {
            prefix: prefix.to_string(),
            stream_state,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum StreamRule {
    On,
    #[default]
    Off,
}

impl FromStr for StreamRule {
    type Err = ParseStreamFilterError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case(ON) {
            Ok(Self::On)
        } else if value.eq_ignore_ascii_case(OFF) {
            Ok(Self::Off)
        } else {
            Err(ParseStreamFilterError::InvalidStreamRuleValue(
                value.to_string(),
            ))
        }
    }
}

impl std::fmt::Display for StreamRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamRule::On => f.write_str(ON),
            StreamRule::Off => f.write_str(OFF),
        }
    }
}

/// An error encountered while parsing a stream policy.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ParseStreamFilterError {
    #[error("empty stream directive")]
    EmptyDirective,
    #[error("duplicate default rule: {default_rule_1} and {default_rule_2}")]
    DuplicateDefaultRule {
        default_rule_1: String,
        default_rule_2: String,
    },
    #[error("duplicate rule for prefix {prefix:?}: {rule_1} and {rule_2}")]
    DuplicatePrefixRule {
        prefix: String,
        rule_1: String,
        rule_2: String,
    },
    #[error("invalid prefix rule. Expected a non-empty prefix, got {0}")]
    InvalidPrefixRule(String),
    #[error("invalid stream rule value. Must be either {ON} or {OFF}, got {0}")]
    InvalidStreamRuleValue(String),
}
