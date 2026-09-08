use std::collections::BTreeMap;
use serde::{Deserialize, Deserializer};


/// A YAML mapping of scalars, as the string map an environment variable can carry.
///
/// Numbers and booleans are coerced because `retries: 3` is the obvious thing to write.
/// A structure is rejected rather than JSON-encoded: an env var cannot carry one, and
/// `{"a":1}` as a value would be indistinguishable from a value that is that text.
pub fn deserialize_string_map<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: BTreeMap<String, serde_yaml::Value> = BTreeMap::deserialize(deserializer)?;

    let mut string_map = BTreeMap::new();

    for (key, value) in raw {

        let string_value = match value {
            serde_yaml::Value::String(value) => value,
            serde_yaml::Value::Number(value) => value.to_string(),
            serde_yaml::Value::Bool(value) => value.to_string(),
            serde_yaml::Value::Null => {
                return Err(serde::de::Error::custom(format!(
                    "'{}' has no value - write \"\" for an empty one",
                    key,
                )));
            }
            other => {
                return Err(serde::de::Error::custom(format!(
                    "'{}' is {}, but only a string, a number or a boolean can reach a command",
                    key,
                    describe_value(&other),
                )));
            }
        };

        string_map.insert(key, string_value);
    }

    Ok(string_map)
}

fn describe_value(value: &serde_yaml::Value) -> &'static str {
    match value {
        serde_yaml::Value::Sequence(_) => "a list",
        serde_yaml::Value::Mapping(_) => "a map",
        serde_yaml::Value::Tagged(_) => "a tagged value",
        _ => "not a scalar",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct Holder {
        #[serde(default, deserialize_with = "deserialize_string_map")]
        map: BTreeMap<String, String>,
    }

    fn parse(yaml: &str) -> anyhow::Result<BTreeMap<String, String>> {
        let holder: Holder = serde_yaml::from_str(yaml)?;
        Ok(holder.map)
    }

    #[test]
    fn a_missing_map_is_empty() {
        assert!(parse("{}").unwrap().is_empty());
    }

    #[test]
    fn a_string_is_kept_as_written() {
        let map = parse("map:\n  region: eu\n").unwrap();
        assert_eq!(map.get("region").unwrap(), "eu");
    }

    #[test]
    fn an_empty_string_is_kept() {
        let map = parse("map:\n  slice: \"\"\n").unwrap();
        assert_eq!(map.get("slice").unwrap(), "");
    }

    #[test]
    fn a_number_is_coerced() {
        let map = parse("map:\n  retries: 3\n").unwrap();
        assert_eq!(map.get("retries").unwrap(), "3");
    }

    #[test]
    fn a_boolean_is_coerced() {
        let map = parse("map:\n  dry_run: true\n").unwrap();
        assert_eq!(map.get("dry_run").unwrap(), "true");
    }

    /// An env var cannot carry a structure, and JSON-encoding one silently would make it
    /// indistinguishable from a value that is literally that text.
    #[test]
    fn a_nested_map_is_rejected_naming_the_key() {
        let error = parse("map:\n  nested:\n    a: 1\n").unwrap_err().to_string();
        assert!(error.contains("nested"), "{error}");
        assert!(error.contains("a map"), "{error}");
    }

    #[test]
    fn a_list_is_rejected_naming_the_key() {
        let error = parse("map:\n  items:\n    - a\n").unwrap_err().to_string();
        assert!(error.contains("items"), "{error}");
        assert!(error.contains("a list"), "{error}");
    }

    /// `slice:` with nothing after it is a value the user forgot, not an empty one. The
    /// message has to say how to write an empty value, or the fix is guesswork.
    #[test]
    fn a_bare_key_is_rejected_and_says_how_to_write_empty() {
        let error = parse("map:\n  slice:\n").unwrap_err().to_string();
        assert!(error.contains("slice"), "{error}");
        assert!(error.contains("\"\""), "{error}");
    }
}
