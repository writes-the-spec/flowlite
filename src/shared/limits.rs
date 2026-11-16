use std::collections::BTreeMap;
use serde::Serialize;


/// One row of the answer: a name, how many running attempts currently claim it, and how
/// many it allows. `global` is a row of this same shape - reserving that key at load time
/// (`AppConfig::load` rejects it in `[concurrency_limits]`) is what buys the uniform
/// rendering below rather than a special case for the cap.
///
/// Serialized as it stands by both `flowlite limits --json` and the `list_limits` tool -
/// `max` keeps the configured `0` there, since the dash that spells "no ceiling" is a
/// human rendering and belongs to the table.
#[derive(Serialize)]
pub struct LimitRow {
    pub name: String,
    pub in_use: u32,
    pub max: u32,
}


/// The global cap first, then the named limits in `BTreeMap` order (alphabetical) - the
/// plan's "config order" isn't achievable since `concurrency_limits` is a `BTreeMap` and
/// does not retain TOML insertion order, so alphabetical is the deterministic stand-in.
/// A name nothing running claims is absent from `claimed_limit_slots`, so it reads as 0
/// here rather than needing its own case.
pub fn limit_rows(
    max_running_attempts: u32,
    running_attempts: u32,
    concurrency_limits: &BTreeMap<String, u32>,
    claimed_limit_slots: &BTreeMap<String, u32>,
) -> Vec<LimitRow> {

    let mut rows = vec![LimitRow {
        name: "global".to_string(),
        in_use: running_attempts,
        max: max_running_attempts,
    }];

    for (name, max) in concurrency_limits {
        rows.push(LimitRow {
            name: name.clone(),
            in_use: claimed_limit_slots.get(name).copied().unwrap_or(0),
            max: *max,
        });
    }

    rows
}


/// A limit configured `0` means no ceiling at all - the dispatcher skips the check
/// entirely for it (`max_running_attempts > 0` and `configured_max == 0` in
/// `task_run_attempt_dispatcher.rs`) - so there is no maximum for it to ever be full
/// against. Shared by the CLI table and the dashboard panel so this rule lives in one
/// place rather than two copies that could drift apart.
pub fn is_full(row: &LimitRow) -> bool {
    row.max != 0 && row.in_use >= row.max
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_global_row_comes_first_and_named_limits_follow_in_alphabetical_order() {
        let concurrency_limits = BTreeMap::from([
            ("warehouse".to_string(), 3),
            ("openai_api".to_string(), 5),
        ]);
        let claimed_limit_slots = BTreeMap::from([
            ("warehouse".to_string(), 3),
            ("openai_api".to_string(), 1),
        ]);

        let rows = limit_rows(32, 32, &concurrency_limits, &claimed_limit_slots);

        let names: Vec<&str> = rows.iter().map(|row| row.name.as_str()).collect();
        assert_eq!(names, vec!["global", "openai_api", "warehouse"]);
    }

    #[test]
    fn a_name_nothing_running_claims_reads_as_zero_in_use() {
        let concurrency_limits = BTreeMap::from([("idle_limit".to_string(), 4)]);
        let claimed_limit_slots = BTreeMap::new();

        let rows = limit_rows(32, 0, &concurrency_limits, &claimed_limit_slots);

        let idle = rows.iter().find(|row| row.name == "idle_limit").unwrap();
        assert_eq!(idle.in_use, 0);
    }

    /// A limit configured `0` means unlimited, not zero slots - a prior review caught this
    /// explained backwards, so it gets its own boundary test rather than trusting the
    /// general case.
    #[test]
    fn a_zero_max_is_never_full_no_matter_how_much_is_in_use() {
        let row = LimitRow { name: "disabled".to_string(), in_use: 9, max: 0 };

        assert!(!is_full(&row));
    }

    #[test]
    fn in_use_at_a_non_zero_max_is_full() {
        let row = LimitRow { name: "warehouse".to_string(), in_use: 3, max: 3 };

        assert!(is_full(&row));
    }

    /// One below the max is the boundary an off-by-one (`in_use >= max - 1`) would get
    /// wrong: 4 of 5 must not read as full.
    #[test]
    fn one_below_a_non_zero_max_is_not_full() {
        let row = LimitRow { name: "openai_api".to_string(), in_use: 4, max: 5 };

        assert!(!is_full(&row));
    }
}
