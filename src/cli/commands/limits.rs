use clap::Args;
use crate::crud::CRUD;
use crate::router::app::limits;
use crate::toolkit::Toolkit;


#[derive(Args)]
pub struct LimitsCmd {
    /// Print the answer as JSON, for a script or a supervisor.
    #[arg(long)]
    pub json: bool,
}


impl LimitsCmd {

    /// Reads `config.toml` for the maxima and the disk tables for the counts, so it
    /// answers for a data directory whose server is down as readily as one whose server
    /// is up - the same reason `status` reads the lock file rather than the database.
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        let max_running_attempts = toolkit.app_config.orchestrator.max_running_attempts;
        let concurrency_limits = toolkit.app_config.concurrency_limits.clone();

        let mut conn = toolkit.get_conn().await?;
        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        let running_attempts = crud.count_running_attempts(&mut conn).await?;
        let claimed_limit_slots = crud.claimed_limit_slots(&mut conn).await?;

        let rows = limits::limit_rows(
            max_running_attempts,
            running_attempts,
            &concurrency_limits,
            &claimed_limit_slots,
        );

        if self.json {
            println!("{}", limits_json(&rows));
        } else {
            println!("{}", limits_table(&rows));
        }

        Ok(())
    }
}


/// A limit configured `0` never reaches `FULL` and prints its max as `-`: throughout this
/// codebase `0` means no ceiling at all - the dispatcher skips the check entirely for it
/// (`max_running_attempts > 0` and `configured_max == 0` in
/// `task_run_attempt_dispatcher.rs`) - so there is no maximum for anything to be full
/// against.
pub fn limits_table(rows: &[limits::LimitRow]) -> String {

    let mut lines = vec![format!("{:<12} {:>6} {:>4}", "NAME", "IN USE", "MAX")];

    for row in rows {

        let max_display = if row.max == 0 { "-".to_string() } else { row.max.to_string() };

        let mut line = format!("{:<12} {:>6} {:>4}", row.name, row.in_use, max_display);

        if limits::is_full(row) {
            line.push_str("  FULL");
        }

        lines.push(line);
    }

    lines.join("\n")
}


/// No envelope, matching the rule the `--json` cut established: a redirected file holds
/// rows or nothing, never a wrapper a script has to unwrap first. `max` stays the
/// configured `0` here - the dash is a human rendering only.
pub fn limits_json(rows: &[limits::LimitRow]) -> serde_json::Value {
    serde_json::Value::Array(
        rows.iter()
            .map(|row| serde_json::json!({
                "name": row.name,
                "in_use": row.in_use,
                "max": row.max,
            }))
            .collect()
    )
}


#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use crate::router::app::limits::{LimitRow, limit_rows};
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

    fn sample_rows() -> Vec<LimitRow> {
        vec![
            LimitRow { name: "global".to_string(), in_use: 32, max: 32 },
            LimitRow { name: "warehouse".to_string(), in_use: 3, max: 3 },
            LimitRow { name: "openai_api".to_string(), in_use: 1, max: 5 },
        ]
    }

    #[test]
    fn the_table_marks_full_only_the_rows_at_their_non_zero_max() {
        let table = limits_table(&sample_rows());

        let global_line = table.lines().find(|line| line.starts_with("global")).unwrap();
        let warehouse_line = table.lines().find(|line| line.starts_with("warehouse")).unwrap();
        let openai_line = table.lines().find(|line| line.starts_with("openai_api")).unwrap();

        assert!(global_line.ends_with("FULL"), "{global_line}");
        assert!(warehouse_line.ends_with("FULL"), "{warehouse_line}");
        assert!(!openai_line.ends_with("FULL"), "{openai_line}");
    }

    /// A limit configured `0` means unlimited, not zero slots - the dispatcher skips the
    /// check entirely for it - so there is no maximum to be full against and the table
    /// prints a dash instead.
    #[test]
    fn a_zero_max_prints_a_dash_and_is_never_full() {
        let rows = vec![LimitRow { name: "disabled".to_string(), in_use: 0, max: 0 }];

        let table = limits_table(&rows);
        let line = table.lines().find(|line| line.starts_with("disabled")).unwrap();

        assert!(line.contains('-'), "{line}");
        assert!(!line.ends_with("FULL"), "{line}");
    }

    #[test]
    fn in_use_below_a_non_zero_max_is_not_full() {
        let rows = vec![LimitRow { name: "openai_api".to_string(), in_use: 1, max: 5 }];

        let table = limits_table(&rows);
        assert!(!table.contains("FULL"), "{table}");
    }

    /// One below the max is the boundary an off-by-one (`in_use >= max - 1`) would get
    /// wrong: 4 of 5 must not read as FULL.
    #[test]
    fn one_below_a_non_zero_max_is_not_full() {
        let rows = vec![LimitRow { name: "openai_api".to_string(), in_use: 4, max: 5 }];

        let table = limits_table(&rows);
        assert!(!table.contains("FULL"), "{table}");
    }

    #[test]
    fn json_has_no_envelope_and_keeps_a_zero_max_as_a_number() {
        let rows = vec![
            LimitRow { name: "global".to_string(), in_use: 32, max: 32 },
            LimitRow { name: "disabled".to_string(), in_use: 0, max: 0 },
        ];

        let json = limits_json(&rows);

        assert!(json.is_array());
        assert_eq!(json[0]["name"], "global");
        assert_eq!(json[0]["in_use"], 32);
        assert_eq!(json[0]["max"], 32);
        assert_eq!(json[1]["max"], 0);
    }
}
