use std::sync::Arc;
use chrono::{DateTime, Utc};
use crate::crud::CRUD;
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter, UpdateTaskRunAttemptsDataInput};


/// Settles the attempts a previous run of the program left `Running`, killing the process
/// groups it can prove are still theirs.
///
/// Runs once, before any poller: `TaskRunAttemptChildren` is empty at startup, so every
/// `Running` attempt belongs to a process this program does not hold. Left to the pollers
/// instead, `TaskRunAttemptMonitor` would settle these rows without ever reading the group
/// id, and the commands would go on running.
pub async fn recover_orphaned_task_run_attempts(
    crud: &Arc<CRUD>,
    conn_pool: &Arc<sqlx::SqlitePool>,
    system_boot_time: Option<DateTime<Utc>>,
) -> anyhow::Result<()> {

    let task_run_attempts = crud.select_task_run_attempts(
        &**conn_pool,
        &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: None,
                task_id: None,
                status: Some(TaskRunAttemptStatus::Running),
            },
            sort: Some(SelectTaskRunAttemptsDataSort::Id),
        },
    ).await?;

    for task_run_attempt in task_run_attempts {

        let killed = kill_orphan_process_group(&task_run_attempt, system_boot_time);

        match killed {
            true => eprintln!(
                "Task run attempt {} was left running by an earlier run of flowlite; killed \
                 its process group {} and settled it invalid, since what its command had \
                 done by then is unknown",
                task_run_attempt.id,
                task_run_attempt.process_group_id.unwrap_or_default(),
            ),
            false => eprintln!(
                "Task run attempt {} was left running by an earlier run of flowlite and has \
                 been settled invalid. Its process group could not be identified, so its \
                 command may still be running.",
                task_run_attempt.id,
            ),
        }

        crud.update_task_run_attempts(
            &**conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: Some(TaskRunAttemptStatus::Invalid),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                    process_group_id: None,
                },
            },
        ).await?;
    }

    Ok(())
}

/// Kills the attempt's process group, and reports whether it did.
///
/// **The guard is against pid reuse, and it refuses rather than guesses.** A group id is
/// just a number, and the kernel hands the same one out again; killing blind could signal
/// something else's process tree, which is a far worse outcome than a leaked command. So a
/// kill happens only where the number cannot have been recycled: the attempt must have
/// started after the machine last booted, since a pid from before it belongs to nothing
/// this program ever spawned. An unknown boot time or start instant kills nothing.
fn kill_orphan_process_group(
    task_run_attempt: &TaskRunAttempt,
    system_boot_time: Option<DateTime<Utc>>,
) -> bool {

    let Some(process_group_id) = task_run_attempt.process_group_id else {
        return false;
    };

    let (Some(started_at), Some(system_boot_time)) = (task_run_attempt.started_at, system_boot_time) else {
        return false;
    };

    if started_at < system_boot_time {
        return false;
    }

    // Safe: killpg only delivers a signal, and a group that has already gone reports ESRCH.
    let killed = unsafe { libc::killpg(process_group_id as i32, libc::SIGKILL) };

    killed == 0
}

/// When the machine last booted, or None where that cannot be read — which makes the guard
/// above refuse every kill rather than trust a number it cannot date.
pub fn system_boot_time() -> Option<DateTime<Utc>> {

    #[cfg(target_os = "linux")]
    {
        // /proc/stat's btime line is the boot instant as a unix timestamp.
        let stat = std::fs::read_to_string("/proc/stat").ok()?;

        let btime = stat
            .lines()
            .find_map(|line| line.strip_prefix("btime "))?
            .trim()
            .parse::<i64>()
            .ok()?;

        DateTime::from_timestamp(btime, 0)
    }

    #[cfg(target_os = "macos")]
    {
        let mut boot_time = libc::timeval { tv_sec: 0, tv_usec: 0 };
        let mut size = std::mem::size_of::<libc::timeval>();
        let mut mib = [libc::CTL_KERN, libc::KERN_BOOTTIME];

        // Safe: sysctl writes at most `size` bytes into a timeval this call owns.
        let read = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                mib.len() as u32,
                &mut boot_time as *mut libc::timeval as *mut libc::c_void,
                &mut size,
                std::ptr::null_mut(),
                0,
            )
        };

        if read != 0 {
            return None;
        }

        DateTime::from_timestamp(boot_time.tv_sec, 0)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::test_support::{has_exited, read_pid_file, reading_the_environment, TestDb};
    use chrono::TimeDelta;
    use std::os::unix::process::CommandExt;

    /// Spawns a process group the way the dispatcher does — a group leader with a worker
    /// under it — and reports the leader to kill by and the worker to watch.
    ///
    /// Every caller holds `reading_the_environment()` for as long as it does: this is a
    /// real spawn, and a spawned process reads `environ` at exec, which is undefined
    /// behaviour beside another test's `set_var`. Nothing here reads a variable, but the
    /// exec does — see `src/test_support.rs` on why that lock is not about intent.
    ///
    /// The assertion is on the **worker**, not the leader: the leader is this test's own
    /// child and nothing reaps it, so it lingers as a zombie that `kill(pid, 0)` still
    /// answers for. The worker is a grandchild, reparented and reaped when its parent dies.
    async fn spawn_orphan(pid_file: &std::path::Path) -> (std::process::Child, i32) {

        let child = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!("sleep 30 & echo $! > {}; wait", pid_file.display()))
            .process_group(0)
            .spawn()
            .unwrap();

        let worker = read_pid_file(pid_file).await;

        (child, worker)
    }

    /// The whole point: a command a crash left running is killed, so the invalid the row
    /// settles as is not also a leaked process.
    #[tokio::test]
    async fn an_orphaned_attempt_is_killed_and_settled_invalid() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let (mut leader, worker) = spawn_orphan(&db.data_dir().join("pid")).await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        db.orphan_task_run_attempt(task_run_attempt.id, leader.id() as i64, Utc::now()).await;

        recover_orphaned_task_run_attempts(
            &db.crud,
            &db.conn_pool,
            Some(Utc::now() - TimeDelta::hours(1)),
        ).await.unwrap();

        assert!(has_exited(worker).await, "the orphan was left running");

        let _ = leader.wait();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Invalid,
        );
    }

    /// A row from before the column existed has no group to kill, and still has to settle.
    #[tokio::test]
    async fn an_attempt_with_no_process_group_is_settled_anyway() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        recover_orphaned_task_run_attempts(&db.crud, &db.conn_pool, Some(Utc::now())).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Invalid,
        );
    }

    /// The pid-reuse guard. An attempt that started before the machine last booted cannot
    /// own the process that holds its group id now — the number was recycled — so the kill
    /// is skipped and something else's process tree is left alone.
    #[tokio::test]
    async fn a_group_id_from_before_the_last_boot_is_not_killed() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let (mut leader, worker) = spawn_orphan(&db.data_dir().join("pid")).await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        db.orphan_task_run_attempt(
            task_run_attempt.id,
            leader.id() as i64,
            Utc::now() - TimeDelta::hours(2),
        ).await;

        recover_orphaned_task_run_attempts(
            &db.crud,
            &db.conn_pool,
            Some(Utc::now() - TimeDelta::hours(1)),
        ).await.unwrap();

        assert!(
            unsafe { libc::kill(worker, 0) } == 0,
            "a process group that predates the boot was killed anyway",
        );

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Invalid,
        );

        unsafe { libc::killpg(leader.id() as i32, libc::SIGKILL) };
        let _ = leader.wait();
    }

    /// Unknown boot time means the guard cannot be applied, so nothing is killed.
    #[tokio::test]
    async fn an_unknown_boot_time_kills_nothing() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let (mut leader, worker) = spawn_orphan(&db.data_dir().join("pid")).await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        db.orphan_task_run_attempt(task_run_attempt.id, leader.id() as i64, Utc::now()).await;

        recover_orphaned_task_run_attempts(&db.crud, &db.conn_pool, None).await.unwrap();

        assert!(unsafe { libc::kill(worker, 0) } == 0, "killed without being able to check");

        unsafe { libc::killpg(leader.id() as i32, libc::SIGKILL) };
        let _ = leader.wait();
    }
}
