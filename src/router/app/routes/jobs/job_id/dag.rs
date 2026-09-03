use std::collections::{BTreeMap, HashMap};

use crate::crud::task::Task;

const NODE_WIDTH: f64 = 176.0;
const NODE_HEIGHT: f64 = 36.0;
const COLUMN_GAP: f64 = 72.0;
const ROW_GAP: f64 = 20.0;
const LABEL_MAX_CHARS: usize = 22;

pub struct DagNode {
    pub label: String,
    pub x: f64,
    pub y: f64,
    pub center_x: f64,
    pub center_y: f64,
}

pub struct DagEdge {
    pub path: String,
}

pub struct Dag {
    pub nodes: Vec<DagNode>,
    pub edges: Vec<DagEdge>,
    pub node_width: f64,
    pub node_height: f64,
    pub width: f64,
    pub height: f64,
}

/// Each task sits one column past its deepest dependency. The relaxation is bounded by the
/// task count so a config with a dependency cycle still terminates instead of spinning.
fn columns_by_task(tasks: &[Task]) -> HashMap<&str, usize> {
    let mut columns: HashMap<&str, usize> = tasks.iter()
        .map(|task| (task.task_id.as_str(), 0))
        .collect();

    for _ in 0..tasks.len() {
        let mut changed = false;

        for task in tasks {
            let deepest_dependency = task.depends_on.0.iter()
                .filter_map(|dependency| columns.get(dependency.as_str()))
                .max()
                .copied();

            let Some(deepest_dependency) = deepest_dependency else { continue };

            if columns[task.task_id.as_str()] < deepest_dependency + 1 {
                columns.insert(task.task_id.as_str(), deepest_dependency + 1);
                changed = true;
            }
        }

        if !changed {
            break;
        }
    }

    columns
}

fn truncate(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }

    let kept: String = text.chars().take(max_chars - 1).collect();

    format!("{}…", kept)
}

pub fn build(tasks: &[Task]) -> Dag {
    if tasks.is_empty() {
        return Dag {
            nodes: Vec::new(),
            edges: Vec::new(),
            node_width: NODE_WIDTH,
            node_height: NODE_HEIGHT,
            width: 0.0,
            height: 0.0,
        };
    }

    let columns = columns_by_task(tasks);

    let mut tasks_by_column: BTreeMap<usize, Vec<&Task>> = BTreeMap::new();
    for task in tasks {
        tasks_by_column.entry(columns[task.task_id.as_str()]).or_default().push(task);
    }

    let tallest_column = tasks_by_column.values().map(|column| column.len()).max().unwrap_or(0);

    let content_width = tasks_by_column.len() as f64 * (NODE_WIDTH + COLUMN_GAP) - COLUMN_GAP;
    let content_height = tallest_column as f64 * (NODE_HEIGHT + ROW_GAP) - ROW_GAP;

    let mut positions: HashMap<&str, (f64, f64)> = HashMap::new();

    for (column_index, column) in tasks_by_column.values().enumerate() {
        let column_height = column.len() as f64 * (NODE_HEIGHT + ROW_GAP) - ROW_GAP;
        let top = (content_height - column_height) / 2.0;

        for (row_index, task) in column.iter().enumerate() {
            let x = column_index as f64 * (NODE_WIDTH + COLUMN_GAP);
            let y = top + row_index as f64 * (NODE_HEIGHT + ROW_GAP);

            positions.insert(task.task_id.as_str(), (x, y));
        }
    }

    let mut edges = Vec::new();

    for task in tasks {
        let (to_x, to_y) = positions[task.task_id.as_str()];

        for dependency in task.depends_on.0.iter() {
            let Some(&(from_x, from_y)) = positions.get(dependency.as_str()) else { continue };

            let start_x = from_x + NODE_WIDTH;
            let start_y = from_y + NODE_HEIGHT / 2.0;
            let end_y = to_y + NODE_HEIGHT / 2.0;
            let bend = (to_x - start_x) / 2.0;

            edges.push(DagEdge {
                path: format!(
                    "M {} {} C {} {} {} {} {} {}",
                    start_x, start_y,
                    start_x + bend, start_y,
                    to_x - bend, end_y,
                    to_x, end_y,
                ),
            });
        }
    }

    let nodes = tasks.iter().map(|task| {
        let (x, y) = positions[task.task_id.as_str()];

        DagNode {
            label: truncate(&task.task_id, LABEL_MAX_CHARS),
            x,
            y,
            center_x: x + NODE_WIDTH / 2.0,
            center_y: y + NODE_HEIGHT / 2.0,
        }
    }).collect();

    Dag {
        nodes,
        edges,
        node_width: NODE_WIDTH,
        node_height: NODE_HEIGHT,
        // A pixel of slack on each side keeps the node strokes off the clip edge.
        width: content_width + 2.0,
        height: content_height + 2.0,
    }
}
