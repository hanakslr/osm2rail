use std::collections::HashMap;
use std::fs::File;

use serde_json;

use osm2rail::{collect_all_railways, collect_nodes, HasTags};

const FILE_NAME: &str = "./osm_data/railways-in-us.osm.pbf";

/// Return a mapping of all of the keys: {value: count} that are found in all of the tags of the
/// provided railways.
pub fn get_used_tags<T: HasTags>(
    elements: &Vec<T>,
    threshold: Option<i64>,
) -> HashMap<String, HashMap<String, i64>> {
    let mut used_tags: HashMap<String, HashMap<String, i64>> = HashMap::new();

    for elem in elements.iter() {
        for (k, v) in elem.tags() {
            let existing_vals = used_tags.entry(k.clone()).or_insert(HashMap::new());
            let count = existing_vals.entry(v.clone()).or_insert(0);
            *count += 1;
        }
    }

    match threshold {
        None => used_tags,
        Some(t) => {
            used_tags.retain(|_, value_map| {
                value_map.retain(|_, count| *count >= t);
                !value_map.is_empty() // Remove key if all its values were filtered out
            });
            used_tags
        }
    }
}

fn main() {
    let railways = collect_all_railways(FILE_NAME);
    let nodes = collect_nodes(FILE_NAME);
    let used_railway_tags = get_used_tags(&railways, Some(100));
    let used_node_tags = get_used_tags(&nodes.into_values().collect(), Some(20));

    // Write to a file
    serde_json::to_writer_pretty(
        File::create("output/railway_tags.json").unwrap(),
        &used_railway_tags,
    )
    .unwrap();

    serde_json::to_writer_pretty(
        File::create("output/node_tags.json").unwrap(),
        &used_node_tags,
    )
    .unwrap();
}
