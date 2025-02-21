use std::collections::HashMap;
use std::fs::File;

use serde_json;

use osm2rail::{collect_all_railways, OsmRailway};

const FILE_NAME: &str = "./osm_data/railways-in-us.osm.pbf";

/// Return a mapping of all of the keys: {value: count} that are found in all of the tags of the
/// provided railways.
pub fn get_used_tags(railways: &Vec<OsmRailway>) -> HashMap<String, HashMap<String, i64>> {
    let mut used_tags: HashMap<String, HashMap<String, i64>> = HashMap::new();

    for railway in railways.iter() {
        for (k, v) in &railway.tags {
            let existing_vals = used_tags.entry(k.clone()).or_insert(HashMap::new());
            let count = existing_vals.entry(v.clone()).or_insert(0);
            *count += 1;
        }
    }

    used_tags
}

fn main() {
    let railways = collect_all_railways(FILE_NAME);
    let mut used_tags = get_used_tags(&railways);

    // Filter out entries with counts < 40
    used_tags.retain(|_, value_map| {
        value_map.retain(|_, count| *count >= 100);
        !value_map.is_empty() // Remove key if all its values were filtered out
    });

    // Write to a file
    let file = File::create("railway_tags.json").unwrap();
    serde_json::to_writer_pretty(file, &used_tags).unwrap();
}
