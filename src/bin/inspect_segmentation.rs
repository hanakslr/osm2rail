use osm2rail::{collect_all_railways, collect_nodes, segment_railways};
use serde::{Deserialize, Serialize};
use std::fs::File;

const FILE_NAME: &str = "./osm_data/railways-in-us-northeast.osm.pbf";

#[derive(Serialize, Deserialize)]
struct SegmentStats {
    id: i64,
    distance: f64,
    node_ids: Vec<i64>,
    num_nodes: usize,
}
fn main() {
    let railways = collect_all_railways(FILE_NAME)
        .into_iter()
        .take(100)
        .collect();

    let nodes = collect_nodes(FILE_NAME);

    let segmented_railways = segment_railways(railways);

    let details: Vec<SegmentStats> = segmented_railways
        .iter()
        .map(|s| SegmentStats {
            id: s.way_id,
            node_ids: s.node_ids.clone(),
            distance: s.get_distance(&nodes),
            num_nodes: s.node_ids.len(),
        })
        .collect();

    println!("Collect details for nodes. Outputting.");

    serde_json::to_writer_pretty(File::create("output/segments.json").unwrap(), &details).unwrap();
}
