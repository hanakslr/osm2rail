use osm2rail::{collect_all_railways, collect_nodes, segment_railways};

mod reader;

fn main() {
    let file = "./osm_data/railways-in-us-northeast.osm.pbf";
    let node_metadata = collect_nodes(file);
    let railways = collect_all_railways(file);
    segment_railways(railways);
}
