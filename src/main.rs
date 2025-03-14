use osm2rail::{collect_all_railways, collect_nodes, segment_railways};
use writer::write_to_pg;

mod reader;
mod writer;

#[tokio::main]
async fn main() {
    let file = "./osm_data/railways-in-us-northeast.osm.pbf";
    // let node_metadata = collect_nodes(file);
    // let railways = collect_all_railways(file);
    // let segments = segment_railways(railways);

    let database_url = "postgresql://admin:pw@localhost:5432/db";
    match write_to_pg(database_url).await {
        Ok(_) => println!("Database operations completed successfully"),
        Err(e) => eprintln!("Error: {}", e),
    }
}
