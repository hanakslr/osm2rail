use osmpbf::{Element, ElementReader};

pub struct OsmRailway {
    // Represent the railway from OSM - moderately untransformed, without any splitting done.
    name: String,

    way_id: i64,

    nodes: Vec<i64>, // A list of the node_ids that make up this way.
}

struct OsmNode {
    // Represents a single OsmNode. Keyed by node_id - these ids correspond to the values of OsmRailway.nodes.
    // Our library can exapnd the Way out into lat/longs directly, but it will be more efficient to split Railways into
    // segments based on intersecting node_id than lat/long.
    node_id: i64,
    latitude: f64,
    longitude: f64,
}

fn collect_all_railways() {
    let reader = ElementReader::from_path("./osm_data/us-northeast-latest.osm.pbf")
        .expect("Could not load data");

    // Iterate ove every element, find the ways - create a Vec of OsmRailway.
    // This particular reader requires that the map_op return the same type as the reduce_op - instead of a reduce function that
    // takes a previous value. This makes sense given the parallelization I think, but it mean that I make these intermediary, single
    // element vecs.
    let (railways, nodes) = reader
        .par_map_reduce(
            |element| match element {
                Element::Way(way) => {
                    match way
                        .tags()
                        .any(|(key, value)| key == "railway" && value == "rail")
                    {
                        true => {
                            let way_name = match way.tags().find(|(key, _)| *key == "name") {
                                Some(res) => res.1.to_string(),
                                _ => way.id().to_string(),
                            };

                            let railway = OsmRailway {
                                name: way_name,
                                way_id: way.id(),
                                nodes: way.raw_refs().to_vec(),
                            };

                            (Some(vec![railway]), None)
                        }
                        false => (None, None),
                    }
                }
                Element::DenseNode(node) => (
                    None,
                    Some(vec![OsmNode {
                        node_id: node.id(),
                        latitude: node.lat(),
                        longitude: node.lon(),
                    }]),
                ),
                _ => (None, None),
            },
            || (None, None),
            |(a_railways, a_nodes), (b_railways, b_nodes)| {
                // Get our rails and nodes from a
                let mut r = a_railways.unwrap_or_else(Vec::new);
                let mut n = a_nodes.unwrap_or_else(Vec::new);

                if let Some(mut b_railways) = b_railways {
                    r.append(&mut b_railways);
                }

                if let Some(mut b_nodes) = b_nodes {
                    n.append(&mut b_nodes);
                }

                (Some(r), Some(n))
            },
        )
        .expect("Error loading OsmRailways");

    match railways {
        None => println!("None found"),
        Some(r) => {
            let num_ways = r.len();
            println!("Number of railways {num_ways}");
        }
    }

    match nodes {
        None => println!("No nodes found"),
        Some(r) => {
            let num_nodes = r.len();
            println!("Number of nodes {num_nodes}")
        }
    }
}

fn count_all_railways() {
    // This file is downloaded from Geofabrik but is 1.5gb so it is not in source control
    let reader = ElementReader::from_path("./osm_data/us-northeast-latest.osm.pbf")
        .expect("Couldn't load data.");

    // Count the railways
    let ways = reader
        .par_map_reduce(
            |element| match element {
                Element::Way(way) => {
                    match way
                        .tags()
                        .any(|(key, value)| key == "railway" && value == "rail")
                    {
                        true => 1,
                        _ => 0,
                    }
                }
                _ => 0,
            },
            || 0_u64,     // Zero is the identity value for addition
            |a, b| a + b, // Sum the partial results
        )
        .expect("Error counting.");

    println!("Number of ways: {ways}");
}

fn main() {
    collect_all_railways();
}
