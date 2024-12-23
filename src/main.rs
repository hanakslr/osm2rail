use std::collections::HashMap;

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
    is_crossing: bool,
}

fn collect_all_railways() {
    let reader = ElementReader::from_path("./osm_data/us-northeast-latest.osm.pbf")
        .expect("Could not load data");

    // Iterate over every element, find the ways - create a Vec of OsmRailway.
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
                        is_crossing: node
                            .tags()
                            .find(|(k, v)| *k == "railway" && *v == "crossing")
                            .is_some(),
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
            println!("Number of nodes {num_nodes}");

            // Also print the number that are crossings
            let num_crossings = r.iter().filter(|n| n.is_crossing).count();
            println!("Number of crossings {num_crossings}")
        }
    }
}

/// Given a vec of OsmRailways, we want to reduce to a graph of railway segments that
/// are broken up at nodes that are deemed crossings.
fn segment_railways_at_crossings(
    railways: Vec<OsmRailway>,
    nodes: Vec<OsmNode>,
) -> Vec<OsmRailway> {
    let mut crossing_node_mapping = HashMap::new();

    for n in nodes.iter().filter(|n| n.is_crossing) {
        crossing_node_mapping.insert(n.node_id, n);
    }

    let mut unprocessed_railways = railways;
    let mut segmented_railways = Vec::new();

    while unprocessed_railways.len() > 0 {
        let mut r = unprocessed_railways.pop().unwrap();
        let way_id = r.way_id;
        println!("Way id: {way_id}");

        // Iterate over each node, and i
        for (index, railway_node) in r.nodes.iter().enumerate() {
            if crossing_node_mapping.contains_key(&railway_node) {
                // Split up this railway - we are moving through the nodes sucessively, so we will split where we are, and
                // make a new railway, being done with the current one.
                let splice = r.nodes.split_off(index);

                // Make a new railway -
                unprocessed_railways.push(OsmRailway {
                    name: r.name.clone(),
                    way_id: r.way_id.clone(),
                    nodes: splice,
                });

                segmented_railways.push(r);
                break;
            }
        }
    }

    segmented_railways
}

fn main() {
    collect_all_railways();
}

#[test]
fn test_segment_railways() {
    // Given a simple vec of railways, break it up
    let railways = Vec::from([
        OsmRailway {
            name: "way 1".to_string(),
            way_id: 1,
            nodes: Vec::from([1, 2, 3, 6, 8, 9]),
        },
        OsmRailway {
            name: "way 2".to_string(),
            way_id: 2,
            nodes: [2, 3].to_vec(),
        },
        OsmRailway {
            name: "way 3".to_string(),
            way_id: 3,
            nodes: [4, 5, 6, 7].to_vec(),
        },
    ]);

    let mut nodes = Vec::new();
    let crossings = [2, 6];

    for i in 1..9 {
        nodes.push(OsmNode {
            node_id: i,
            latitude: -44.00 + i as f64, // this is a dummy value
            longitude: 77.0 + i as f64,  // this is a dummy value
            is_crossing: crossings.contains(&i),
        })
    }

    let segmented_railways = segment_railways_at_crossings(railways, nodes);

    assert_eq!(6, segmented_railways.len());

    // Add a check for the nodes.
    // Should have
    // [1,2], [2,3], [2,6], [4,5,6], [6,7], [6,8,9]
}
