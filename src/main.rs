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
                    }]),
                ),
                _ => (None, None),
            },
            || (None, None),
            |(a_railways, a_nodes), (b_railways, b_nodes)| {
                // Get our rails and nodes from a
                let railways = match (a_railways, b_railways) {
                    (res @ Some(_), None) => res,
                    (None, res @ Some(_)) => res,
                    (Some(r1), Some(r2)) => Some(r1.into_iter().chain(r2).collect()),
                    _ => None,
                };

                let nodes = match (a_nodes, b_nodes) {
                    (res @ Some(_), None) => res,
                    (None, res @ Some(_)) => res,
                    (Some(r1), Some(r2)) => Some(r1.into_iter().chain(r2).collect()),
                    _ => None,
                };

                (railways, nodes)
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

/// Given a vec of OsmRailways, we want to reduce to a graph of railway segments that
/// are broken up at interesections from other segments. That each segments start_node and end_node
/// should either be a terminals or contain an intersection with another segment
fn segment_railways(railways: Vec<OsmRailway>) -> Vec<OsmRailway> {
    // Make a mapping of node_id to the OsmRailway and index that it is first seen at
    // If a node_id already exists in the mapping - split that Railway into 2 segments, as well as the new one
    let mut split_railways = Vec::new();
    let previous_nodes = HashMap::<i64, (&OsmRailway, usize)>::new();

    for railway in railways {
        // iterate over the nodes - check to see if the node_id is in the hashmap
        for (index, n) in railway.nodes.iter().enumerate() {
            if previous_nodes.contains_key(n) {
                // This node exists in another segment, which means we need to break up that segment AND
                // this is the end of this segment.
                //let new_slice = railway.nodes.split_off(index);
                split_railways.push(OsmRailway {
                    name: railway.name.clone(),
                    way_id: railway.way_id,
                    nodes: Vec::new(),
                })
            } else {
                // We haven't seen this node before - add it to the mapping so we know that it has been used
            }
        }
    }

    Vec::new()
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

    let segmented_railways = segment_railways(railways);

    assert_eq!(6, segmented_railways.len());

    // Add a check for the nodes.
    // Should have
    // [1,2], [2,3], [2,6], [4,5,6], [6,7], [6,8,9]
}
