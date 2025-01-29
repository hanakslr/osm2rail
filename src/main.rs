use std::collections::{HashMap, HashSet};

use osmpbf::{Element, ElementReader};

#[derive(Debug)]
pub struct OsmRailway {
    // Represent the railway from OSM - moderately untransformed, without any splitting done.
    name: String,

    way_id: i64,

    node_ids: Vec<i64>, // A list of the node_ids that make up this way.
}

impl OsmRailway {
    pub fn from_osm_way(way: osmpbf::Way) -> OsmRailway {
        let way_name = match way.tags().find(|(key, _)| *key == "name") {
            Some(res) => res.1.to_string(),
            _ => way.id().to_string(),
        };

        OsmRailway {
            name: way_name,
            way_id: way.id(),
            node_ids: way.raw_refs().to_owned(),
        }
    }

    pub fn get_used_node_counts(railways: &Vec<OsmRailway>) -> HashMap<i64, i64> {
        // Iterate over all of the railways and find make a hashap of nodes and the number
        // of times they are used.
        let mut node_use_count = HashMap::new();
        for railway in railways.iter() {
            for node_id in railway.node_ids.iter() {
                let count = node_use_count.entry(*node_id).or_insert(0);
                *count += 1;
            }
        }

        node_use_count
    }

    pub fn split_at_intersections(self, intersection_nodes: &HashSet<i64>) -> Vec<OsmRailway> {
        // Take ownership of self, and if needed split it into multiple selfs on the used node ids.

        // Iterate over our nodes - but we never want to split on the first index
        let mut index = 0;
        let mut previous_node = None;
        let split_railways: Vec<OsmRailway> = self
            .node_ids
            .split_inclusive(|n| {
                let should_split = index != 0 && intersection_nodes.contains(n);
                index += 1;
                should_split
            })
            .map(|nodes| {
                // Our node splits are
                let mut node_ids = nodes.to_owned();

                // If there is a previous_node, add it at the beginning of the list
                if let Some(previous_node) = previous_node {
                    node_ids.insert(0, previous_node);
                }

                previous_node = nodes.last().cloned();
                OsmRailway {
                    name: self.name.clone(),
                    way_id: self.way_id,
                    node_ids,
                }
            })
            .collect();

        split_railways
    }
}

struct OsmNode {
    // Represents a single OsmNode. Keyed by node_id - these ids correspond to the values of OsmRailway.nodes.
    // Our library can exapnd the Way out into lat/longs directly, but it will be more efficient to split Railways into
    // segments based on intersecting node_id than lat/long.
    node_id: i64,
    latitude: f64,
    longitude: f64,
}

fn collect_nodes(node_ids: Vec<i64>) -> Vec<OsmNode> {
    todo!()
}

/// Read an OSM file and parse out all of the railways.
fn collect_all_railways() -> Vec<OsmRailway> {
    let reader =
        ElementReader::from_path("./osm_data/us-latest.osm.pbf").expect("Could not load data");

    // Iterate over every element, find the ways - create a Vec of OsmRailway.
    // This particular reader requires that the map_op return the same type as the reduce_op - instead of a reduce function that
    // takes a previous value. This makes sense given the parallelization I think, but it mean that I make these intermediary, single
    // element vecs.
    let railways = reader
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

                            Some(vec![OsmRailway {
                                name: way_name,
                                way_id: way.id(),
                                node_ids: way.raw_refs().to_owned(),
                            }])
                        }
                        false => None,
                    }
                }
                _ => None,
            },
            || None,
            |a_railways, b_railways| match (a_railways, b_railways) {
                (res @ Some(_), None) => res,
                (None, res @ Some(_)) => res,
                (Some(r1), Some(r2)) => Some(r1.into_iter().chain(r2).collect()),
                _ => None,
            },
        )
        .expect("Error loading OsmRailways");

    match railways {
        None => println!("None found"),
        Some(ref r) => {
            let num_ways = r.len();
            println!("Number of railways {num_ways}");
        }
    }

    railways.unwrap_or(Vec::new())
}

/// Given a vec of OsmRailways, break at the nodes that are seen in other railways.
/// Return a vec of OsmRailways that are appropriately segmented, and a HashSet of the used
/// node ids.
fn segment_railways(railways: Vec<OsmRailway>, intersectons: HashSet<i64>) -> Vec<OsmRailway> {
    let segmented_railways: Vec<OsmRailway> = railways
        .into_iter()
        .flat_map(|railway| railway.split_at_intersections(&intersectons))
        .collect();

    let num_segmented_railways = segmented_railways.len();
    println!("Number of segmented railways: {num_segmented_railways}");

    segmented_railways
}

fn count_all_railways() {
    // This file is downloaded from Geofabrik but is 1.5gb so it is not in source control
    let reader =
        ElementReader::from_path("./osm_data/us-latest.osm.pbf").expect("Couldn't load data.");

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
    let railways = collect_all_railways();
    let node_counts = OsmRailway::get_used_node_counts(&railways);

    let intersections = node_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(node_id, _)| *node_id) // Extract keys
        .collect();

    segment_railways(railways, intersections);

    let nodes = collect_nodes(node_counts.into_keys().collect());

    {}
}

#[test]
fn test_segment_railways() {
    // Given a simple vec of railways, break it up
    let railways = Vec::from([
        OsmRailway {
            name: "way 1".to_string(),
            way_id: 1,
            node_ids: Vec::from([1, 2, 3, 6, 8, 9]),
        },
        OsmRailway {
            name: "way 2".to_string(),
            way_id: 2,
            node_ids: [2, 3].to_vec(),
        },
        OsmRailway {
            name: "way 3".to_string(),
            way_id: 3,
            node_ids: [4, 5, 6, 7].to_vec(),
        },
    ]);

    let node_counts = OsmRailway::get_used_node_counts(&railways);

    let intersections = node_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(node_id, _)| *node_id) // Extract keys
        .collect();

    let segmented_railways = segment_railways(railways, intersections);

    assert_eq!(7, segmented_railways.len(), "{:?}", segmented_railways);
}
