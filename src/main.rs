use osmpbf::{Element, ElementReader};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::Read;

const FILE_NAME: &str = "./osm_data/us-northeast-latest.osm.pbf";

trait Filter {
    fn collect_filtered<T, F>(path: &str, filter_map: F) -> Result<Vec<T>, osmpbf::Error>
    where
        F: Fn(Element) -> Option<T> + Send + Sync,
        T: Send;
}

impl<R: Read + Send> Filter for ElementReader<R> {
    fn collect_filtered<T, F>(path: &str, filter_map: F) -> Result<Vec<T>, osmpbf::Error>
    where
        F: Fn(Element) -> Option<T> + Send + Sync,
        T: Send,
    {
        let reader = ElementReader::from_path(path)?;
        reader
            .par_map_reduce(
                |element| match filter_map(element) {
                    Some(item) => Some(vec![item]),
                    None => None,
                },
                || None,
                |a, b| match (a, b) {
                    (res @ Some(_), None) => res,
                    (None, res @ Some(_)) => res,
                    (Some(r1), Some(r2)) => Some(r1.into_iter().chain(r2).collect()),
                    _ => None,
                },
            )
            .map(|opt| opt.unwrap_or_default())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OsmRailway {
    // Represent the railway from OSM - moderately untransformed, without any splitting done.
    name: String,

    way_id: i64,

    node_ids: Vec<i64>, // A list of the node_ids that make up this way.
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RailwaySegment {
    name: String,
    way_id: i64,
    node_ids: Vec<i64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LatLong(f64, f64);

impl OsmRailway {
    pub fn from_osm_way(way: &osmpbf::Way) -> OsmRailway {
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

    /// Helper function to get a count of the number of times nodes are used in a vec of `OsmRailway`s.
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

    pub fn to_railway_segments(self, intersections: &HashSet<i64>) -> Vec<RailwaySegment> {
        // Keep track of the index as we iterate through the nodes. We never split on the
        // first node or we end up with a segment of length 1 which is useless.
        let mut index = 0;

        // Additionally, keep track of the last node in the previous split. We add it to the
        // beginning of the next split so that we don't lose that edge.
        let mut previous_node = None;
        let railway_segments: Vec<RailwaySegment> = self
            .node_ids
            .split_inclusive(|n| {
                let should_split = index != 0 && intersections.contains(n);
                index += 1;
                should_split
            })
            .map(|nodes| {
                // If there is a previous_node, add it at the beginning of the list
                let node_ids: Vec<_> = if let Some(previous_node) = previous_node {
                    let mut vec = Vec::with_capacity(nodes.len() + 1); // Reserve space for one more node
                    vec.push(previous_node);
                    vec.extend_from_slice(nodes); // Extend with nodes
                    vec
                } else {
                    nodes.to_vec() // No previous_node, just copy the slice to Vec
                };

                previous_node = nodes.last().cloned(); // Clone last element for future use

                RailwaySegment {
                    name: self.name.clone(),
                    way_id: self.way_id,
                    node_ids,
                }
            })
            .collect();

        railway_segments
    }
}

fn collect_nodes(node_ids: HashSet<i64>) -> HashMap<i64, LatLong> {
    let reader = ElementReader::from_path(FILE_NAME).unwrap();

    let node_coords =
        reader
            .par_map_reduce(
                |element| match element {
                    Element::DenseNode(node) if node_ids.contains(&node.id()) => Some(
                        HashMap::from([(node.id(), LatLong(node.lat(), node.lon()))]),
                    ),
                    _ => None,
                },
                || None,
                |a, b| match (a, b) {
                    (res @ Some(_), None) => res,
                    (None, res @ Some(_)) => res,
                    (Some(mut map1), Some(map2)) => {
                        map1.extend(map2);
                        Some(map1)
                    }
                    _ => None,
                },
            )
            .expect("Error parsing nodes.")
            .expect("No nodes found.");

    let count = node_coords.len();
    println!("Number of nodes {count}");

    node_coords
}

/// Read an OSM file and parse out all of the railways.
fn collect_all_railways() -> Vec<OsmRailway> {
    let railways =
        ElementReader::<std::fs::File>::collect_filtered(FILE_NAME, |element| match element {
            Element::Way(way) if way.tags().any(|(k, v)| k == "railway" && v == "rail") => {
                Some(OsmRailway::from_osm_way(&way))
            }
            _ => None,
        })
        .expect("Error collecting filtered elements");

    let num_ways = railways.len();
    println!("Number of railways {num_ways}");

    railways
}

fn segment_railways(railways: Vec<OsmRailway>) -> Vec<RailwaySegment> {
    let node_counts = OsmRailway::get_used_node_counts(&railways);

    let intersections = node_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(node_id, _)| *node_id)
        .collect();

    let railway_segments: Vec<RailwaySegment> = railways
        .into_iter()
        .flat_map(|railway| railway.to_railway_segments(&intersections))
        .collect();

    let num_segments = railway_segments.len();

    println!("Number of segments {num_segments}");

    railway_segments
}

fn main() {
    let railways = collect_all_railways();
    segment_railways(railways);
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

    let segmented_railways = segment_railways(railways);

    assert_eq!(7, segmented_railways.len(), "{:?}", segmented_railways);
}
