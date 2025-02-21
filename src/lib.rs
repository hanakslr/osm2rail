use crate::reader::Filter;
use geo::{Distance, Haversine, Point};
use osmpbf::{Element, ElementReader, WayNodeLocation};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

mod reader;

pub trait HasTags {
    fn tags(&self) -> &HashMap<String, String>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OsmRailway {
    // Represent the railway from OSM - moderately untransformed, without any splitting done.
    pub name: String,

    pub way_id: i64,

    pub node_ids: Vec<i64>, // A list of the node_ids that make up this way.

    pub tags: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OsmNode {
    pub lat: f64,
    pub lon: f64,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RailwaySegment {
    pub name: String,
    pub way_id: i64,
    pub node_ids: Vec<i64>,
}

impl RailwaySegment {
    /// Given the railway segment and a mapping of the node ids to their locations
    /// calculate the distance of the segment in kilometers.
    pub fn get_distance(&self, node_locations: &HashMap<i64, OsmNode>) -> f64 {
        let mut found_count = 0;
        let mut not_found_count = 0;
        let ls: Vec<Point> = self
            .node_ids
            .iter()
            .filter_map(|node_id| match node_locations.get(node_id) {
                Some(n) => {
                    found_count += 1;
                    Some(Point::new(n.lon, n.lat))
                }
                None => {
                    not_found_count += 1;
                    None
                }
            })
            .collect();

        ls.iter()
            .zip(ls.iter().skip(1))
            .fold(0.0, |dist, (pt_a, pt_b)| {
                let seg_dist = Haversine::distance(*pt_a, *pt_b);
                dist + seg_dist
            })
    }
}

impl OsmRailway {
    pub fn from_osm_way(way: &osmpbf::Way) -> OsmRailway {
        let way_name = match way.tags().find(|(key, _)| *key == "name") {
            Some(res) => res.1.to_string(),
            _ => way.id().to_string(),
        };

        let tag_map = way
            .tags()
            .fold(HashMap::<String, String>::new(), |mut acc, (k, v)| {
                acc.insert(k.to_string(), v.to_string());
                acc
            });

        // Our refs are *deltas* - this means if we actually want the node ids, we need to iterate
        // over and add the delta to the previous number and that is our node id.
        let node_ids = way
            .raw_refs()
            .iter()
            .scan(0_i64, |acc, n| {
                *acc += n;
                Some(*acc)
            })
            .collect();

        OsmRailway {
            name: way_name,
            way_id: way.id(),
            node_ids: node_ids,
            tags: tag_map,
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

impl HasTags for OsmRailway {
    fn tags(&self) -> &HashMap<String, String> {
        &self.tags
    }
}

impl HasTags for OsmNode {
    fn tags(&self) -> &HashMap<String, String> {
        &self.tags
    }
}

/// Read an OSM file and parse out all of the railways.
pub fn collect_all_railways(file: &str) -> Vec<OsmRailway> {
    let railways =
        ElementReader::<std::fs::File>::collect_filtered(file, |element| match element {
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

pub fn collect_nodes(file: &str) -> HashMap<i64, OsmNode> {
    let reader = ElementReader::from_path(file).unwrap();

    let node_coords = reader
        .par_map_reduce(
            |element| match element {
                Element::DenseNode(node) => Some(HashMap::from([(
                    node.id(),
                    OsmNode {
                        lat: node.lat(),
                        lon: node.lon(),
                        tags: node.tags().fold(
                            HashMap::<String, String>::new(),
                            |mut acc, (k, v)| {
                                acc.insert(k.to_string(), v.to_string());
                                acc
                            },
                        ),
                    },
                )])),
                _ => None,
            },
            || None,
            |a, b| match (a, b) {
                (None, None) => None,
                (res @ Some(_), None) | (None, res @ Some(_)) => res,
                (Some(mut map1), Some(map2)) => {
                    map1.extend(map2);
                    Some(map1)
                }
            },
        )
        .expect("Error parsing nodes.")
        .expect("No nodes found.");

    let count = node_coords.len();
    println!("Number of nodes {count}");

    node_coords
}

pub fn segment_railways(railways: Vec<OsmRailway>) -> Vec<RailwaySegment> {
    let node_counts = OsmRailway::get_used_node_counts(&railways);

    let intersections = node_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(node_id, _)| *node_id)
        .collect();

    railways
        .into_iter()
        .flat_map(|railway| railway.to_railway_segments(&intersections))
        .collect()
}

#[test]
fn test_segment_railways() {
    // Given a simple vec of railways, break it up
    let railways = Vec::from([
        OsmRailway {
            name: "way 1".to_string(),
            way_id: 1,
            node_ids: Vec::from([1, 2, 3, 6, 8, 9]),
            tags: HashMap::new(),
        },
        OsmRailway {
            name: "way 2".to_string(),
            way_id: 2,
            node_ids: [2, 3].to_vec(),
            tags: HashMap::new(),
        },
        OsmRailway {
            name: "way 3".to_string(),
            way_id: 3,
            node_ids: [4, 5, 6, 7].to_vec(),
            tags: HashMap::new(),
        },
    ]);

    let segmented_railways = segment_railways(railways);

    assert_eq!(7, segmented_railways.len(), "{:?}", segmented_railways);
}
