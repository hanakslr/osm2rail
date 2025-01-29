use osmpbf::{Element, ElementReader};
use std::collections::{HashMap, HashSet};
use std::io::Read;

trait ElementReaderExt {
    fn collect_filtered<T, F>(path: &str, filter_map: F) -> Result<Vec<T>, osmpbf::Error>
    where
        F: Fn(Element) -> Option<T> + Send + Sync,
        T: Send;
}

impl<R: Read + Send> ElementReaderExt for ElementReader<R> {
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

#[derive(Debug)]
pub struct OsmRailway {
    // Represent the railway from OSM - moderately untransformed, without any splitting done.
    name: String,

    way_id: i64,

    node_ids: Vec<i64>, // A list of the node_ids that make up this way.
}

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

impl OsmNode {
    pub fn from_osm_dense_node(node: osmpbf::DenseNode) -> OsmNode {
        OsmNode {
            node_id: node.id(),
            latitude: node.lat(),
            longitude: node.lon(),
        }
    }
}

fn collect_nodes(node_ids: Vec<i64>) -> Vec<OsmNode> {
    let nodes = ElementReader::<std::fs::File>::collect_filtered(
        "./osm_data/us-latest.osm.pbf",
        |element| match element {
            Element::DenseNode(node) if node_ids.contains(&node.id()) => {
                Some(OsmNode::from_osm_dense_node(node))
            }
            _ => None,
        },
    )
    .expect("Error collecting filtered elements");

    let num_nodes = nodes.len();
    println!("Number of nodes: {num_nodes}");

    nodes
}

/// Read an OSM file and parse out all of the railways.
fn collect_all_railways() -> Vec<OsmRailway> {
    let railways = ElementReader::<std::fs::File>::collect_filtered(
        "./osm_data/us-latest.osm.pbf",
        |element| match element {
            Element::Way(way) if way.tags().any(|(k, v)| k == "railway" && v == "rail") => {
                Some(OsmRailway::from_osm_way(&way))
            }
            _ => None,
        },
    )
    .expect("Error collecting filtered elements");

    let num_ways = railways.len();
    println!("Number of railways {num_ways}");

    railways
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

fn main() {
    let railways = collect_all_railways();
    let node_counts = OsmRailway::get_used_node_counts(&railways);

    let intersections = node_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(node_id, _)| *node_id) // Extract keys
        .collect();

    segment_railways(railways, intersections);

    collect_nodes(node_counts.into_keys().collect());

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
