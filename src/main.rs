use osmpbf::{Element, ElementReader};

pub struct OsmRailway {
    // Represent the railway from OSM - moderately untransformed, without any splitting done.
    name: String,

    way_id: i64,

    nodes: Vec<u64>, // A list of the node_ids that make up this way.
}

fn collect_all_railways() {
    let reader = ElementReader::from_path("./osm_data/us-northeast-latest.osm.pbf")
        .expect("Could not load data");

    // Iterate ove every element, find the ways - create a Vec of OsmRailway.
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
                            let mut vec = Vec::new();

                            let res = way.tags().find(|(key, _)| *key == "name");

                            let way_name = match res {
                                Some(res) => res.1.to_string(),
                                _ => way.id().to_string(),
                            };

                            let railway = OsmRailway {
                                name: way_name,
                                way_id: way.id(),
                                nodes: Vec::new(),
                            };

                            vec.push(railway);
                            Some(vec)
                        }
                        false => None,
                    }
                }
                _ => None,
            },
            || None,
            |a, b| match a {
                None => match b {
                    None => None,
                    Some(b) => Some(b),
                },
                Some(mut a) => match b {
                    None => Some(a),
                    Some(b) => {
                        // Combine them
                        a.extend(b);
                        Some(a)
                    }
                },
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
    count_all_railways();
    collect_all_railways();
}
