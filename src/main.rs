use osmpbf::{Element, ElementReader};

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
}
