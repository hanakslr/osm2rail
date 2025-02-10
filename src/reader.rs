use osmpbf::{Element, ElementReader};
use std::io::Read;

pub trait Filter {
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
