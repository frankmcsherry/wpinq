use std::rc::Rc;
use std::cell::RefCell;
use std::hash::Hash;

use timely::ExchangeData;
use timely::dataflow::{ProbeHandle, Scope};
use ::{Dataset, Measurement};

// Reports for each `index` the number of nodes with degree greater than `index`.
//
// This measurement captures the cumulative density function for the vertex degree, where each
// vertex contributes one unit to each integral value less or equal to its degree. Each count
// is subjected to Laplace noise, but each are relatively accurate measurements of the number
// of nodes with at least the given degree.
pub fn cdf<G: Scope, D: ExchangeData+Ord+Hash>(
    dataset: Dataset<G, D>,
    probe: &mut ProbeHandle<G::Timestamp>,
    total: &Rc<RefCell<i64>>,
    width: i64) -> Measurement<usize> {
    dataset
        .shave(width)
        .map(|(_src, idx)| idx)
        .measure(probe, total)
}

// Reports for each `index` the `index`-th largest degree in the graph.
//
// This measurement captures the degree sequence, from largest to smallest, by transposing the
// cumulative density function. The double-transposition has the effect of re-ordering the
// degrees from largest to smallest; we could also have taken the measurements using the original
// node identifiers if we had reason to know what they ranged over, but generally we do not.
pub fn seq<G: Scope, D: ExchangeData+Ord+Hash>(
    dataset: Dataset<G, D>,
    probe: &mut ProbeHandle<G::Timestamp>,
    total: &Rc<RefCell<i64>>,
    width:i64) -> Measurement<usize> {
    dataset
        .shave(width)
        .map(|(_src, idx)| idx)
        .shave(width)
        .map(|(_src, idx)| idx)
        .measure(probe, total)
}

/// Fits joint cdf and sequence measurements
///
/// This method tries to find the minimum weight grid path connecting the points (0, infinity) and
/// (infinity, 0), where the cost of an edge corresponds to committing to that measurement. More
/// specifically, edges are either horizontal or vertical, and their costs are
///
/// cost((a,b) -> (a+1,b)) : math::abs(b - seqs[a])
/// cost((a,b+1) -> (a,b)) : math::abs(a - cdfs[b])
///
/// The intuition is that traversing an edge corresponds to committing to that edge in the actual
/// cdf/seq measurement, and so the cost is the sum of the errors in the corresponding measurements.
pub fn fit_cdf_seq(horizontal: &[f64], vertical: &[f64], cost: impl Fn(f64,f64)->f64) -> (Vec<usize>, Vec<usize>) {

    #[derive(PartialEq)]
    struct QueueKey(f64);

    impl PartialOrd for QueueKey {
        fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
            (other.0).partial_cmp(&self.0)
        }
    }
    impl Ord for QueueKey {
        fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
            self.partial_cmp(other).unwrap()
        }
    }

    impl Eq for QueueKey { }

    assert!(!horizontal.is_empty());
    assert!(!vertical.is_empty());

    let mut queue = ::std::collections::BinaryHeap::new();
    let mut dists = ::std::collections::HashMap::new();

    let max_x = ::std::cmp::max(vertical.iter().map(|x| x.round() as i64).max().unwrap(), 0) as usize;
    let max_y = ::std::cmp::max(horizontal.iter().map(|x| x.round() as i64).max().unwrap(), 0) as usize;

    queue.push((QueueKey(0.0), 0, max_y));
    while !dists.contains_key(&(max_x, 0)) {

        if let Some((QueueKey(d), x, y)) = queue.pop() {
            if !dists.contains_key(&(x,y)) {
                dists.insert((x,y), d);
                // consider (x,y) -> (x+1,y); costs additional abs(h[x] - y)
                if x + 1 <= max_x {
                    queue.push((QueueKey(d + cost(horizontal[x], y as f64)), x+1, y));
                }

                // consider (x,y) -> (x,y-1); costs additional abs(v[y-1] - x)
                if y > 0 {
                    queue.push((QueueKey(d + cost(vertical[y-1], x as f64)), x, y-1));
                }
            }
        }
        else {
            panic!("ran out of reachable states; mysterious!");
        }
    }

    // now we walk backwards from (max_x, 0) to find the minimum path
    let mut current = (max_x, 0);

    let mut result_h = vec![0; max_x];
    let mut result_v = vec![0; max_y];

    while current != (0, max_y) {

        let (x,y) = current;
        let dist1 = dists.get(&(x-1,y));
        let dist2 = dists.get(&(x,y+1));

        match (dist1, dist2) {
            (None, None) => { panic!("backwards tracing failed!") }
            (Some(_), None) => {
                // edge (x-1,y) -> (x,y)
                current = (x-1, y);
                result_h[x-1] = y;
            },
            (None, Some(_)) => {
                // edge (x,y+1) -> (x,y)
                current = (x, y+1);
                result_v[y] = x;
            },
            (Some(d1), Some(d2)) => {
                let d1 = d1 + cost(horizontal[x-1], y as f64);
                let d2 = d2 + cost(vertical[y], x as f64);

                if d1 <= d2 {
                    // edge (x-1,y) -> (x,y)
                    current = (x-1, y);
                    result_h[x-1] = y;
                }
                else {
                    // edge (x,y+1) -> (x,y)
                    current = (x, y+1);
                    result_v[y] = x;
                }
            }
        }
    }

    (result_h, result_v)
}

mod tests {
    #[test]
    fn test_fit1() {
        let h = vec![10, 4, 2, 1, 1];
        let v = vec![5, 3, 2, 2, 1, 1, 1, 1, 1, 1];

        let hf = h.iter().map(|&x| x as f64).collect::<Vec<_>>();
        let vf = v.iter().map(|&x| x as f64).collect::<Vec<_>>();

        let (hn, vn) = super::fit_cdf_seq(&hf[..], &vf[..]);

        assert_eq!(h, hn);
        assert_eq!(v, vn);
    }
}