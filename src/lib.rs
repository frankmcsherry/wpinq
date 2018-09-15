//! Weighted Privacy Integrated Queries (wPINQ)
//!
//! This project provides a data analysis language and data synthesis tools that provide
//! the guarantee of differential privacy for records in the source data.
//!
//! The techniques used here are developed from those described in the paper
//! [Calibrating Data to Sensitivity in Private Data Analysis](http://www.vldb.org/pvldb/vol7/p637-proserpio.pdf).
//! There is a similar project written in C#, available at
//! [http://www-bcf.usc.edu/~proserpi/wPINQ.html](http://www-bcf.usc.edu/~proserpi/wPINQ.html).
//!
//! This project is initially a reproduction of the C# repository, from scratch and built
//! on the timely dataflow runtime. Its goals are to reproduce the original work, and also
//! to serve as a basis for experimentation.

extern crate fnv;
extern crate rand;
extern crate timely;

use std::rc::Rc;
use std::cell::RefCell;
use std::hash::{Hash, Hasher};

use timely::{Data, ExchangeData, Allocate};
use timely::progress::Timestamp;
use timely::dataflow::{Scope, Stream, ProbeHandle, InputHandle};
use timely::dataflow::operators::{Map, Filter, Concat};
use timely::dataflow::scopes::{Child, Root};

mod operators;
pub mod analyses;
mod merge_sort;

pub use operators::measure::Measurement;

/// A dataflow-agnostic handle to input data.
///
/// A dataset represents two streams of data: "truth", which is
/// the raw and sensitive data requiring protection, and "synth",
/// which is the synthetic data that may be tested against results
/// of computation applied to the true data.
pub struct DatasetHandle<T: Timestamp, D: Data> {
    pub truth: InputHandle<T, (D, i64)>,
    pub synth: InputHandle<T, (D, i64)>,
}

impl<T: Timestamp, D: Data> DatasetHandle<T, D> {
    /// Create a new empty dataset handle.
    pub fn new() -> Self {
        DatasetHandle {
            truth: InputHandle::new(),
            synth: InputHandle::new(),
        }
    }
    /// Introduce the dataset into a dataflow scope, for computation.
    pub fn enter<'a, A: Allocate>(&mut self, scope: &mut Child<'a, Root<A>, T>) -> Dataset<Child<'a, Root<A>, T>, D> {
        Dataset::from(self.truth.to_stream(scope), self.synth.to_stream(scope))
    }
    /// Initialize the dataset's data from a supplied iterator.
    pub fn truth_from<I: Iterator<Item=(D,i64)>>(&mut self, iter: I) {
        for item in iter {
            self.truth.send(item);
        }
    }
    /// Close the dataset handle.
    pub fn close(self) {
        self.truth.close();
        self.synth.close();
    }
}

/// A collection of weighted elements of type `D`.
///
/// A `Dataset` represents a collection of weighted elements, and supports several
/// operations on such collections. All of these operations are designed to be affine,
/// in the sense that any change in weight in a dataset results in at most the same
/// change in weight across all derived datasets.
///
/// The two member streams correspond to the stream of sensitive data, and to the stream
/// of synthetic data.
pub struct Dataset<G: Scope, D: Data> {
    truth: Stream<G, (D, i64)>,
    synth: Stream<G, (D, i64)>,
}

impl<G: Scope, D: Data> Dataset<G, D> {

    // Constructs a new `Dataset` from a stream of weighted elements.
    pub fn from(truth: Stream<G, (D, i64)>, synth: Stream<G, (D, i64)>) -> Self {
        Dataset { truth: truth, synth: synth }
    }

    // Transform each record using `function`.
    pub fn map<R: Data, F: Fn(D)->R+'static>(self, function: F) -> Dataset<G, R> {
        let function1 = Rc::new(function);
        let function2 = function1.clone();
        Dataset::from(
            self.truth.map(move |(d,w)| (function1(d), w)),
            self.synth.map(move |(d,w)| (function2(d), w))
        )
    }

    /// Restrict the collection to elements satisfying `predicate`.
    ///
    /// This has the defect that it simply drops some elements, where they should
    /// probably instead be consumed through measurement, or returned separately.
    pub fn filter<P: Fn(&D)->bool+'static>(self, predicate: P) -> Dataset<G, D> {
        let predicate1 = Rc::new(predicate);
        let predicate2 = predicate1.clone();
        Dataset::from(
            self.truth.filter(move |&(ref d,_)| (predicate1)(d)),
            self.synth.filter(move |&(ref d,_)| (predicate2)(d))
        )
    }

    /// Merges two datasets, accumulating their weights.
    pub fn concat(self, other: Self) -> Self {
        Dataset::from(
            self.truth.concat(&other.truth),
            self.synth.concat(&other.synth)
        )
    }

    /// Merges two datasets, subtracting their weights.
    pub fn except(self, other: Self) -> Self {
        Dataset::from(
            self.truth.concat(&other.truth.map(|(d,w)| (d,-w))),
            self.synth.concat(&other.synth.map(|(d,w)| (d,-w)))
        )
    }
}

impl<G: Scope, D: ExchangeData+Ord+Hash> Dataset<G, D> {

    // Maps each element into a list of elements, distributing weight among them.
    pub fn flat_map<I, F>(self, function: F) -> Dataset<G, I::Item>
    where
        I: IntoIterator,
        I::Item: Data+Eq+Hash+Clone,
        F: Fn(D)->I+'static,
    {
        let function1 = Rc::new(function);
        let function2 = function1.clone();
        Dataset::from(
            operators::flat_map::flat_map(&self.truth, move |x| (*function1)(x)),
            operators::flat_map::flat_map(&self.synth, move |x| (*function2)(x))
        )
    }

    /// Transforms each weighted element into a sequence of elements of common weight.
    ///
    /// This method takes a collection of elements of the form (datum, weight) and produces
    ///
    /// ((datum, index), clamp(weight - index * width))
    ///
    /// where `clamp` clamps a value to the interval (0, width]. In practice, this means that
    /// values of `index` are produced for `0 .. weight / width`, where the last `index` value
    /// may have a weight less than `width` if `weight` is not a multiple of `width`.
    pub fn shave(self, width: i64) -> Dataset<G, (D, usize)> {
        Dataset::from(
            operators::shave::shave(&self.truth, width),
            operators::shave::shave(&self.synth, width)
        )
    }

    /// Returns two collections, of the minimum and maximum weights for each element, respectively.
    ///
    /// This method is useful for finding the intersection or union, but by consuming the inputs both are
    /// produced at no additional cost.
    pub fn min_max(self, other: Self) -> (Self, Self) {
        let (min_truth, max_truth) = operators::min_max::min_max(&self.truth, &other.truth);
        let (min_synth, max_synth) = operators::min_max::min_max(&self.synth, &other.synth);
        (Dataset::from(min_truth, min_synth), Dataset::from(max_truth, max_synth))
    }
}

impl<G: Scope, K: ExchangeData+Eq+Hash, V1: ExchangeData+Ord> Dataset<G, (K, V1)> {

    /// Joins two keyed collections, pairing values with the same keys.
    ///
    /// This method produces pairs whose weights are proportional to the product of the weights
    /// of the source elements, scaled down by a factor equal to the sum of the weights of all
    /// elements associated with the key. This scaling is necessary to ensure that a change in
    /// either input has a correspondingly bounded change in the output, independent of the total
    /// weight of elements in the other input.
    pub fn join<V2: ExchangeData+Ord>(self, other: Dataset<G, (K, V2)>) -> Dataset<G, (K, (V1, V2))> {
        Dataset::from(
            operators::join::join(&self.truth, &other.truth),
            operators::join::join(&self.synth, &other.synth)
        )
    }
}

impl<G: Scope, D: ExchangeData+Ord+Hash> Dataset<G, D> {

    /// Performs a Laplace-based noisy measurement.
    ///
    /// This measurement captures and tracks an approximate count for each element in the domain of the
    /// collection. To avoid disclosing details, the measurement does not list its contents but rather
    /// responds to queries about the counts for specific records. If a measurement exists it is returned,
    /// and if no measurement yet exists one is made and recorded.
    ///
    /// The supplied probe handle is used to indicate whether all measurements have been updated for an
    /// indicated timestamp.
    ///
    /// # Privacy
    ///
    /// This method uses `handle` to communicate when results are completely populated, and interaction with
    /// the resulting measurement may not provide differential privacy if not all updates have been applied.
    pub fn measure(self, handle: &mut ProbeHandle<G::Timestamp>, total: &Rc<RefCell<i64>>) -> operators::measure::Measurement<D> {
        operators::measure::measure(self.truth, self.synth, handle, total)
    }
}

/// Compute a FNV hash of an `element` implementing `Hash`.
fn fnv_hash<T: Hash>(element: &T) -> u64 {
    let mut h: ::fnv::FnvHasher = Default::default();
    element.hash(&mut h);
    h.finish()
}

/// Consolidates a disordered collection of `(T, i64)` pairs.
fn consolidate<T: Ord>(list: &mut Vec<(T,i64)>) {
    list.sort_unstable_by(|x,y| x.0.cmp(&y.0));
    for index in 1 .. list.len() {
        if list[index-1].0 == list[index].0 {
            list[index].1 += list[index-1].1;
            list[index-1].1 = 0;
        }
    }
    list.retain(|x| x.1 != 0);
}