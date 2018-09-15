use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;

use timely::ExchangeData;
use timely::dataflow::{Scope, Stream, ProbeHandle};
use timely::dataflow::operators::{Operator, Probe};
use timely::dataflow::channels::pact::Exchange;

use super::super::{consolidate, fnv_hash};
// use super::super::merge_sort::MergeSorter;

/// Performs a Laplace-based noisy measurement.
///
/// This measurement captures and tracks an approximate count for each element in the domain of the
/// collection. To avoid disclosing details, the measurement does not list its contents but rather
/// responds to queries about the counts for specific records. If a measurement exists it is returned,
/// and if no measurement yet exists one is made and recorded.
///
/// The supplied probe handle is used to indicate whether all measurements have been updated for an
/// indicated timestamp.
pub fn measure<G: Scope, D: ExchangeData+Ord+Hash>(
    stream1: Stream<G, (D,i64)>,
    stream2: Stream<G, (D,i64)>,
    handle: &mut ProbeHandle<G::Timestamp>,
    total: &Rc<RefCell<i64>>) -> Measurement<D>
{
    let shared = Rc::new(RefCell::new(MeasurementState::new(total)));
    measure_truth(&stream1, shared.clone(), handle);
    measure_synth(&stream2, shared.clone(), handle);
    Measurement { shared: shared }
}

fn measure_truth<G: Scope, D: ExchangeData+Ord+Hash>(
    stream: &Stream<G, (D,i64)>,
    shared: Rc<RefCell<MeasurementState<D>>>,
    handle: &mut ProbeHandle<G::Timestamp>)
{
    stream.unary::<(),_,_,_>(Exchange::new(|x: &(D,i64)| fnv_hash(&x.0)), "MeasureTruth", |_,_| {

        let mut buffer = Vec::new();

        move |input, _output| {

            input.for_each(|_time, data| {
                buffer.extend(data.drain(..));
            });

            let mut borrow = shared.borrow_mut();
            consolidate(&mut buffer);
            for (datum, delta) in buffer.drain(..) {
                borrow.update_truth(datum, delta);
            }
        }
    })
    .probe_with(handle);
}

fn measure_synth<G: Scope, D: ExchangeData+Ord+Hash>(
    stream: &Stream<G, (D,i64)>,
    shared: Rc<RefCell<MeasurementState<D>>>,
    handle: &mut ProbeHandle<G::Timestamp>)
{
    stream.unary::<(),_,_,_>(Exchange::new(|x: &(D,i64)| fnv_hash(&x.0)), "MeasureSynth", |_,_| move |input, _output| {

        let mut buffer = Vec::new();

        input.for_each(|_time, data| {
            buffer.extend(data.drain(..));
        });

        let mut borrow = shared.borrow_mut();
        consolidate(&mut buffer);
        for (datum, delta) in buffer.drain(..) {
            borrow.update_synth(datum, delta);
        }
    })
    .probe_with(handle);
}

/// The state required to back measurements made of sensitive data.
///
/// This state tracks both the accumulated counts for the sensitive and the synthetic data.
/// It allows one to query the sensitive data, which binds and returns the measurement, and
/// to assess the fit of synthetic data by reporting the sum of absolute values in error for
/// the measurements.
struct MeasurementState<D: Hash+Eq> {
    total_error: Rc<RefCell<i64>>,
    measurements: HashMap<D, (i64, i64)>,
}

impl<D: Hash+Eq> MeasurementState<D> {

    pub fn new(total: &Rc<RefCell<i64>>) -> Self {
        MeasurementState {
            total_error: total.clone(),
            measurements: HashMap::new(),
        }
    }

    pub fn update_truth(&mut self, element: D, delta: i64) {
        let entry =
        self.measurements
            .entry(element)
            .or_insert((0, laplace()));

        // update total error measurements.
        *self.total_error.borrow_mut() -= (entry.1 - entry.0).abs();
        entry.1 += delta;
        *self.total_error.borrow_mut() += (entry.1 - entry.0).abs();
    }

    pub fn update_synth(&mut self, element: D, delta: i64) {
        let entry =
        self.measurements
            .entry(element)
            .or_insert((0, laplace()));

        // update total error measurements.
        *self.total_error.borrow_mut() -= (entry.1 - entry.0).abs();
        entry.0 += delta;
        *self.total_error.borrow_mut() += (entry.1 - entry.0).abs();
    }

    /// Observes the noisy count associated with an element.
    ///
    /// This method also binds the current measurement as "truth", so that future
    /// updates to the counts are reflected in `self.total_error`. This is important
    /// for tracking how well subsequent candidate datasets match observed counts.
    ///
    /// This method binds the observation as truth, from which
    pub fn observe(&mut self, element: D) -> i64 {
        self.measurements
            .entry(element)
            .or_insert((0, laplace()))
            .1
    }
}

pub struct Measurement<D: Hash+Eq> {
    shared: Rc<RefCell<MeasurementState<D>>>,
}

impl<D: Hash+Eq> Measurement<D> {
    /// Observes the noised count associated with `data`.
    ///
    /// This method inserts noise if the key is not yet present, so that repeated
    /// queries do not risk disclosing its absence.
    pub fn observe(&mut self, data: D) -> i64 {
        self.shared.borrow_mut().observe(data)
    }
}

// generates a sample from the Laplace distribution
fn laplace() -> i64 {

    use rand::Rng;

    // TODO: Replace with independent bit flipping.
    let mut rng = ::rand::thread_rng();
    let logarithm: f64 = rng.gen::<f64>().ln();
    let result = (logarithm * (i32::max_value() as f64)) as i64;
    if rng.gen() { result } else { -result }
}