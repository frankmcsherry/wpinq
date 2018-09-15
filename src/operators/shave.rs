use std::cmp::{min, max};
use std::collections::HashMap;
use std::hash::Hash;

use timely::ExchangeData;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Exchange;

use super::super::fnv_hash;
use super::super::merge_sort::MergeSorter;

use std::ops::DerefMut;

pub fn shave<G: Scope, D: ExchangeData+Ord+Hash>(stream: &Stream<G, (D,i64)>, width: i64) -> Stream<G, ((D, usize), i64)> {

    stream.unary(Exchange::new(|x: &(D,i64)| fnv_hash(&x.0)), "Shave", |_,_| {

        let mut state = HashMap::new();
        let mut sorters = HashMap::new();

        move |input, output| {

            while let Some((time, data)) = input.next() {
                sorters
                    .entry(time.retain())
                    .or_insert(MergeSorter::new())
                    .push(data.deref_mut());
            }

            for (time, mut data) in sorters.drain() {
                // consolidate(&mut data);

                let mut dataz = Vec::new();
                data.finish_into(&mut dataz);

                let mut session = output.session(&time);

                for data in dataz.into_iter() {
                for (datum, mut delta) in data.into_iter() {

                    let weight = state.entry(datum.clone()).or_insert(0);

                    // increment `weight`.
                    while delta > 0 {
                        let index = *weight / width;
                        let change = min((index + 1) * width - *weight, delta);
                        delta -= change;
                        *weight += change;
                        session.give(((datum.clone(), index as usize), change));
                    }

                    // decrement `weight`.
                    while delta < 0 {
                        let index = (*weight - 1) / width;
                        let change = max((index * width) - *weight, delta);
                        delta -= change;
                        *weight += change;
                        session.give(((datum.clone(), index as usize), change));
                    }
                }
                }
            }
        }
    })
}