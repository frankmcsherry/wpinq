use std::collections::HashMap;
use std::hash::Hash;

use timely::ExchangeData;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Exchange;

use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

use super::super::fnv_hash;

pub fn min_max<G: Scope, D: ExchangeData+Eq+Hash>(
    stream1: &Stream<G, (D, i64)>,
    stream2: &Stream<G, (D, i64)>) -> (Stream<G, (D, i64)>, Stream<G, (D, i64)>)
{
    let mut state = HashMap::<D, (i64, i64)>::new();

    let exchange1 = Exchange::new(|x: &(D,i64)| fnv_hash(&x.0));
    let exchange2 = Exchange::new(|x: &(D,i64)| fnv_hash(&x.0));

    let mut builder = OperatorBuilder::new("MinMax".to_owned(), stream1.scope());

    let mut input1 = builder.new_input(stream1, exchange1);
    let mut input2 = builder.new_input(stream2, exchange2);
    let (mut output1, stream1) = builder.new_output();
    let (mut output2, stream2) = builder.new_output();

    builder.build(move |_capability| {

        move |frontiers| {

            let mut input_handle1 = FrontieredInputHandle::new(&mut input1, &frontiers[0]);
            let mut input_handle2 = FrontieredInputHandle::new(&mut input2, &frontiers[1]);
            let mut output_handle1 = output1.activate();
            let mut output_handle2 = output2.activate();

            while let Some((time, data)) = input_handle1.next() {

                let mut session1 = output_handle1.session(&time);
                let mut session2 = output_handle2.session(&time);

                for (key, delta) in data.drain(..) {

                    let mut entry = state.entry(key.clone()).or_insert((0, 0));

                    let mut min_change = ::std::cmp::min(entry.0, entry.1);
                    let mut max_change = ::std::cmp::max(entry.0, entry.1);
                    entry.0 += delta;
                    min_change -= ::std::cmp::min(entry.0, entry.1);
                    max_change -= ::std::cmp::max(entry.0, entry.1);

                    if min_change != 0 {
                        session1.give((key.clone(), min_change));
                    }
                    if max_change != 0 {
                        session2.give((key.clone(), max_change));
                    }
                }

            }

            while let Some((time, data)) = input_handle2.next() {

                let mut session1 = output_handle1.session(&time);
                let mut session2 = output_handle2.session(&time);

                for (key, delta) in data.drain(..) {

                    let mut entry = state.entry(key.clone()).or_insert((0, 0));

                    let mut min_change = ::std::cmp::min(entry.0, entry.1);
                    let mut max_change = ::std::cmp::max(entry.0, entry.1);
                    entry.1 += delta;
                    min_change -= ::std::cmp::min(entry.0, entry.1);
                    max_change -= ::std::cmp::max(entry.0, entry.1);

                    if min_change != 0 {
                        session1.give((key.clone(), min_change));
                    }
                    if max_change != 0 {
                        session2.give((key.clone(), max_change));
                    }
                }
            }
        }
    });

    (stream1, stream2)
}