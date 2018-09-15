use std::collections::HashMap;
use std::hash::Hash;

use timely::ExchangeData;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Exchange;

use super::super::{consolidate, fnv_hash};

pub fn join<G: Scope, K: ExchangeData+Eq+Hash, V1: ExchangeData+Ord, V2: ExchangeData+Ord>(
    stream1: &Stream<G, ((K, V1), i64)>,
    stream2: &Stream<G, ((K, V2), i64)>) -> Stream<G, ((K, (V1, V2)), i64)>
{
    // The intended behavior of `join` is that it takes a pair of similarly keyed collections
    // to a collection of keyed pairs, whose weights are scaled down so that each input record
    // results in output records with weight at most that of the input record.
    //
    // Specifically, if for some key we have values (v1_i, w1_i) and (v2_i, w2_i), the output
    // collection should be equal to
    //
    //   (k, (v1_i, v2_j)) with weight = w1_i * w2_j / (sum_i |w1_i| + sum_i |w2_i|)
    //
    // There are several issues related to rounding and such, but this is the intent.

    // let mut input1_stash = Vec::<(V1, i64)>::new();
    // let mut input2_stash = Vec::<(V2, i64)>::new();

    let exchange1 = Exchange::new(|x: &((K,V1),i64)| fnv_hash(&(x.0).0));
    let exchange2 = Exchange::new(|x: &((K,V2),i64)| fnv_hash(&(x.0).0));

    stream1.binary(stream2, exchange1, exchange2, "Join", |_,_| {

        let mut output_stash = Vec::new();
        let mut state = HashMap::<K, (Vec<(V1,i64)>, Vec<(V2,i64)>)>::new();

        move |input1, input2, output| {

            // TODO: This could be much more efficient if updates are first consolidated
            //       by key. That would result in fewer re-evaluations, as well as optimized
            //       performance when there is a net-zero change to the sum of the absolute
            //       values (not yet implemented).

            // drain the first input.
            while let Some((time, data)) = input1.next() {
                let mut session = output.session(&time);
                for ((key, val), delta) in data.drain(..) {
                    let entry = state.entry(key.clone()).or_insert((Vec::new(), Vec::new()));

                    // compute old output, then negate.
                    join_helper(&entry.0, &entry.1, &mut output_stash);
                    for pair in output_stash.iter_mut() { pair.1 *= -1; }

                    // apply update.
                    entry.0.push((val, delta));
                    consolidate(&mut entry.0);

                    // compute new output, don't negate.
                    join_helper(&entry.0, &entry.1, &mut output_stash);

                    consolidate(&mut output_stash);
                    for (result, delta) in output_stash.drain(..) {
                        session.give(((key.clone(), result), delta));
                    }
                }
            }

            // drain the second input.
            while let Some((time, data)) = input2.next() {
                let mut session = output.session(&time);
                for ((key, val), delta) in data.drain(..) {
                    let entry = state.entry(key.clone()).or_insert((Vec::new(), Vec::new()));

                    // compute old output, then negate.
                    join_helper(&entry.0, &entry.1, &mut output_stash);
                    for pair in output_stash.iter_mut() { pair.1 *= -1; }

                    // apply update.
                    entry.1.push((val, delta));
                    consolidate(&mut entry.1);

                    // compute new output, don't negate.
                    join_helper(&entry.0, &entry.1, &mut output_stash);

                    consolidate(&mut output_stash);
                    for (result, delta) in output_stash.drain(..) {
                        session.give(((key.clone(), result), delta));
                    }
                }
            }
        }
    })
}

fn join_helper<V1:Ord+Clone, V2:Ord+Clone>(
    list1: &[(V1,i64)],
    list2: &[(V2,i64)],
    output: &mut Vec<((V1,V2),i64)>)
{
    let total1: i64 = list1.iter().map(|x| x.1.abs()).sum();
    let total2: i64 = list2.iter().map(|x| x.1.abs()).sum();
    let total = total1 + total2;

    for &(ref datum1, weight1) in list1.iter() {
        for &(ref datum2, weight2) in list2.iter() {
            output.push(((datum1.clone(), datum2.clone()), (weight1 * weight2) / total));
        }
    }
}

