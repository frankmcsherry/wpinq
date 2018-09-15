// use std::collections::HashMap;
use std::hash::Hash;

use timely::{Data, ExchangeData};
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Exchange;

use super::super::fnv_hash;

pub fn flat_map<D, G, I, F>(stream: &Stream<G, (D,i64)>, function: F) -> Stream<G, (I::Item, i64)>
where
    G: Scope,
    D: ExchangeData+Eq+Hash,
    I: IntoIterator,
    I::Item: Data+Eq+Hash+Clone,
    F: Fn(D)->I+'static,
{
    // TODO: Rounding may be an issue here, as dividing by the weight could do surprising things if
    //       we don't see exact negations of records.

    stream.unary(Exchange::new(|x: &(D,i64)| fnv_hash(&x.0)), "FlatMap", |_,_| {

        let mut stash = Vec::new();

        move |input, output| {
            while let Some((time, data)) = input.next() {
                let mut session = output.session(&time);
                for (datum, delta) in data.drain(..) {
                    stash.extend(function(datum.clone()));
                    let length = stash.len() as i64;
                    for result in stash.drain(..) {
                        session.give((result, delta / length));
                    }
                }
            }
        }
    })
}