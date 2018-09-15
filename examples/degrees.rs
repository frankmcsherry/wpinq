extern crate rand;
extern crate timely;
extern crate wpinq;

use std::rc::Rc;
use std::cell::RefCell;
use std::io::{BufRead, BufReader, Write};
use std::fs::File;

use rand::Rng;
use timely::dataflow::{InputHandle, ProbeHandle};

use wpinq::Dataset;
use wpinq::analyses::degrees;

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        let mut truth = InputHandle::new();
        let mut synth = InputHandle::new();

        let mut probe = ProbeHandle::new();

        let total = Rc::new(RefCell::new(0i64));


        let weight = i32::max_value() as i64 / 10;

        // measure the number of edges.
        let mut nodes_measurement = worker.dataflow(|scope| {
            let dataset = Dataset::from(truth.to_stream(scope), synth.to_stream(scope));
            degrees::cdf(dataset.flat_map(|(src, dst)| Some(src).into_iter().chain(Some(dst))), &mut probe, &total, weight / 2)
        });

        // measure the number of edges.
        let mut edges_measurement = worker.dataflow(|scope| {
            let dataset = Dataset::from(truth.to_stream(scope), synth.to_stream(scope));
            dataset.map(|_| ()).measure(&mut probe, &total)
        });

        // measure the number of nodes with at least each number of edges.
        let mut measurements1 = worker.dataflow(|scope| {
            let dataset = Dataset::from(truth.to_stream(scope), synth.to_stream(scope));
            degrees::cdf(dataset.map(|(src, _)| src), &mut probe, &total, weight)
        });

        // measure the degrees of nodes from large to small.
        let mut measurements2 = worker.dataflow(|scope| {
            let dataset = Dataset::from(truth.to_stream(scope), synth.to_stream(scope));
            degrees::seq(dataset.map(|(src, _)| src), &mut probe, &total, weight)
        });

        let mut graph = Vec::new();

        // load up the "sensitive" data.
        let filename = std::env::args().nth(1).unwrap();
        let file = BufReader::new(File::open(filename).unwrap());
        for readline in file.lines() {
            let line = readline.ok().expect("read error");
            if !line.starts_with('#') {
                let mut elts = line[..].split_whitespace();
                let src: usize = elts.next().unwrap().parse().ok().expect("malformed src");
                let dst: usize = elts.next().unwrap().parse().ok().expect("malformed dst");
                // graph.push((src, dst));
                truth.send(((src, dst), weight));
            }
        }
        truth.close();

        println!("{:?}\tloading complete", timer.elapsed());

        // propagate true data.
        synth.advance_to(1);
        while probe.less_than(synth.time()) { worker.step(); }

        println!("{:?}\tcomputation stable, total error: {:?}", timer.elapsed(), *total.borrow() / weight);

        // report measurements on nodes, edges, and degree distributions.
        let nodes = nodes_measurement.observe(0) / (weight/2);
        let edges = edges_measurement.observe(()) / weight;
        println!("nodes: {:?}", nodes);
        println!("edges: {:?}", edges);

        let mut degree_cdf = Vec::new();
        let mut degree_seq = Vec::new();
        for i in 0 .. (nodes as usize) {
            degree_cdf.push((measurements1.observe(i) as f64) / (weight as f64));
        }
        for i in 0 .. (nodes as usize) {
            degree_seq.push((measurements2.observe(i) as f64) / (weight as f64));
        }

        // let (fitted_cdf, fitted_seq) = degrees::fit_cdf_seq(&degree_cdf[..], &degree_seq[..], |x,y| (x-y).abs());
        let (fitted_cdf, fitted_seq) = degrees::fit_cdf_seq(&degree_cdf[..], &degree_seq[..], |x,y| (x-y) * (x-y));
        let limit = fitted_seq[0];

        // for i in 0 .. limit  {
        //     println!("cdf\t{:?}\t{:?}", i, degree_cdf[i]);
        // }

        // for i in 0 .. limit  {
        //     println!("fit\t{:?}\t{:?}", i, fitted_cdf[i]);
        // }

        let mut rng = ::rand::thread_rng();

        // synthesize random graph.
        println!("{:?}\tsynthesizing random graph on {:?} nodes and {:?} edges", timer.elapsed(), nodes, edges);
        for _ in 0 .. edges {
            let src = rng.gen_range(0, nodes as usize);
            let dst = rng.gen_range(0, nodes as usize);
            graph.push((src, dst));
        }

        for &(src, dst) in graph.iter() {
            synth.send(((src, dst), weight));
        }

        println!("{:?}\tdata synthesized", timer.elapsed());

        synth.advance_to(2);
        while probe.less_than(synth.time()) { worker.step(); }

        let mut total_error = *total.borrow();

        println!("{:?}\tround {:?}, total error: {:?}", timer.elapsed(), 0, total_error / weight);

        // for round in 3 .. {

        //     if round % 1000000 == 0 {
        //         println!("{:?}\tround {:?}, total error: {:?}", timer.elapsed(), round, total_error / weight);
        //     }

        //     if round % 10000000 == 0 {
        //         let mut file = File::create(format!("output-{}.txt", round)).unwrap();
        //         for &(src, dst) in graph.iter() {
        //             file.write_fmt(format_args!("{}\t{}", src, dst)).unwrap();
        //         }
        //     }

        //     let index = rng.gen_range(0, graph.len());

        //     let src = rng.gen_range(0, nodes as usize);
        //     let dst = rng.gen_range(0, nodes as usize);
        //     let change = (src, dst);

        //     // try out a change
        //     synth.send((graph[index], -weight));
        //     synth.send((change, weight));
        //     synth.advance_to(round);
        //     while probe.less_than(synth.time()) { worker.step(); }

        //     let new_error = *total.borrow();

        //     if total_error < new_error {
        //         synth.send((graph[index], weight));
        //         synth.send((change, -weight))
        //     }
        //     else {
        //         graph[index] = change;
        //         total_error = new_error;
        //     }
        // }
    }).unwrap();
}