#[macro_use]
extern crate abomonation;
extern crate timely;
extern crate arrayvec;
extern crate regex;

extern crate wpinq;

use std::rc::Rc;
use std::cell::RefCell;
use std::io::BufRead;
use regex::Regex;

use timely::dataflow::ProbeHandle;
use wpinq::DatasetHandle;

use types::*;

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let prefix = ::std::env::args().nth(1).unwrap();;

        // let timer = ::std::time::Instant::now();
        let index = worker.index();
        let peers = worker.peers();

        let mut lineitems = DatasetHandle::new();
        let mut orders = DatasetHandle::new();
        let mut suppliers = DatasetHandle::new();
        let mut partsupps = DatasetHandle::new();
        let mut customers = DatasetHandle::new();

        let mut probe = ProbeHandle::new();

        let total = Rc::new(RefCell::new(0i64));

        let weight = i32::max_value() as i64 / 10;

        // Measure Q0: (preliminary statistics)
        let mut q00 = worker.dataflow::<(), _, _>(|scope| {
            lineitems
                .enter(scope)
                .flat_map(|l: LineItem| (0 .. 64).map(move |i| (i, l.quantity >> i)))
                .measure(&mut probe, &total)
        });

        // Measure Q1:
        let mut q01 = worker.dataflow::<(), _, _>(|scope| {
            lineitems
                .enter(scope)
                .filter(|x: &LineItem| x.ship_date <= create_date(1998, 9, 2))
                .map(|x: LineItem| (x.return_flag[0] as u8, x.line_status[0] as u8))
                .measure(&mut probe, &total)
        });

        // Measure Q4:
        let mut q04 = worker.dataflow::<(), _, _>(|scope| {

            let lineitems =
            lineitems
                .enter(scope)
                .filter(|l: &LineItem| l.commit_date < l.receipt_date)
                .map(|l: LineItem| (l.order_key, ()))
                .shave(weight)          // "distinct", line 1/3
                .filter(|x| x.1 == 0)   // "distinct", line 2/3
                .map(|x| x.0);          // "distinct", line 3/3

            let orders =
            orders
                .enter(scope)
                .filter(|o: &Order| o.order_date >= create_date(1993, 7, 1) &&
                                    o.order_date < create_date(1993, 10, 1))
                .map(|o: Order| (o.order_key, o.order_priority));

            orders
                .join(lineitems)
                .map(|(_key, (priority, ()))| priority)
                .measure(&mut probe, &total)
        });

        // Measure Q13:
        let mut q13 = worker.dataflow::<(), _, _>(|scope| {

            let regex = Regex::new("special.*requests").expect("Regex construction failed");
            let orders =
            orders
                .enter(scope)
                .filter(move |o: &Order| !regex.is_match(&o.comment))
                .map(|o: Order| o.cust_key);

            let customers =
            customers
                .enter(scope)
                .map(|c: Customer| c.cust_key);

            customers
                .concat(orders)
                .shave(weight)
                .map(|(_src, idx)| idx)
                .measure(&mut probe, &total)
        });

        // Measure Q16:
        let mut q16 = worker.dataflow::<(), _, _>(|scope| {

            let regex = Regex::new("Customer.*Complaints").expect("Regex construction failed");
            let suppliers =
            suppliers
                .enter(scope)
                .filter(move |s: &Supplier| !regex.is_match(&s.comment))
                .map(|s: Supplier| (s.supp_key, ()));

            let partsupps =
            partsupps
                .enter(scope)
                .map(|ps: PartSupp| (ps.supp_key, ps.part_key));

            partsupps
                .join(suppliers)
                .shave(weight / 100)
                .filter(|x| x.1 == 0)
                .map(|x| x.0)
                .map(|(_key, (part_id, ()))| part_id)
                .shave(weight / 100)
                .map(|(_src, idx)| idx)
                .measure(&mut probe, &total)
        });

        orders.truth_from(load::<Order>(prefix.as_str(), "orders.tbl", index, peers).into_iter().map(|x| (x, weight)));
        orders.close();

        suppliers.truth_from(load::<Supplier>(prefix.as_str(), "supplier.tbl", index, peers).into_iter().map(|x| (x, weight)));
        suppliers.close();

        partsupps.truth_from(load::<PartSupp>(prefix.as_str(), "PartSupp.tbl", index, peers).into_iter().map(|x| (x, weight)));
        partsupps.close();

        customers.truth_from(load::<Customer>(prefix.as_str(), "customer.tbl", index, peers).into_iter().map(|x| (x, weight)));
        customers.close();

        lineitems.truth_from(load::<LineItem>(prefix.as_str(), "lineitem.tbl", index, peers).into_iter().map(|x| (x, weight)));
        lineitems.close();

        println!("data loaded");

        while worker.step() { }

        println!("Q01:");
        for a in 0 .. 256 {
            for b in 0 .. 256 {
                let measure = (q01.observe((a as u8, b as u8)) as f64) / (weight as f64);
                if measure > 1000.0 {
                    println!("\t{:?}:\t{:?}", (a,b), measure);
                }
            }
        }

        println!("Q04:");
        for priority in vec!["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"] {
            let measure = 2.0 * (q04.observe(types::read_u15(priority)) as f64) / (weight as f64);
            println!("\t{:?}:\t{:?}", priority, measure);
        }

        println!("Q13:");
        let mut counts = Vec::new();
        for count in 0 .. 50 {
            let measure = (q13.observe(count) as f64) / (weight as f64);
            if count > 0 {
                println!("\t{:?}:\t{:?}", count - 1, *counts.last().unwrap_or(&0.0) - measure);
            }
            counts.push(measure);
        }

        println!("Q16:");
        let mut counts = Vec::new();
        for count in 0 .. 50 {
            let measure = (q16.observe(count) as f64) / ((weight / 100) as f64);
            if count > 0 {
                println!("\t{:?}:\t{:?}", count, *counts.last().unwrap_or(&0.0) - measure);
            }
            counts.push(measure);
        }


    }).expect("did not complete cleanly.");
}

// Returns a sequence of physical batches of ready-to-go timestamped data.
// Not clear that `input` can exploit the pre-arrangement yet.
fn load<T>(prefix: &str, name: &str, index: usize, peers: usize) -> Vec<T>
where T: for<'a> From<&'a str> {

    let mut result = Vec::new();

    let path = format!("{}{}", prefix, name);

    let items_file = ::std::fs::File::open(&path).expect("didn't find items file");
    let mut items_reader =  ::std::io::BufReader::new(items_file);
    let mut count = 0;

    let mut line = String::new();

    while items_reader.read_line(&mut line).unwrap() > 0 {

        if count % peers == index {
            result.push(T::from(line.as_str()));
        }

        count += 1;
        line.clear();
    }

    result
}

pub mod types {

    use arrayvec::ArrayString;
    use abomonation::Abomonation;

    pub type Date = u32;

    #[inline(always)]
    pub fn create_date(year: u16, month: u8, day: u8) -> u32 {
        ((year as u32) << 16) + ((month as u32) << 8) + (day as u32)
    }

    fn parse_date(date: &str) -> Date {
        let delim = "-";
        let mut fields = date.split(&delim);
        let year = fields.next().unwrap().parse().unwrap();
        let month = fields.next().unwrap().parse().unwrap();
        let day = fields.next().unwrap().parse().unwrap();
        create_date(year, month, day)
    }

    fn copy_from_to(src: &[u8], dst: &mut [u8]) {
        let limit = if src.len() < dst.len() { src.len() } else { dst.len() };
        for index in 0 .. limit {
            dst[index] = src[index];
        }
    }

    pub fn read_u01(string: &str) -> [u8;1] { let mut buff = [0;1]; copy_from_to(string.as_bytes(), &mut buff); buff }
    pub fn read_u10(string: &str) -> [u8;10] { let mut buff = [0;10]; copy_from_to(string.as_bytes(), &mut buff); buff }
    pub fn read_u15(string: &str) -> [u8;15] { let mut buff = [0;15]; copy_from_to(string.as_bytes(), &mut buff); buff }
    pub fn read_u25(string: &str) -> [u8;25] { let mut buff = [0;25]; copy_from_to(string.as_bytes(), &mut buff); buff }

    unsafe_abomonate!(AbomonationWrapper<ArrayString<[u8; 25]>>);
    unsafe_abomonate!(AbomonationWrapper<ArrayString<[u8; 40]>>);
    unsafe_abomonate!(AbomonationWrapper<ArrayString<[u8; 128]>>);

    #[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash,Default)]
    pub struct AbomonationWrapper<T> {
        pub element: T,
    }

    use ::std::ops::Deref;
    impl<T> Deref for AbomonationWrapper<T> {
        type Target = T;
        fn deref(&self) -> &Self::Target {
            &self.element
        }
    }

    unsafe_abomonate!(Part);

    #[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Part {
        pub part_key: usize,
        pub name: ArrayString<[u8;56]>,
        pub mfgr: [u8; 25],
        pub brand: [u8; 10],
        pub typ: AbomonationWrapper<ArrayString<[u8;25]>>,
        pub size: i32,
        pub container: [u8; 10],
        pub retail_price: i64,
        pub comment: ArrayString<[u8;23]>,
    }

    impl<'a> From<&'a str> for Part {
        fn from(text: &'a str) -> Part {

            let delim = "|";
            let mut fields = text.split(&delim);

            Part {
                part_key: fields.next().unwrap().parse().unwrap(),
                name: ArrayString::from(fields.next().unwrap()).unwrap(),
                mfgr: read_u25(fields.next().unwrap()),
                brand: read_u10(fields.next().unwrap()),
                typ: AbomonationWrapper { element: ArrayString::from(fields.next().unwrap()).unwrap() },
                size: fields.next().unwrap().parse().unwrap(),
                container: read_u10(fields.next().unwrap()),
                retail_price: (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64,
                comment: ArrayString::from(fields.next().unwrap()).unwrap()
            }
        }
    }

    unsafe_abomonate!(Supplier);

    #[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Supplier {
        pub supp_key: usize,
        pub name: [u8; 25],
        pub address: AbomonationWrapper<ArrayString<[u8; 40]>>,
        pub nation_key: usize,
        pub phone: [u8; 15],
        pub acctbal: i64,
        pub comment: AbomonationWrapper<ArrayString<[u8; 128]>>,
    }

    impl<'a> From<&'a str> for Supplier {
        fn from(text: &'a str) -> Supplier {

            let delim = "|";
            let mut fields = text.split(&delim);

            Supplier {
                supp_key: fields.next().unwrap().parse().unwrap(),
                name: read_u25(fields.next().unwrap()),
                address: AbomonationWrapper { element: ArrayString::from(fields.next().unwrap()).unwrap() },
                nation_key: fields.next().unwrap().parse().unwrap(),
                phone: read_u15(fields.next().unwrap()),
                acctbal: (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64,
                comment: AbomonationWrapper { element: ArrayString::from(fields.next().unwrap()).unwrap() },
            }
        }
    }

    unsafe_abomonate!(PartSupp);

    #[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct PartSupp {
        pub part_key: usize,
        pub supp_key: usize,
        pub availqty: i32,
        pub supplycost: i64,
        pub comment: ArrayString<[u8; 224]>,
    }

    impl<'a> From<&'a str> for PartSupp {
        fn from(text: &'a str) -> PartSupp {

            let delim = "|";
            let mut fields = text.split(&delim);

            PartSupp {
                part_key: fields.next().unwrap().parse().unwrap(),
                supp_key: fields.next().unwrap().parse().unwrap(),
                availqty: fields.next().unwrap().parse().unwrap(),
                supplycost: (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64,
                comment: ArrayString::from(fields.next().unwrap()).unwrap(),
            }
        }
    }

    unsafe_abomonate!(Customer);

    #[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Customer {
        pub cust_key: usize,
        pub name: AbomonationWrapper<ArrayString<[u8;25]>>,
        pub address: AbomonationWrapper<ArrayString<[u8;40]>>,
        pub nation_key: usize,
        pub phone: [u8; 15],
        pub acctbal: i64,
        pub mktsegment: [u8; 10],
        pub comment: AbomonationWrapper<ArrayString<[u8;128]>>,
    }

    impl<'a> From<&'a str> for Customer {
        fn from(text: &'a str) -> Customer {

            // let mut result: Customer = Default::default();
            let delim = "|";
            let mut fields = text.split(&delim);

            Customer {
                cust_key: fields.next().unwrap().parse().unwrap(),
                name: AbomonationWrapper { element: ArrayString::from(fields.next().unwrap()).unwrap() },
                address: AbomonationWrapper { element: ArrayString::from(fields.next().unwrap()).unwrap() },
                nation_key: fields.next().unwrap().parse().unwrap(),
                phone: read_u15(fields.next().unwrap()),
                acctbal: (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64,
                mktsegment: read_u10(fields.next().unwrap()),
                comment: AbomonationWrapper { element: ArrayString::from(fields.next().unwrap()).unwrap() },
            }
        }
    }

    unsafe_abomonate!(Order);

    #[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Order {
        pub order_key: usize,
        pub cust_key: usize,
        pub order_status: [u8; 1],
        pub total_price: i64,
        pub order_date: Date,
        pub order_priority: [u8; 15],
        pub clerk: [u8; 15],
        pub ship_priority: i32,
        pub comment: ArrayString<[u8; 96]>,
    }

    impl<'a> From<&'a str> for Order {
        fn from(text: &'a str) -> Order {

            let delim = "|";
            let mut fields = text.split(&delim);

            Order {
                order_key: fields.next().unwrap().parse().unwrap(),
                cust_key: fields.next().unwrap().parse().unwrap(),
                order_status: read_u01(fields.next().unwrap()),
                total_price: (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64,
                order_date: parse_date(&fields.next().unwrap()),
                order_priority: read_u15(fields.next().unwrap()),
                clerk: read_u15(fields.next().unwrap()),
                ship_priority: fields.next().unwrap().parse().unwrap(),
                comment: ArrayString::from(fields.next().unwrap()).unwrap(),
            }
        }
    }

    unsafe_abomonate!(LineItem);

    #[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct LineItem {
        pub order_key: usize,
        pub part_key: usize,
        pub supp_key: usize,
        pub line_number: i32,
        pub quantity: i64,
        pub extended_price: i64,
        pub discount: i64,
        pub tax: i64,
        pub return_flag: [u8; 1],
        pub line_status: [u8; 1],
        pub ship_date: Date,
        pub commit_date: Date,
        pub receipt_date: Date,
        pub ship_instruct: [u8; 25],
        pub ship_mode: [u8; 10],
        pub comment: ArrayString<[u8; 48]>,
    }

    impl<'a> From<&'a str> for LineItem {
        fn from(text: &'a str) -> LineItem {

            let delim = "|";
            let mut fields = text.split(&delim);

            LineItem {
                order_key: fields.next().unwrap().parse().unwrap(),
                part_key: fields.next().unwrap().parse().unwrap(),
                supp_key: fields.next().unwrap().parse().unwrap(),
                line_number: fields.next().unwrap().parse().unwrap(),
                quantity: fields.next().unwrap().parse().unwrap(),
                // quantity: (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64,
                extended_price: (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64,
                discount: (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64,
                tax: (fields.next().unwrap().parse::<f64>().unwrap() * 100.0) as i64,
                return_flag: read_u01(fields.next().unwrap()),
                line_status: read_u01(fields.next().unwrap()),
                ship_date: parse_date(&fields.next().unwrap()),
                commit_date: parse_date(&fields.next().unwrap()),
                receipt_date: parse_date(&fields.next().unwrap()),
                ship_instruct: read_u25(fields.next().unwrap()),
                ship_mode: read_u10(fields.next().unwrap()),
                comment: ArrayString::from(fields.next().unwrap()).unwrap(),
            }
        }
    }

    unsafe_abomonate!(Nation);

    #[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Nation {
        pub nation_key: usize,
        pub name: [u8; 25],
        pub region_key: usize,
        // pub comment: String,
        pub comment: ArrayString<[u8;160]>,
    }

    impl<'a> From<&'a str> for Nation {
        fn from(text: &'a str) -> Nation {

            let delim = "|";
            let mut fields = text.split(&delim);

            Nation {
                nation_key: fields.next().unwrap().parse().unwrap(),
                name: read_u25(fields.next().unwrap()),
                region_key: fields.next().unwrap().parse().unwrap(),
                comment: ArrayString::from(fields.next().unwrap()).unwrap(),
            }
        }
    }

    unsafe_abomonate!(Region);

    #[derive(Ord,PartialOrd,Eq,PartialEq,Clone,Debug,Hash)]
    pub struct Region {
        pub region_key: usize,
        pub name: [u8; 25],
        pub comment: ArrayString<[u8;160]>,
    }

    impl<'a> From<&'a str> for Region {
        fn from(text: &'a str) -> Region {

            let delim = "|";
            let mut fields = text.split(&delim);

            Region {
                region_key: fields.next().unwrap().parse().unwrap(),
                name: read_u25(fields.next().unwrap()),
                comment: ArrayString::from(fields.next().unwrap()).unwrap(),
            }
        }
    }
}
