use std::slice::{from_raw_parts};

pub struct VecQueue<T> {
    list: Vec<T>,
    head: usize,
    tail: usize,
}

impl<T> VecQueue<T> {
    #[inline(always)]
    pub fn new() -> Self { VecQueue::from(Vec::new()) }
    #[inline(always)]
    pub fn pop(&mut self) -> T {
        debug_assert!(self.head < self.tail);
        self.head += 1;
        unsafe { ::std::ptr::read(self.list.as_mut_ptr().offset((self.head as isize) - 1)) }
    }
    #[inline(always)]
    pub fn peek(&self) -> &T {
        debug_assert!(self.head < self.tail);
        unsafe { self.list.get_unchecked(self.head) }
    }
    #[inline(always)]
    pub fn _peek_tail(&self) -> &T {
        debug_assert!(self.head < self.tail);
        unsafe { self.list.get_unchecked(self.tail-1) }
    }
    #[inline(always)]
    pub fn _slice(&self) -> &[T] {
        debug_assert!(self.head < self.tail);
        unsafe { from_raw_parts(self.list.get_unchecked(self.head), self.tail - self.head) }
    }
    #[inline(always)]
    pub fn from(mut list: Vec<T>) -> Self {
        let tail = list.len();
        unsafe { list.set_len(0); }
        VecQueue {
            list: list,
            head: 0,
            tail: tail,
        }
    }
    // could leak, if self.head != self.tail.
    #[inline(always)]
    pub fn done(self) -> Vec<T> {
        debug_assert!(self.head == self.tail);
        self.list
    }
    #[inline(always)]
    pub fn len(&self) -> usize { self.tail - self.head }
    #[inline(always)]
    pub fn is_empty(&self) -> bool { self.head == self.tail }
}

#[inline(always)]
unsafe fn push_unchecked<T>(vec: &mut Vec<T>, element: T) {
    debug_assert!(vec.len() < vec.capacity());
    let len = vec.len();
    ::std::ptr::write(vec.get_unchecked_mut(len), element);
    vec.set_len(len + 1);
}

pub struct MergeSorter<T: Ord> {
    queue: Vec<Vec<Vec<(T, i64)>>>,    // each power-of-two length list of allocations.
    stash: Vec<Vec<(T, i64)>>,
}

impl<T: Ord> MergeSorter<T> {

    #[inline]
    pub fn new() -> Self { MergeSorter { queue: Vec::new(), stash: Vec::new() } }

    #[inline(never)]
    pub fn _sort(&mut self, list: &mut Vec<Vec<(T, i64)>>) {
        for mut batch in list.drain(..) {
            self.push(&mut batch);
        }
        self.finish_into(list);
    }

    #[inline]
    pub fn push(&mut self, batch: &mut Vec<(T, i64)>) {

        let mut batch = if self.stash.len() > 2 {
            ::std::mem::replace(batch, self.stash.pop().unwrap())
        }
        else {
            ::std::mem::replace(batch, Vec::new())
        };

        if batch.len() > 0 {
            batch.sort_unstable_by(|x,y| x.0.cmp(&y.0));
            for index in 1 .. batch.len() {
                if batch[index].0 == batch[index - 1].0 {
                    batch[index].1 = batch[index].1 + batch[index - 1].1;
                    batch[index - 1].1 = 0;
                }
            }
            batch.retain(|x| x.1 != 0);

            self.queue.push(vec![batch]);
            while self.queue.len() > 1 && (self.queue[self.queue.len()-1].len() >= self.queue[self.queue.len()-2].len() / 2) {
                let list1 = self.queue.pop().unwrap();
                let list2 = self.queue.pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue.push(merged);
            }
        }
    }

    // This is awkward, because it isn't a power-of-two length any more, and we don't want
    // to break it down to be so.
    pub fn _push_list(&mut self, list: Vec<Vec<(T, i64)>>) {
        while self.queue.len() > 1 && self.queue[self.queue.len()-1].len() < list.len() {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);
        }
        self.queue.push(list);
    }

    #[inline(never)]
    pub fn finish_into(&mut self, target: &mut Vec<Vec<(T, i64)>>) {
        while self.queue.len() > 1 {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);
        }

        if let Some(mut last) = self.queue.pop() {
            ::std::mem::swap(&mut last, target);
        }
    }

    // merges two sorted input lists into one sorted output list.
    #[inline(never)]
    fn merge_by(&mut self, list1: Vec<Vec<(T, i64)>>, list2: Vec<Vec<(T, i64)>>) -> Vec<Vec<(T, i64)>> {

        use std::cmp::Ordering;

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024));

        let mut list1 = VecQueue::from(list1);
        let mut list2 = VecQueue::from(list2);

        let mut head1 = if !list1.is_empty() { VecQueue::from(list1.pop()) } else { VecQueue::new() };
        let mut head2 = if !list2.is_empty() { VecQueue::from(list2.pop()) } else { VecQueue::new() };

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {

            while (result.capacity() - result.len()) > 0 && head1.len() > 0 && head2.len() > 0 {

                let cmp = {
                    let x = head1.peek();
                    let y = head2.peek();
                    x.0.cmp(&y.0)
                };
                match cmp {
                    Ordering::Less    => { unsafe { push_unchecked(&mut result, head1.pop()); } }
                    Ordering::Greater => { unsafe { push_unchecked(&mut result, head2.pop()); } }
                    Ordering::Equal   => {
                        let (data1, diff1) = head1.pop();
                        let (_data2, diff2) = head2.pop();
                        let diff = diff1 + diff2;
                        if diff != 0 {
                            unsafe { push_unchecked(&mut result, (data1, diff)); }
                        }
                    }
                }
            }

            if result.capacity() == result.len() {
                output.push(result);
                result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024));
            }

            if head1.is_empty() {
                let done1 = head1.done();
                if done1.capacity() == 1024 { self.stash.push(done1); }
                head1 = if !list1.is_empty() { VecQueue::from(list1.pop()) } else { VecQueue::new() };
            }
            if head2.is_empty() {
                let done2 = head2.done();
                if done2.capacity() == 1024 { self.stash.push(done2); }
                head2 = if !list2.is_empty() { VecQueue::from(list2.pop()) } else { VecQueue::new() };
            }
        }

        if result.len() > 0 { output.push(result); }
        else if result.capacity() > 0 { self.stash.push(result); }

        if !head1.is_empty() {
            let mut result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024));
            for _ in 0 .. head1.len() { result.push(head1.pop()); }
            output.push(result);
        }
        while !list1.is_empty() {
            output.push(list1.pop());
        }

        if !head2.is_empty() {
            let mut result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024));
            for _ in 0 .. head2.len() { result.push(head2.pop()); }
            output.push(result);
        }
        while !list2.is_empty() {
            output.push(list2.pop());
        }

        output
    }
}
