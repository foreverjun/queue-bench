/******************************************************************************
 * Copyright (c) 2014-2016, Pedro Ramalhete, Andreia Correia
 * All rights reserved.
 *
 * Rust Version by Junker Jörg
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Concurrency Freaks nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************
 */

/**
 * <h1> Fetch-And-Add Array Queue </h1>
 *
 * Each node has one array but we don't search for a vacant entry. Instead, we
 * use FAA to obtain an index in the array, for enqueueing or dequeuing.
 *
 * There are some similarities between this queue and the basic queue in YMC:
 * http://chaoran.me/assets/pdf/wfq-ppopp16.pdf
 * but it's not the same because the queue in listing 1 is obstruction-free, while
 * our algorithm is lock-free.
 * In FAAArrayQueue eventually a new node will be inserted (using Michael-Scott's
 * algorithm) and it will have an item pre-filled in the first position, which means
 * that at most, after BUFFER_SIZE steps, one item will be enqueued (and it can then
 * be dequeued). This kind of progress is lock-free.
 *
 * Each entry in the array may contain one of three possible values:
 * - A valid item that has been enqueued;
 * - nullptr, which means no item has yet been enqueued in that position;
 * - taken, a special value that means there was an item but it has been dequeued;
 *
 * Enqueue algorithm: FAA + CAS(null,item)
 * Dequeue algorithm: FAA + CAS(item,taken)
 * Consistency: Linearizable
 * enqueue() progress: lock-free
 * dequeue() progress: lock-free
 * Memory Reclamation: Hazard Pointers (lock-free)
 * Uncontended enqueue: 1 FAA + 1 CAS + 1 HP
 * Uncontended dequeue: 1 FAA + 1 CAS + 1 HP
 *
 *
 * <p>
 * Lock-Free Linked List as described in Maged Michael and Michael Scott's paper:
 * {@link http://www.cs.rochester.edu/~scott/papers/1996_PODC_queues.pdf}
 * <a href="http://www.cs.rochester.edu/~scott/papers/1996_PODC_queues.pdf">
 * Simple, Fast, and Practical Non-Blocking and Blocking Concurrent Queue Algorithms</a>
 * <p>
 * The paper on Hazard Pointers is named "Hazard Pointers: Safe Memory
 * Reclamation for Lock-Free objects" and it is available here:
 * http://web.cecs.pdx.edu/~walpole/class/cs510/papers/11.pdf
 *
 * @author Pedro Ramalhete
 * @author Andreia Correia
 */
use crossbeam_utils::CachePadded;
use haphazard::{AtomicPtr, Domain, Global, HazardPointer};
use std::{
    ptr,
    sync::atomic::{AtomicPtr as StdAtomicPtr, AtomicUsize, Ordering},
};

const BUFFER_SIZE: usize = 1024;

fn taken_ptr<T>() -> *mut T {
    1_usize as *mut T
}

struct Node<T> {
    deqidx: AtomicUsize,
    enqidx: AtomicUsize,
    next: AtomicPtr<Node<T>>,
    items: [StdAtomicPtr<T>; BUFFER_SIZE],
}

impl<T> Node<T> {
    unsafe fn new_with_item_ptr(item_ptr: *mut T) -> *mut Self {
        let items: [StdAtomicPtr<T>; BUFFER_SIZE] =
            std::array::from_fn(|_| StdAtomicPtr::new(ptr::null_mut()));
        items[0].store(item_ptr, Ordering::Relaxed);
        let boxed = Box::new(Node {
            deqidx: AtomicUsize::new(0),
            enqidx: AtomicUsize::new(1),
            next: AtomicPtr::new(ptr::null_mut()),
            items,
        });
        Box::into_raw(boxed)
    }

    unsafe fn new_empty() -> *mut Self {
        let items: [StdAtomicPtr<T>; BUFFER_SIZE] =
            std::array::from_fn(|_| StdAtomicPtr::new(ptr::null_mut()));
        let boxed = Box::new(Node {
            deqidx: AtomicUsize::new(0),
            enqidx: AtomicUsize::new(0),
            next: AtomicPtr::new(ptr::null_mut()),
            items,
        });
        Box::into_raw(boxed)
    }
}

trait RetireHelpers<T: Send + Sync> {
    unsafe fn retire_node(&self, node: *mut Node<T>);
}

impl<T: Send + Sync> RetireHelpers<T> for Domain<Global> {
    unsafe fn retire_node(&self, node: *mut Node<T>) {
        self.retire_ptr::<Node<T>, Box<Node<T>>>(node);
    }
}

pub struct FAAArrayQueue<T: Send + Sync + 'static> {
    head: CachePadded<AtomicPtr<Node<T>>>,
    tail: CachePadded<AtomicPtr<Node<T>>>,
    domain: &'static Domain<Global>,
}

impl<T: Send + Sync + 'static> FAAArrayQueue<T> {
    pub fn new() -> Self {
        let sentinel = unsafe { Node::new_empty() };
        FAAArrayQueue {
            head: CachePadded::new(unsafe { AtomicPtr::new(sentinel) }),
            tail: CachePadded::new(unsafe { AtomicPtr::new(sentinel) }),
            domain: Domain::global(),
        }
    }

    pub fn enqueue(&self, item_ptr: *mut T) {
        let mut hp_tail = HazardPointer::new_in_domain(self.domain);

        loop {
            let ltail = match unsafe { self.tail.load(&mut hp_tail) } {
                Some(nn) => nn,
                None => continue,
            };
            let ltail_ptr = ltail as *const _ as *mut _;

            let idx = ltail.enqidx.fetch_add(1, Ordering::AcqRel);
            if idx > BUFFER_SIZE - 1 {
                if self.tail.load_ptr() != ltail_ptr {
                    hp_tail.reset_protection();
                    continue;
                }
                let next_ptr = ltail.next.load_ptr();
                if next_ptr.is_null() {
                    let new_node = unsafe { Node::new_with_item_ptr(item_ptr) };
                    if unsafe { ltail.next.compare_exchange_ptr(ptr::null_mut(), new_node) }.is_ok()
                    {
                        let _ = unsafe { self.tail.compare_exchange_ptr(ltail_ptr, new_node) };
                        return;
                    }

                    unsafe {
                        drop(Box::from_raw(new_node));
                    }
                } else {
                    let _ = unsafe { self.tail.compare_exchange_ptr(ltail_ptr, next_ptr) };
                }
                continue;
            }
            if ltail.items[idx]
                .compare_exchange(
                    ptr::null_mut(),
                    item_ptr,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return;
            }
            hp_tail.reset_protection();
        }
    }

    pub fn dequeue(&self) -> *mut T {
        let mut hp_head = HazardPointer::new_in_domain(self.domain);
        let taken = taken_ptr::<T>();

        loop {
            let lhead = match unsafe { self.head.load(&mut hp_head) } {
                Some(nn) => nn,
                None => return ptr::null_mut(),
            };
            let lhead_ptr = lhead as *const _ as *mut _;
            let deqidx = lhead.deqidx.load(Ordering::Acquire);
            let enqidx = lhead.enqidx.load(Ordering::Acquire);
            if deqidx >= enqidx && lhead.next.load_ptr().is_null() {
                return ptr::null_mut();
            }

            let idx = lhead.deqidx.fetch_add(1, Ordering::AcqRel);
            if idx > BUFFER_SIZE - 1 {
                let lnext = lhead.next.load_ptr();
                if lnext.is_null() {
                    return ptr::null_mut();
                }
                if unsafe { self.head.compare_exchange_ptr(lhead_ptr, lnext) }.is_ok() {
                    unsafe { self.domain.retire_node(lhead_ptr) };
                }
                continue;
            }
            let item = lhead.items[idx].swap(taken, Ordering::AcqRel);
            if item.is_null() || item == taken {
                continue;
            }
            return item;
        }
    }
}

impl<T: Send + Sync + 'static> Drop for FAAArrayQueue<T> {
    fn drop(&mut self) {
        while !self.dequeue().is_null() {}
        let ptr = self.head.load_ptr();
        if !ptr.is_null() {
            unsafe { self.domain.retire_node(ptr) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    const THREADS: usize = 4;
    const PER_THREAD: usize = 10_000;

    #[test]
    fn test_single_thread_basic() {
        let q = FAAArrayQueue::<usize>::new();
        assert!(q.dequeue().is_null());

        let p1 = Box::into_raw(Box::new(42));
        q.enqueue(p1);
        let r1 = q.dequeue();
        assert_eq!(unsafe { *r1 }, 42);
        unsafe { drop(Box::from_raw(r1)) };

        assert!(q.dequeue().is_null());
    }

    #[test]
    fn test_multiple_segments() {
        let q = FAAArrayQueue::<usize>::new();
        let total = BUFFER_SIZE * 3 + 50;
        for i in 0..total {
            let p = Box::into_raw(Box::new(i));
            q.enqueue(p);
        }
        for i in 0..total {
            let r = q.dequeue();
            assert_eq!(unsafe { *r }, i);
            unsafe { drop(Box::from_raw(r)) };
        }
        assert!(q.dequeue().is_null());
    }

    #[test]
    fn test_concurrent_producers_consumers() {
        let q = Arc::new(FAAArrayQueue::<usize>::new());
        let barrier = Arc::new(Barrier::new(THREADS * 2));
        let mut handles = Vec::with_capacity(THREADS * 2);

        for t in 0..THREADS {
            let q_clone = q.clone();
            let cbar = barrier.clone();
            handles.push(thread::spawn(move || {
                cbar.wait();
                for i in 0..PER_THREAD {
                    let p = Box::into_raw(Box::new(t * PER_THREAD + i));
                    q_clone.enqueue(p);
                }
            }));
        }

        let results = Arc::new(std::sync::Mutex::new(Vec::new()));
        for _ in 0..THREADS {
            let q_clone = q.clone();
            let cbar = barrier.clone();
            let res = results.clone();
            handles.push(thread::spawn(move || {
                cbar.wait();
                for _ in 0..PER_THREAD {
                    loop {
                        let r = q_clone.dequeue();
                        if !r.is_null() {
                            let val = unsafe { *r };
                            unsafe { drop(Box::from_raw(r)) };
                            res.lock().unwrap().push(val);
                            break;
                        }
                        thread::yield_now();
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        let collected = results.lock().unwrap();
        assert_eq!(collected.len(), THREADS * PER_THREAD);
        let mut sorted = collected.clone();
        sorted.sort_unstable();
        for (i, v) in sorted.iter().enumerate() {
            assert_eq!(*v, i);
        }

        assert!(q.dequeue().is_null());
    }
}
