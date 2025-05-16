use std::sync::Arc;

use crate::{BatchQueue, SendPtr};
use faa_queue::faa_array_hazard::FAAArrayQueue;

pub struct FAAhazardBQ<T: Send + Sync + 'static> {
    items: Arc<FAAArrayQueue<T>>,
}

impl FAAhazardBQ<u8> {
    pub fn new() -> Self {
        FAAhazardBQ {
            items: Arc::new(FAAArrayQueue::new()),
        }
    }
}

impl<T: Send + Sync> BatchQueue for FAAhazardBQ<T> {
    type Item = T;

    fn enqueue_batch(&self, items_to_enqueue: &[SendPtr<Self::Item>]) {
        for item_ptr in items_to_enqueue {
            self.items.enqueue(item_ptr.as_ptr());
        }
    }

    fn dequeue_batch(&self, max_to_dequeue: usize) -> usize {
        let mut dequeued_count = 0;
        for _ in 0..max_to_dequeue {
            if !self.items.dequeue().is_null() {
                dequeued_count += 1;
            } else {
                break;
            }
        }
        dequeued_count
    }
}
