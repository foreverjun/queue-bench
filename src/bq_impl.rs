use std::sync::Arc;

use crate::BatchQueue;
use crate::SendPtr;
use bq::bq::BQueue;

pub struct BQ<T> {
    items: Arc<BQueue<SendPtr <T>>>,
}
unsafe impl<T> Send for BQ<T> {}
unsafe impl<T> Sync for BQ<T> {}

impl BQ<u8> {
    pub fn new() -> Self {
        BQ {
            items: Arc::new(BQueue::new()),
        }
    }
}

impl<T: Send + Sync + Clone> BatchQueue for BQ<T> {
    type Item = T;

    fn enqueue_batch(&self, items_to_enqueue: &[SendPtr<Self::Item>]) {
        self.items.enqueue_batch(items_to_enqueue.iter().cloned());
    }

    fn dequeue_batch(&self, max_to_dequeue: usize) -> usize {
        let mut deq_count = 0;
        let mut iter = self.items.deq_batch(max_to_dequeue);
        while iter.next().is_some() {
            deq_count+=1;
        }
        deq_count
    }
}