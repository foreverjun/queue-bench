pub mod faa_ebr_impl;
pub mod faa_hazard_impl;
pub mod bq_impl;

#[derive(Clone)]
pub struct SendPtr<T>(*mut T);

unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

impl<T> SendPtr<T> {
    pub fn new(ptr: *mut T) -> Self {
        SendPtr(ptr)
    }

    pub fn as_ptr(&self) -> *mut T {
        self.0
    }
}

pub trait BatchQueue {
    type Item;
    fn enqueue_batch(&self, items: &[SendPtr<Self::Item>]);
    fn dequeue_batch(&self, max_to_dequeue: usize) -> usize;
}
