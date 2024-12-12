use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct IDGenerator {
    id: Arc<AtomicU64>,
}

impl IDGenerator {
    pub fn new(init: u64) -> Self {
        Self {
            id: Arc::new(AtomicU64::new(init)),
        }
    }

    pub fn set(&self, val: u64) {
        self.id.store(val, Ordering::Release);
    }

    pub fn next_id(&self) -> u64 {
        self.id.fetch_add(1, Ordering::SeqCst)
    }
}
