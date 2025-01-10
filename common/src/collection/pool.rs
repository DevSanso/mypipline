mod owned_pool;

use std::sync::Arc;
use std::error::Error;

pub trait PoolItem<T> {
    fn get_value<'b>(&'b mut self) -> &'b mut T;
    fn dispose(&mut self);
    fn restoration(&mut self);
}
pub trait ThreadSafePool<T,P>  where T : 'static, P: 'static {
    fn get_owned(&self, param : P) -> Result<Box<dyn PoolItem<T>>, Box<dyn Error>>;
    fn alloc_size(&self) -> usize;
    fn max_size(&self) -> usize;
}

pub fn get_thread_safe_pool<T : 'static,P : 'static>(name : String, gen : Box<dyn Fn(P) -> Option<T>>, max_size : usize) -> Arc<dyn ThreadSafePool<T,P>> {
    owned_pool::OwnedPool::new(name, gen, max_size)
}



