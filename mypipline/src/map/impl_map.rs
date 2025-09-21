use std::collections::HashMap;
use std::marker::PhantomData;
use std::cmp::Eq;
use std::hash::Hash;
use std::cell::RefCell;
use std::error::Error;


pub type CallFn<T, P, R>  = dyn Fn(Option<&mut T>, Option<P>) -> Result<R, Box<dyn Error>>;
pub type CallMapInitFn<K,T, T2>  = dyn Fn(HashMap<K,T2>) -> HashMap<K,T>;

pub struct CallMap<K : Eq + Hash + Clone, T, T2> where T : 'static, T2 : 'static {
    real_map : RefCell<HashMap<K, T>>,
    
    _marker : PhantomData<T2>
}

impl<K : Eq + Hash + Clone, T, T2> CallMap<K, T, T2>  where T : 'static, T2 : 'static  {
    pub fn init(param : HashMap<K,T2>, init_callback : &'static CallMapInitFn<K,T,T2>) -> Self {
        CallMap { real_map: RefCell::new(init_callback(param)), _marker: PhantomData::default() }
    }

    pub fn call_fn<P, R>(&self, key : &K, param : Option<P>, func : & CallFn<T,P,R>) -> Result<R, Box<dyn Error>> {
        let mut h = self.real_map.borrow_mut();
        func(h.get_mut(key), param)
        
    }

    pub fn keys(&self) -> Vec<K> {
        let b = self.real_map.borrow();
        let mut ks = b.keys();
        let l = ks.by_ref().count();
        ks.fold(Vec::with_capacity(l), |mut acc, x| {
            acc.push(x.clone());
            acc
        })
    }
}