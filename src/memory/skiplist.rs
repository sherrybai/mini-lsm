use std::ptr::NonNull;
use anyhow::Result;

type Link<T> = Option<NonNull<T>>;

pub struct SkipList<K, V> {
    head: NonNull<Head<K, V>>,
    max_level: usize
}

impl<K, V> SkipList<K, V> {
    pub fn new(max_level: usize) -> Self {
        Self {
            head: NonNull::new(&mut Head::new(max_level)).expect("head pointer is null"),
            max_level: max_level
        }
    }
}

pub struct Head<K, V> {
    forward: Vec<Link<SkipNode<K, V>>>
}

impl<K, V> Head<K, V> {
    pub fn new(max_level: usize) -> Self {
        let forward: Vec<Link<SkipNode<K, V>>> = vec![None; max_level];
        Head { forward }
    }

    pub fn get(self, key: K) -> Option<V> {
        None
    }

    pub fn insert(self, key: K, value: V) -> Result<()> {
        
        Ok(())
    }
}

pub struct SkipNode<K, V> {
    key: K,
    value: V,
    forward: Vec<Link<SkipNode<K, V>>>
}

impl<K, V> SkipNode<K, V> {
    pub fn new(key: K, value: V) -> Self {
        SkipNode { key, value, forward: Vec::new() }
    }
}