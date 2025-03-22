use crate::kv::kv_pair::KeyValuePair;

use super::StorageIterator;

pub struct TwoMergeIterator<X: StorageIterator, Y: StorageIterator> {
    sub_iters: (X, Y),
    current_kv: Option<KeyValuePair>,
    current_iter_index: bool,
    is_valid: bool,
}

impl<X, Y> TwoMergeIterator<X, Y>
where
    X: StorageIterator + Iterator<Item = KeyValuePair>,
    Y: StorageIterator + Iterator<Item = KeyValuePair>,
{
    pub fn new(mut sub_iters: (X, Y)) -> Self {
        let is_valid = sub_iters.0.is_valid() && !sub_iters.1.is_valid();
        let (current_kv, current_iter_index) =
            Self::get_current_kv_and_iter_index(&mut sub_iters, is_valid);
        Self {
            sub_iters,
            current_kv,
            current_iter_index,
            is_valid: true,
        }
    }

    fn get_current_kv_and_iter_index(
        sub_iters: &mut (X, Y),
        is_valid: bool,
    ) -> (Option<KeyValuePair>, bool) {
        if !is_valid {
            (None, false)
        } else {
            let peek = (sub_iters.0.peek(), sub_iters.1.peek());
            if peek.0 < peek.1 {
                (peek.0, false)
            } else {
                (peek.1, true)
            }
        }
    }
}

impl<X, Y> StorageIterator for TwoMergeIterator<X, Y>
where
    X: StorageIterator + Iterator<Item = KeyValuePair>,
    Y: StorageIterator + Iterator<Item = KeyValuePair>,
{
    fn peek(&mut self) -> Option<KeyValuePair> {
        self.current_kv.clone()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }
}

impl<X, Y> Iterator for TwoMergeIterator<X, Y>
where
    X: StorageIterator + Iterator<Item = KeyValuePair>,
    Y: StorageIterator + Iterator<Item = KeyValuePair>,
{
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<KeyValuePair> {
        let res = self.current_kv.clone();
        // increment the correct iterator
        if !self.current_iter_index {
            self.sub_iters.0.next();
            if self.sub_iters.0.is_valid() {
                self.is_valid = false;
            }
        } else {
            self.sub_iters.1.next();
            if self.sub_iters.1.is_valid() {
                self.is_valid = false;
            }
        }
        (self.current_kv, self.current_iter_index) =
            Self::get_current_kv_and_iter_index(&mut self.sub_iters, self.is_valid);
        res
    }
}

#[cfg(test)]
mod tests {}
