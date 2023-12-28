// Copyright Rouven Bauer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;

use super::value_send::ValueSend;

pub trait ValueMap: Debug {
    fn items(&self) -> impl Iterator<Item = (&'_ str, &'_ ValueSend)> + '_;
    fn items_len(&self) -> usize {
        self.items().count()
    }
}

impl<M: ValueMap> ValueMap for &M {
    #[inline]
    fn items(&self) -> impl Iterator<Item = (&'_ str, &'_ ValueSend)> + '_ {
        (*self).items()
    }
    #[inline]
    fn items_len(&self) -> usize {
        (*self).items_len()
    }
}

impl<K: Borrow<str> + Debug, V: Borrow<ValueSend> + Debug> ValueMap for HashMap<K, V> {
    #[inline]
    fn items(&self) -> impl Iterator<Item = (&'_ str, &'_ ValueSend)> + '_ {
        self.iter().map(|(k, v)| (k.borrow(), v.borrow()))
    }
    #[inline]
    fn items_len(&self) -> usize {
        self.len()
    }
}

impl<K: Borrow<str> + Debug, V: Borrow<ValueSend> + Debug> ValueMap for BTreeMap<K, V> {
    #[inline]
    fn items(&self) -> impl Iterator<Item = (&'_ str, &'_ ValueSend)> + '_ {
        self.iter().map(|(k, v)| (k.borrow(), v.borrow()))
    }
    #[inline]
    fn items_len(&self) -> usize {
        self.len()
    }
}

impl<K: Borrow<str> + Debug, V: Borrow<ValueSend> + Debug> ValueMap for Vec<(K, V)> {
    #[inline]
    fn items(&self) -> impl Iterator<Item = (&'_ str, &'_ ValueSend)> + '_ {
        self.iter().map(|(k, v)| (k.borrow(), v.borrow()))
    }
    #[inline]
    fn items_len(&self) -> usize {
        self.len()
    }
}

impl<const N: usize, K: Borrow<str> + Debug, V: Borrow<ValueSend> + Debug> ValueMap
    for [(K, V); N]
{
    #[inline]
    fn items(&self) -> impl Iterator<Item = (&'_ str, &'_ ValueSend)> + '_ {
        self.iter().map(|(k, v)| (k.borrow(), v.borrow()))
    }
    #[inline]
    fn items_len(&self) -> usize {
        self.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compare_as_items(map: impl ValueMap, expected: &[(String, ValueSend)]) {
        let mut map_vec = map.items().collect::<Vec<_>>();
        let mut expected_vec = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect::<Vec<_>>();
        map_vec.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        expected_vec.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        assert_eq!(map_vec, expected_vec);
    }

    fn get_expected() -> [(String, ValueSend); 2] {
        [
            (String::from("a"), ValueSend::Null),
            (String::from("b"), ValueSend::Integer(1)),
        ]
    }

    // -------------
    // -- HashMap --
    // -------------

    #[test]
    fn test_str_value_ref_hash_map_ref_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect::<HashMap<_, _>>();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_str_value_hash_map_ref_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect::<HashMap<_, _>>();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_string_value_ref_hash_map_ref_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect::<HashMap<_, _>>();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_string_value_hash_map_ref_as_value_map() {
        let expected = get_expected();
        let map = expected.clone();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_str_value_ref_hash_map_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect::<HashMap<_, _>>();
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_str_value_hash_map_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect::<HashMap<_, _>>();
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_string_value_ref_hash_map_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect::<HashMap<_, _>>();
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_string_value_hash_map_as_value_map() {
        let expected = get_expected();
        let map = expected.iter().cloned().collect::<HashMap<_, _>>();
        compare_as_items(map, &expected);
    }

    // --------------
    // -- BTreeMap --
    // --------------

    #[test]
    fn test_str_value_ref_b_tree_map_ref_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect::<BTreeMap<_, _>>();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_str_value_b_tree_map_ref_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect::<BTreeMap<_, _>>();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_string_value_ref_b_tree_map_ref_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect::<BTreeMap<_, _>>();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_string_value_b_tree_map_ref_as_value_map() {
        let expected = get_expected();
        let map = expected.clone();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_str_value_ref_b_tree_map_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect::<BTreeMap<_, _>>();
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_str_value_b_tree_map_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect::<BTreeMap<_, _>>();
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_string_value_ref_b_tree_map_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect::<BTreeMap<_, _>>();
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_string_value_b_tree_map_as_value_map() {
        let expected = get_expected();
        let map = expected.iter().cloned().collect::<BTreeMap<_, _>>();
        compare_as_items(map, &expected);
    }

    // -----------------
    // -- Vec<(K, V)> --
    // -----------------

    #[test]
    fn test_str_value_ref_vec_ref_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect::<Vec<_>>();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_str_value_vec_ref_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect::<Vec<_>>();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_string_value_ref_vec_ref_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect::<Vec<_>>();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_string_value_vec_ref_as_value_map() {
        let expected = get_expected();
        let map = expected.clone();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_str_value_ref_vec_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect::<Vec<_>>();
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_str_value_vec_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect::<Vec<_>>();
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_string_value_ref_vec_as_value_map() {
        let expected = get_expected();
        let map = expected
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect::<Vec<_>>();
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_string_value_vec_as_value_map() {
        let expected = get_expected();
        let map: Vec<(String, ValueSend)> = expected.to_vec();
        compare_as_items(map, &expected);
    }

    // -----------------
    // -- [(K, V); N] --
    // -----------------

    fn map_each<'a, const N: usize, T: 'a, F: Fn(&'a T) -> R, R: Debug + 'a>(
        array: &'a [T; N],
        f: F,
    ) -> [R; N] {
        array.iter().map(f).collect::<Vec<_>>().try_into().unwrap()
    }

    #[test]
    fn test_str_value_ref_array_ref_as_value_map() {
        let expected = get_expected();
        let map = map_each(&expected, |(k, v)| (k.as_str(), v));
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_str_value_array_ref_as_value_map() {
        let expected = get_expected();
        let map = map_each(&expected, |(k, v)| (k.as_str(), v.clone()));
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_string_value_ref_array_ref_as_value_map() {
        let expected = get_expected();
        let map = map_each(&expected, |(k, v)| (k.clone(), v));
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_string_value_array_ref_as_value_map() {
        let expected = get_expected();
        let map = expected.clone();
        #[allow(clippy::needless_borrows_for_generic_args)] // for testing purposes
        compare_as_items(&map, &expected);
    }

    #[test]
    fn test_str_value_ref_array_as_value_map() {
        let expected = get_expected();
        let map = map_each(&expected, |(k, v)| (k.as_str(), v));
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_str_value_array_as_value_map() {
        let expected = get_expected();
        let map = map_each(&expected, |(k, v)| (k.as_str(), v.clone()));
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_string_value_ref_array_as_value_map() {
        let expected = get_expected();
        let map = map_each(&expected, |(k, v)| (k.clone(), v));
        compare_as_items(map, &expected);
    }

    #[test]
    fn test_string_value_array_as_value_map() {
        let expected = get_expected();
        let map = expected.clone();
        compare_as_items(map, &expected);
    }
}
