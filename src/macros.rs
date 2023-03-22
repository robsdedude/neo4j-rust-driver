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

// heavily inspired by [serde_json]'s `json!` macro
// [serde_json]: https://github.com/serde-rs/json

#[cfg(doc)]
use crate::Value;

macro_rules! map {
    () => {std::collections::HashMap::new()};
    ( $($key:expr => $value:expr),* $(,)? ) => {
        {
            let mut m = std::collections::HashMap::with_capacity(map!(_capacity($($value),*)));
            $(
                m.insert($key, $value);
            )*
            m
        }
    };
    ( _capacity() ) => (0usize);
    ( _capacity($x:tt) ) => (1usize);
    ( _capacity($x:tt, $($xs:tt),*) ) => (1usize + map!(_capacity($($xs),*)));
}

pub(crate) use map;

/// Short notation for creating a [`Value`].
///
/// # Examples
///
/// Special values:
/// ```
/// use neo4j::{value, Value};
///
/// // null
/// assert_eq!(Value::Null, value!(null));
///
/// // true, false
/// assert_eq!(Value::Boolean(true), value!(true));
/// assert_eq!(Value::Boolean(false), value!(false));
/// ```
///
/// Any value that implements `Into<Value>`:
/// ```
/// use neo4j::{value, Value};
///
/// // integers
/// assert_eq!(Value::Integer(1), value!(1));
///
/// // floats
/// assert_eq!(Value::Float(1.234), value!(1.234));
///
/// // strings
/// assert_eq!(Value::String(String::from("foo")), value!("foo"));
/// let foo = String::from("foo");
/// assert_eq!(Value::String(foo.clone()), value!(foo));
///
/// // spatial types
/// use neo4j::spatial::Cartesian2D;
///
/// assert_eq!(
///     Value::Cartesian2D(Cartesian2D::new(1., 2.)),
///     value!(Cartesian2D::new(1., 2.))
/// )
/// ```
///
/// Create [`Value::Bytes`]:
/// ```
/// use neo4j::{value, Value};
///
/// assert_eq!(Value::Bytes(vec![1, 2, 3]), value!(bytes(1, 2, 3)));
/// ```
///
/// Create a [`Value::List`]:
/// ```
/// use neo4j::{value, Value};
///
/// assert_eq!(
///     Value::List(vec![
///         Value::Integer(1),
///         Value::Float(2.),
///         Value::Null
///     ]),
///     value!([1, 2., null])
/// );
/// ```
///
/// Create a [`Value::Map`]:
/// ```
/// use std::collections::HashMap;
/// use neo4j::{value, Value};
///
/// let mut map = HashMap::new();
/// map.insert(String::from("foo"), Value::Integer(1));
/// map.insert(String::from("bar"), Value::Null);
/// map.insert(String::from("baz"), Value::List(vec![Value::Integer(1)]));
///
/// assert_eq!(
///     Value::Map(map),
///     value!({"foo": 1, "bar": null, "baz": [1]})
/// );
/// ```
#[macro_export(local_inner_macros)]
macro_rules! value {
    // Hide distracting implementation details from the generated rustdoc.
    ($($value:tt)+) => {
        __value_internal!($($value)+)
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __value_internal {
    //////////////////////////////////////////////////////////////////////////
    // TT muncher for parsing the inside of a list [...].
    // Produces a Value::List(vec![...]) of the elements.
    //
    // Must be invoked as: __value_internal!(@list [] $($tt)*)
    //////////////////////////////////////////////////////////////////////////

    // Done with trailing comma.
    (@list [$($elems:expr,)*]) => {
        vec![$($elems,)*]
    };

    // Done without trailing comma.
    (@list [$($elems:expr),*]) => {
        vec![$($elems),*]
    };

    // Next element is `null`.
    (@list [$($elems:expr,)*] null $($rest:tt)*) => {
        $crate::__value_internal!(@list [$($elems,)* $crate::__value_internal!(null)] $($rest)*)
    };

    // Next element is a list.
    (@list [$($elems:expr,)*] [$($array:tt)*] $($rest:tt)*) => {
        $crate::__value_internal!(@list [$($elems,)* $crate::__value_internal!([$($array)*])] $($rest)*)
    };

    // Next element is a map.
    (@list [$($elems:expr,)*] {$($map:tt)*} $($rest:tt)*) => {
        $crate::__value_internal!(@list [$($elems,)* $crate::__value_internal!({$($map)*})] $($rest)*)
    };

    // Next element are bytes.
    (@list [$($elems:expr,)*] bytes($($bytes:tt)*) $($rest:tt)*) => {
        $crate::__value_internal!(@list [$($elems,)* $crate::__value_internal!(bytes($($bytes)*))] $($rest)*)
    };

    // Next element is an expression followed by comma.
    (@list [$($elems:expr,)*] $next:expr, $($rest:tt)*) => {
        $crate::__value_internal!(@list [$($elems,)* $crate::__value_internal!($next),] $($rest)*)
    };

    // Last element is an expression with no trailing comma.
    (@list [$($elems:expr,)*] $last:expr) => {
        $crate::__value_internal!(@list [$($elems,)* $crate::__value_internal!($last)])
    };

    // Comma after the most recent element.
    (@list [$($elems:expr),*] , $($rest:tt)*) => {
        $crate::__value_internal!(@list [$($elems,)*] $($rest)*)
    };

    // Unexpected token after most recent element.
    (@list [$($elems:expr),*] $unexpected:tt $($rest:tt)*) => {
        $crate::__value_unexpected!($unexpected)
    };

    //////////////////////////////////////////////////////////////////////////
    // TT muncher for parsing the inside of an map {...}. Each entry is
    // inserted into the given map variable.
    //
    // Must be invoked as: __value_internal!(@map $map () ($($tt)*) ($($tt)*))
    //
    // We require two copies of the input tokens so that we can match on one
    // copy and trigger errors on the other copy.
    //////////////////////////////////////////////////////////////////////////

    // Done.
    (@map $map:ident () () ()) => {};

    // Insert the current entry followed by trailing comma.
    (@map $map:ident [$($key:tt)+] ($value:expr) , $($rest:tt)*) => {
        let _ = $map.insert(($($key)+).into(), $value);
        $crate::__value_internal!(@map $map () ($($rest)*) ($($rest)*));
    };

    // Current entry followed by unexpected token.
    (@map $map:ident [$($key:tt)+] ($value:expr) $unexpected:tt $($rest:tt)*) => {
        $crate::__value_unexpected!($unexpected);
    };

    // Insert the last entry without trailing comma.
    (@map $map:ident [$($key:tt)+] ($value:expr)) => {
        let _ = $map.insert(($($key)+).into(), $value);
    };

    // Next value is `null`.
    (@map $map:ident ($($key:tt)+) (: null $($rest:tt)*) $copy:tt) => {
        $crate::__value_internal!(@map $map [$($key)+] ($crate::__value_internal!(null)) $($rest)*);
    };

    // Next value is a list.
    (@map $map:ident ($($key:tt)+) (: [$($array:tt)*] $($rest:tt)*) $copy:tt) => {
        $crate::__value_internal!(@map $map [$($key)+] ($crate::__value_internal!([$($array)*])) $($rest)*);
    };

    // Next value is a map.
    (@map $map:ident ($($key:tt)+) (: {$($mapp:tt)*} $($rest:tt)*) $copy:tt) => {
        $crate::__value_internal!(@map $map [$($key)+] ($crate::__value_internal!({$($mapp)*})) $($rest)*);
    };

    // Next value are bytes.
    (@map $map:ident ($($key:tt)+) (: bytes( $($bytes:tt)* ) $($rest:tt)*) $copy:tt) => {
        $crate::__value_internal!(@map $map [$($key)+] ($crate::__value_internal!(bytes($($bytes)*))) $($rest)*);
    };

    // Next value is an expression followed by comma.
    (@map $map:ident ($($key:tt)+) (: $value:expr , $($rest:tt)*) $copy:tt) => {
        $crate::__value_internal!(@map $map [$($key)+] ($crate::__value_internal!($value)) , $($rest)*);
    };

    // Last value is an expression with no trailing comma.
    (@map $map:ident ($($key:tt)+) (: $value:expr) $copy:tt) => {
        $crate::__value_internal!(@map $map [$($key)+] ($crate::__value_internal!($value)));
    };

    // Missing value for last entry. Trigger a reasonable error message.
    (@map $map:ident ($($key:tt)+) (:) $copy:tt) => {
        // "unexpected end of macro invocation"
        $crate::__value_internal!();
    };

    // Missing colon and value for last entry. Trigger a reasonable error
    // message.
    (@map $map:ident ($($key:tt)+) () $copy:tt) => {
        // "unexpected end of macro invocation"
        $crate::__value_internal!();
    };

    // Misplaced colon. Trigger a reasonable error message.
    (@map $map:ident () (: $($rest:tt)*) ($colon:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `:`".
        $crate::__value_unexpected!($colon);
    };

    // Found a comma inside a key. Trigger a reasonable error message.
    (@map $map:ident ($($key:tt)*) (, $($rest:tt)*) ($comma:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `,`".
        $crate::__value_unexpected!($comma);
    };

    // Key is fully parenthesized. This avoids clippy double_parens false
    // positives because the parenthesization may be necessary here.
    (@map $map:ident () (($key:expr) : $($rest:tt)*) $copy:tt) => {
        $crate::__value_internal!(@map $map ($key) (: $($rest)*) (: $($rest)*));
    };

    // Refuse to absorb colon token into key expression.
    (@map $map:ident ($($key:tt)*) (: $($unexpected:tt)+) $copy:tt) => {
        $crate::value_expect_expr_comma!($($unexpected)+);
    };

    // Munch a token into the current key.
    (@map $map:ident ($($key:tt)*) ($tt:tt $($rest:tt)*) $copy:tt) => {
        $crate::__value_internal!(@map $map ($($key)* $tt) ($($rest)*) ($($rest)*));
    };

    //////////////////////////////////////////////////////////////////////////
    // The main implementation.
    //
    // Must be invoked as: __value_internal!($($value)+)
    //////////////////////////////////////////////////////////////////////////

    (null) => {
        $crate::Value::Null
    };

    ([]) => {
        $crate::Value::List(vec![])
    };

    ([ $($tt:tt)+ ]) => {
        $crate::Value::List($crate::__value_internal!(@list [] $($tt)+))
    };

    ({$(,)?}) => {
        $crate::Value::Map(std::collections::HashMap::new())
    };

    ({ $($tt:tt)+ }) => {
        $crate::Value::Map({
            let mut map = std::collections::HashMap::new();
            $crate::__value_internal!(@map map () ($($tt)+) ($($tt)+));
            map
        })
    };

    (bytes()) => {
        $crate::Value::Bytes(vec![])
    };

    (bytes( $($tt:tt),+ $(,)?)) => {
        $crate::Value::Bytes(vec![$($tt),+])
    };

    // Any Serialize type: numbers, strings, struct literals, variables etc.
    // Must be below every other rule.
    ($other:expr) => {
        $other.into()
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __value_unexpected {
    () => {};
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::spatial::*;
    use crate::Value;

    #[test]
    fn test_null() {
        assert_eq!(value!(null), Value::Null)
    }

    #[rstest]
    #[case(value!(true), Value::Boolean(true))]
    #[case(value!(false), Value::Boolean(false))]
    fn test_boolean(#[case] input: Value, #[case] output: Value) {
        dbg!(&input, &output);
        assert_eq!(input, output);
    }

    #[rstest]
    #[case(value!(1), Value::Integer(1))]
    #[case(value!(-1), Value::Integer(-1))]
    #[case(value!(1u8), Value::Integer(1))]
    #[case(value!(1i8), Value::Integer(1))]
    #[case(value!(-1i8), Value::Integer(-1))]
    #[case(value!(1u16), Value::Integer(1))]
    #[case(value!(1i16), Value::Integer(1))]
    #[case(value!(-1i16), Value::Integer(-1))]
    #[case(value!(1u32), Value::Integer(1))]
    #[case(value!(1i32), Value::Integer(1))]
    #[case(value!(-1i32), Value::Integer(-1))]
    #[case(value!(1i64), Value::Integer(1))]
    #[case(value!(-1i64), Value::Integer(-1))]
    #[case(value!(i64::MAX), Value::Integer(i64::MAX))]
    #[case(value!(i64::MIN), Value::Integer(i64::MIN))]
    fn test_int(#[case] input: Value, #[case] output: Value) {
        dbg!(&input, &output);
        assert_eq!(input, output);
    }

    #[rstest]
    #[case(value!(1.0f32), Value::Float(1.))]
    #[case(value!(-1.0f32), Value::Float(-1.))]
    #[case(value!(1.0f64), Value::Float(1.))]
    #[case(value!(-1.0f64), Value::Float(-1.))]
    #[case(value!(f32::NAN), Value::Float(f32::NAN.into()))]
    #[case(value!(f32::MIN), Value::Float(f32::MIN.into()))]
    #[case(value!(f32::MAX), Value::Float(f32::MAX.into()))]
    #[case(value!(f64::NAN), Value::Float(f64::NAN))]
    #[case(value!(f64::MIN), Value::Float(f64::MIN))]
    #[case(value!(f64::MAX), Value::Float(f64::MAX))]
    fn test_float(#[case] input: Value, #[case] output: Value) {
        dbg!(&input, &output);
        if input.as_float().unwrap().is_nan() {
            assert!(output.as_float().unwrap().is_nan())
        } else {
            assert_eq!(input, output);
        }
    }

    #[rstest]
    #[case(value!(bytes()), Value::Bytes(vec![]))]
    #[case(value!(bytes(1)), Value::Bytes(vec![1]))]
    #[case(value!(bytes(1,)), Value::Bytes(vec![1]))]
    fn test_bytes(#[case] input: Value, #[case] output: Value) {
        dbg!(&input, &output);
        assert_eq!(input, output);
    }

    #[rstest]
    #[case(value!(""), Value::String("".into()))]
    #[case(value!("foo"), Value::String("foo".into()))]
    #[case(value!("foo bar"), Value::String("foo bar".into()))]
    #[case(value!(String::from("")), Value::String("".into()))]
    #[case(value!(String::from("foo")), Value::String("foo".into()))]
    #[case(value!(String::from("foo bar")), Value::String("foo bar".into()))]
    fn test_string(#[case] input: Value, #[case] output: Value) {
        dbg!(&input, &output);
        assert_eq!(input, output);
    }

    #[rstest]
    #[case(value!([]), Value::List(vec![]))]
    #[case(value!([,]), Value::List(vec![]))]
    #[case(value!([null]), Value::List(vec![Value::Null]))]
    #[case(value!([null,]), Value::List(vec![Value::Null]))]
    #[case(value!([true]), Value::List(vec![Value::Boolean(true)]))]
    #[case(value!([true,]), Value::List(vec![Value::Boolean(true)]))]
    #[case(value!([false]), Value::List(vec![Value::Boolean(false)]))]
    #[case(value!([false,]), Value::List(vec![Value::Boolean(false)]))]
    #[case(value!([1]), Value::List(vec![Value::Integer(1)]))]
    #[case(value!([1,]), Value::List(vec![Value::Integer(1)]))]
    #[case(value!([1.]), Value::List(vec![Value::Float(1.)]))]
    #[case(value!([1.,]), Value::List(vec![Value::Float(1.)]))]
    #[case(value!([bytes()]), Value::List(vec![Value::Bytes(vec![])]))]
    #[case(value!([bytes(1, 2, 3)]), Value::List(vec![Value::Bytes(vec![1, 2, 3])]))]
    #[case(value!([bytes(1, 2, 3,)]), Value::List(vec![Value::Bytes(vec![1, 2, 3])]))]
    #[case(
        value!([1, [2], 3]),
        Value::List(vec![
            Value::Integer(1),
            Value::List(vec![Value::Integer(2)]),
            Value::Integer(3),
        ])
    )]
    #[case(
        value!([null, 1, 2., {"foo": "bar"}, bytes(1)]),
        Value::List(vec![
            Value::Null,
            Value::Integer(1),
            Value::Float(2.0),
            Value::Map(map!("foo".into() => Value::String("bar".into()))),
            Value::Bytes(vec![1]),
        ])
    )]
    fn test_list(#[case] input: Value, #[case] output: Value) {
        dbg!(&input, &output);
        assert_eq!(input, output);
    }

    #[rstest]
    #[case(value!({}), Value::Map(map!()))]
    #[case(value!({,}), Value::Map(map!()))]
    #[case(value!({"a": null}), Value::Map(map!("a".into() => Value::Null)))]
    #[case(value!({"a": null,}), Value::Map(map!("a".into() => Value::Null)))]
    #[case(value!({"a": true}), Value::Map(map!("a".into() => Value::Boolean(true))))]
    #[case(value!({"a": false}), Value::Map(map!("a".into() => Value::Boolean(false))))]
    #[case(value!({"a": 1}), Value::Map(map!("a".into() => Value::Integer(1))))]
    #[case(value!({"a": 1,}), Value::Map(map!("a".into() => Value::Integer(1))))]
    #[case(
        value!({"a": 1, "b": 1.}), 
        Value::Map(map!(
            "a".into() => Value::Integer(1),
            "b".into() => Value::Float(1.),
        ))
    )]
    #[case(
        value!({"a": 1, "b": 1., "c": [1, "foo", null], "d": {"bar": false}}), 
        Value::Map(map!(
            "a".into() => Value::Integer(1),
            "b".into() => Value::Float(1.),
            "c".into() => Value::List(vec![
                Value::Integer(1), Value::String("foo".into()), Value::Null,
            ]),
            "d".into() => Value::Map(map!("bar".into() => Value::Boolean(false))),
        ))
    )]
    fn test_map(#[case] input: Value, #[case] output: Value) {
        dbg!(&input, &output);
        assert_eq!(input, output);
    }

    #[rstest]
    #[case(value!(
        Cartesian2D::new(1., 2.)),
        Value::Cartesian2D(Cartesian2D::new(1., 2.))
    )]
    #[case(value!(
        Cartesian3D::new(1., 2., 3.)),
        Value::Cartesian3D(Cartesian3D::new(1., 2., 3.))
    )]
    #[case(value!(
        WGS84_2D::new(1., 2.)),
        Value::WGS84_2D(WGS84_2D::new(1., 2.))
    )]
    #[case(value!(
        WGS84_3D::new(1., 2., 3.)),
        Value::WGS84_3D(WGS84_3D::new(1., 2., 3.))
    )]
    fn test_structs(#[case] input: Value, #[case] output: Value) {
        dbg!(&input, &output);
        assert_eq!(input, output);
    }
}
