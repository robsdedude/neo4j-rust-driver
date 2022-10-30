use std::any::type_name;

macro_rules! map {
    () => {std::collections::HashMap::new()};
    ( $($key:expr => $value:expr),* ) => {
        {
            let mut m = std::collections::HashMap::new();
            $(
                m.insert($key, $value);
            )*
            m
        }
    };
}

pub(crate) use map;

/// until (type_name_of_val)[https://doc.rust-lang.org/std/any/fn.type_name_of_val.html]
/// becomes stable, we'll do it on our own =)
pub fn get_type_name<T>(_: T) -> &'static str {
    type_name::<T>()
}
