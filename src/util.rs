use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

pub trait MultiFind<Item> {
    type Output;
    fn init() -> (Self::Output, usize);
    fn check(&mut self, item: &Item, found: &mut Self::Output, remaining: &mut usize);
}

macro_rules! impl_multi_find {
    ($( $idx:tt : $F:ident ),+) => {
        impl<Item, $( $F ),+> MultiFind<Item> for ($( $F, )+)
        where
            $( $F: FnMut(&Item) -> bool, )+
        {
            type Output = ($( impl_multi_find!(@bool $F), )+);
            fn init() -> (Self::Output, usize) {
                (($( impl_multi_find!(@false $F), )+), [$( impl_multi_find!(@ignore $F) ),+].len())
            }
            fn check(&mut self, item: &Item, found: &mut Self::Output, remaining: &mut usize) {
                $(
                    if !found.$idx && (self.$idx)(item) {
                        found.$idx = true;
                        *remaining -= 1;
                    }
                )+
            }
        }
    };
    (@bool $F:ident) => { bool };
    (@false $F:ident) => { false };
    (@ignore $F:ident) => { () };
}

impl_multi_find!(0: F0, 1: F1);
impl_multi_find!(0: F0, 1: F1, 2: F2);
impl_multi_find!(0: F0, 1: F1, 2: F2, 3: F3);

pub trait IteratorExt: Iterator + Sized {
    fn find_conditions<M>(mut self, mut conditions: M) -> M::Output
    where
        M: MultiFind<Self::Item>,
    {
        let (mut found, mut remaining) = M::init();
        for item in self.by_ref() {
            conditions.check(&item, &mut found, &mut remaining);
            if remaining == 0 {
                break;
            }
        }
        found
    }
}

impl<I: Iterator> IteratorExt for I {}

pub struct AtomicInstant {
    anchor: Instant,
    offset_ns: AtomicU64,
}

impl AtomicInstant {
    pub fn new(now: Instant) -> Self {
        Self {
            anchor: now,
            offset_ns: AtomicU64::new(0),
        }
    }

    pub fn store(&self, instant: Instant, order: Ordering) {
        if instant >= self.anchor {
            let duration = instant.duration_since(self.anchor);
            self.offset_ns.store(duration.as_nanos() as u64, order);
        } else {
            self.offset_ns.store(0, order);
        }
    }

    pub fn load(&self, order: Ordering) -> Instant {
        let ns = self.offset_ns.load(order);
        self.anchor + Duration::from_nanos(ns)
    }
}

pub use pastey::paste;
pub use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

/// 创建 ID 结构体。
/// - `gen_id!(Task);` -> `pub TaskId`, `priv TaskIdCounter`
/// - `gen_id!(Task, counter: pub);` -> `pub TaskId`, `pub TaskIdCounter`
/// - `gen_id!(pub(crate) Task);` -> `pub(crate) TaskId`, `priv TaskIdCounter`
/// - `gen_id!(pub(crate) Task, counter: pub(super));` -> `pub(crate) TaskId`, `pub(super) TaskIdCounter`
#[macro_export]
macro_rules! use_id {
    (@impl ($($id_vis:tt)*) ($($counter_vis:tt)*) $name:ident) => {
        $crate::util::paste! {
            #[derive(
                Debug, Clone, Copy, Hash, PartialEq, Eq,
                $crate::util::SerdeSerialize,
                $crate::util::SerdeDeserialize
            )]
            $($id_vis)* struct [<$name Id>](pub u64);
            impl From<u64> for [<$name Id>] { fn from(value: u64) -> Self { Self(value) } }
            impl Into<u64> for [<$name Id>] { fn into(self) -> u64 { self.0 } }
            $($counter_vis)* struct [<$name IdCounter>](std::sync::atomic::AtomicU64);
            impl Default for [<$name IdCounter>] { fn default() -> Self { Self(std::sync::atomic::AtomicU64::new(1)) } }
            impl [<$name IdCounter>] {
                pub fn next(&self) -> [<$name Id>] { [<$name Id>](self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)) }
            }
        }
    };
    ($name:ident, counter: $counter_vis:vis) => { $crate::use_id!(@impl (pub) ($counter_vis) $name); };
    ($id_vis:vis $name:ident, counter: $counter_vis:vis) => { $crate::use_id!(@impl ($id_vis) ($counter_vis) $name); };
    ($name:ident) => { $crate::use_id!(@impl (pub) () $name); };
    ($id_vis:vis $name:ident) => { crate::use_id!(@impl ($id_vis) () $name); };
}
