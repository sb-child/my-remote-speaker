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
