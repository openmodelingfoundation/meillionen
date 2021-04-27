#[macro_export]
macro_rules! impl_try_from_u8 {
 ($t:ty) => {
    impl std::convert::TryFrom<&[u8]> for $t {
        type Error = serde_json::Error;

        fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
            serde_json::from_reader(value)
        }
    }
 }
}

#[macro_export]
macro_rules! impl_try_from_validator {
    ($t:ty) => {
        impl std::convert::TryFrom<&$t> for Vec<u8> {
            type Error = serde_json::Error;

            fn try_from(value: &$t) -> Result<Self, Self::Error> {
                serde_json::to_vec(value)
            }
        }
    }
}