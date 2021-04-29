#[macro_export]
macro_rules! impl_try_from_u8 {
 ($T:ty) => {
    impl std::convert::TryFrom<&[u8]> for $T {
        type Error = serde_json::Error;

        fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
            serde_json::from_reader(value)
        }
    }
 }
}

#[macro_export]
macro_rules! impl_try_from_validator {
    ($T:ty) => {
        impl std::convert::TryFrom<&$T> for Vec<u8> {
            type Error = serde_json::Error;

            fn try_from(value: &$T) -> Result<Self, Self::Error> {
                serde_json::to_vec(value)
            }
        }
    }
}