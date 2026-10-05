/// Control protocol response code.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(i32)]
pub enum ControlResponseCode {
    /// Operation successful.
    OK = 0_i32,
    /// Error occurred during operation.
    ERROR = 1_i32,
    /// Recording id was unknown.
    RECORDING_UNKNOWN = 2_i32,
    /// Subscription id was unknown.
    SUBSCRIPTION_UNKNOWN = 3_i32,
    #[default]
    NullVal = -2147483648_i32,
}
impl From<i32> for ControlResponseCode {
    #[inline]
    fn from(v: i32) -> Self {
        match v {
            0_i32 => Self::OK,
            1_i32 => Self::ERROR,
            2_i32 => Self::RECORDING_UNKNOWN,
            3_i32 => Self::SUBSCRIPTION_UNKNOWN,
            _ => Self::NullVal,
        }
    }
}
impl From<ControlResponseCode> for i32 {
    #[inline]
    fn from(v: ControlResponseCode) -> Self {
        match v {
            ControlResponseCode::OK => 0_i32,
            ControlResponseCode::ERROR => 1_i32,
            ControlResponseCode::RECORDING_UNKNOWN => 2_i32,
            ControlResponseCode::SUBSCRIPTION_UNKNOWN => 3_i32,
            ControlResponseCode::NullVal => -2147483648_i32,
        }
    }
}
impl core::str::FromStr for ControlResponseCode {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "OK" => Ok(Self::OK),
            "ERROR" => Ok(Self::ERROR),
            "RECORDING_UNKNOWN" => Ok(Self::RECORDING_UNKNOWN),
            "SUBSCRIPTION_UNKNOWN" => Ok(Self::SUBSCRIPTION_UNKNOWN),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for ControlResponseCode {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OK => write!(f, "OK"),
            Self::ERROR => write!(f, "ERROR"),
            Self::RECORDING_UNKNOWN => write!(f, "RECORDING_UNKNOWN"),
            Self::SUBSCRIPTION_UNKNOWN => write!(f, "SUBSCRIPTION_UNKNOWN"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
