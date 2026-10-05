/// State of a recording in the Catalog.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(i32)]
pub enum RecordingState {
    /// Recording is invalid.
    INVALID = 0_i32,
    /// Recording is valid.
    VALID = 1_i32,
    /// Recording was deleted.
    DELETED = 2_i32,
    #[default]
    NullVal = -2147483648_i32,
}
impl From<i32> for RecordingState {
    #[inline]
    fn from(v: i32) -> Self {
        match v {
            0_i32 => Self::INVALID,
            1_i32 => Self::VALID,
            2_i32 => Self::DELETED,
            _ => Self::NullVal,
        }
    }
}
impl From<RecordingState> for i32 {
    #[inline]
    fn from(v: RecordingState) -> Self {
        match v {
            RecordingState::INVALID => 0_i32,
            RecordingState::VALID => 1_i32,
            RecordingState::DELETED => 2_i32,
            RecordingState::NullVal => -2147483648_i32,
        }
    }
}
impl core::str::FromStr for RecordingState {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "INVALID" => Ok(Self::INVALID),
            "VALID" => Ok(Self::VALID),
            "DELETED" => Ok(Self::DELETED),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for RecordingState {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::INVALID => write!(f, "INVALID"),
            Self::VALID => write!(f, "VALID"),
            Self::DELETED => write!(f, "DELETED"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
