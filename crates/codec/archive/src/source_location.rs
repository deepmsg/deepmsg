/// Source location for recorded stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(i32)]
pub enum SourceLocation {
    /// Archive is local to driver.
    LOCAL = 0_i32,
    /// Archive is remote to driver.
    REMOTE = 1_i32,
    #[default]
    NullVal = -2147483648_i32,
}
impl From<i32> for SourceLocation {
    #[inline]
    fn from(v: i32) -> Self {
        match v {
            0_i32 => Self::LOCAL,
            1_i32 => Self::REMOTE,
            _ => Self::NullVal,
        }
    }
}
impl From<SourceLocation> for i32 {
    #[inline]
    fn from(v: SourceLocation) -> Self {
        match v {
            SourceLocation::LOCAL => 0_i32,
            SourceLocation::REMOTE => 1_i32,
            SourceLocation::NullVal => -2147483648_i32,
        }
    }
}
impl core::str::FromStr for SourceLocation {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "LOCAL" => Ok(Self::LOCAL),
            "REMOTE" => Ok(Self::REMOTE),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for SourceLocation {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::LOCAL => write!(f, "LOCAL"),
            Self::REMOTE => write!(f, "REMOTE"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
