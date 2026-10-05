/// Signal of operations happening to a recording.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(i32)]
pub enum RecordingSignal {
    /// Recording has started for a stream.
    START = 0_i32,
    /// Recording has stopped for a stream.
    STOP = 1_i32,
    /// Recording has started extending.
    EXTEND = 2_i32,
    /// Recording descriptor replicated from source archive.
    REPLICATE = 3_i32,
    /// Recording merged with live stream after replay.
    MERGE = 4_i32,
    /// Recording synchronised with source archive.
    SYNC = 5_i32,
    /// Recording has deleted segments.
    DELETE = 6_i32,
    /// Recording replication has ended.
    REPLICATE_END = 7_i32,
    #[default]
    NullVal = -2147483648_i32,
}
impl From<i32> for RecordingSignal {
    #[inline]
    fn from(v: i32) -> Self {
        match v {
            0_i32 => Self::START,
            1_i32 => Self::STOP,
            2_i32 => Self::EXTEND,
            3_i32 => Self::REPLICATE,
            4_i32 => Self::MERGE,
            5_i32 => Self::SYNC,
            6_i32 => Self::DELETE,
            7_i32 => Self::REPLICATE_END,
            _ => Self::NullVal,
        }
    }
}
impl From<RecordingSignal> for i32 {
    #[inline]
    fn from(v: RecordingSignal) -> Self {
        match v {
            RecordingSignal::START => 0_i32,
            RecordingSignal::STOP => 1_i32,
            RecordingSignal::EXTEND => 2_i32,
            RecordingSignal::REPLICATE => 3_i32,
            RecordingSignal::MERGE => 4_i32,
            RecordingSignal::SYNC => 5_i32,
            RecordingSignal::DELETE => 6_i32,
            RecordingSignal::REPLICATE_END => 7_i32,
            RecordingSignal::NullVal => -2147483648_i32,
        }
    }
}
impl core::str::FromStr for RecordingSignal {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "START" => Ok(Self::START),
            "STOP" => Ok(Self::STOP),
            "EXTEND" => Ok(Self::EXTEND),
            "REPLICATE" => Ok(Self::REPLICATE),
            "MERGE" => Ok(Self::MERGE),
            "SYNC" => Ok(Self::SYNC),
            "DELETE" => Ok(Self::DELETE),
            "REPLICATE_END" => Ok(Self::REPLICATE_END),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for RecordingSignal {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::START => write!(f, "START"),
            Self::STOP => write!(f, "STOP"),
            Self::EXTEND => write!(f, "EXTEND"),
            Self::REPLICATE => write!(f, "REPLICATE"),
            Self::MERGE => write!(f, "MERGE"),
            Self::SYNC => write!(f, "SYNC"),
            Self::DELETE => write!(f, "DELETE"),
            Self::REPLICATE_END => write!(f, "REPLICATE_END"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
