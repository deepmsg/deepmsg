//! The incoming half of the UDP channel transport's **interceptors**
//! (`media/aeron_udp_channel_transport_bindings.c:71-93`).
//!
//! An interceptor sits between the socket and the endpoint: a datagram arrives,
//! every interceptor in the chain is offered it, and **the first one that
//! decides to drop it ends the chain** — the reference's `incoming_func` calls
//! its delegate only when it is not dropping
//! (`media/aeron_udp_channel_transport_loss.c:139-166`).
//!
//! Three are compiled into the reference driver and reachable by name:
//!
//! | Name | What it drops | Where |
//! |---|---|---|
//! | `loss` | a fraction of the frames whose type is in a mask | `media/aeron_udp_channel_transport_loss.c` |
//! | `fixed-loss` | one fixed byte range of one term, once per stream and session | `media/aeron_udp_channel_transport_fixed_loss.c` |
//! | `multi-gap-loss` | several gaps, at offsets chosen by a radix | `media/aeron_udp_channel_transport_multi_gap_loss.c` |
//!
//! They are **not** a plugin mechanism: the reference resolves a name against
//! that compiled-in table and has no `dlopen` fallback
//! (`aeron_udp_channel_interceptor_bindings_load`, `:123-184`), so serving them
//! is not a departure from ADR-0002 — nothing is loaded. A name that is *not*
//! in the table stops the reference's driver (`aeron_driver_context.c:1283-1290`
//! returns `NULL` into a `goto error`), and this build refuses it the same way.
//!
//! The settings are the reference's, environment-only
//! (`aeron-driver/src/main/c/aeronmd.h:745-746`):
//!
//! ```text
//! AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS=loss,fixed-loss
//! AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS=rate=0.1|seed=3405691582|recv-msg-mask=0x9
//! ```
//!
//! # Two deliberate divergences
//!
//! `loss` draws from a **process-wide** generator in the reference
//! (`static unsigned short data_loss_xsubi[3]`,
//! `media/aeron_udp_channel_transport_loss.c:47`), so which frames it drops
//! depends on what every other transport received, in order. Here the sequence
//! is per channel — one generator seeded the same way per interceptor — because
//! the alternative is a global the receive threads would have to synchronise
//! on. The **rate** and the **mask** are the same; the frames are a different
//! draw from the same distribution. `docs/compat.md` carries the row.
//!
//! The second is the shape of the chain and not its behaviour: the reference
//! builds a linked list of `(interceptor_state, function pointers)` per
//! transport, and this is an enum per entry, so a channel with no interceptors
//! configured has an empty slice and the receive path does no work at all.

use crate::protocol::{DataFrame, FrameHeader};

/// `AERON_MAX_INTERCEPTOR_NAMES` (`media/aeron_udp_channel_transport_bindings.c:121`).
pub const MAX_INTERCEPTOR_NAMES: usize = 10;

/// `AERON_MAX_INTERCEPTORS_LEN` (`:120`).
pub const MAX_INTERCEPTORS_LEN: usize = 4094;

/// `AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS_ENV_VAR` (`aeronmd.h:746`).
pub const INCOMING_INTERCEPTORS_ENV: &str = "AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS";

/// `AERON_UDP_CHANNEL_OUTGOING_INTERCEPTORS_ENV_VAR` (`aeronmd.h:745`).
pub const OUTGOING_INTERCEPTORS_ENV: &str = "AERON_UDP_CHANNEL_OUTGOING_INTERCEPTORS";

/// The args variable each interceptor reads its own parameters from
/// (`media/aeron_udp_channel_transport_loss.c:42` and its two siblings).
const LOSS_ARGS_ENV: &str = "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS";
const FIXED_LOSS_ARGS_ENV: &str = "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_FIXED_LOSS_ARGS";
const MULTI_GAP_LOSS_ARGS_ENV: &str = "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MULTI_GAP_LOSS_ARGS";

/// `AERON_URI_DATA_LOSS_TYPE_MASK`'s selector is the frame type as a bit, so a
/// type outside `0..32` names no bit at all
/// (`media/aeron_udp_channel_transport_loss.c:132-136` reads
/// `1U << (unsigned int)frame_header->type`).
fn message_type_bit(frame_type: i16) -> Option<u32> {
    u32::try_from(frame_type)
        .ok()
        .filter(|kind| *kind < u32::BITS)
        .map(|kind| 1u32 << kind)
}

/// `erand48`, the generator the reference's `loss` interceptor draws from
/// (`aeron-client/src/main/c/util/aeron_erand48.h`; glibc's `__drand48_iterate`).
///
/// The 48 bits are held in three shorts and the **first is the least
/// significant** — the convention is glibc's, not the intuitive one, and it is
/// what makes Aeron's own seeding (`xsubi[0]` from the seed's high half,
/// `media/aeron_udp_channel_transport_loss.c:85-87`) produce the sequence the
/// reference produces. Getting the halves the other way round still yields
/// numbers in `[0, 1)`, which is the kind of bug only a value taken from the
/// real generator catches.
fn erand48(xsubi: &mut [u16; 3]) -> f64 {
    let x = (u64::from(xsubi[2]) << 32) | (u64::from(xsubi[1]) << 16) | u64::from(xsubi[0]);

    let next = (0x5DEECE66D_u64.wrapping_mul(x).wrapping_add(0xB)) & ((1u64 << 48) - 1);

    xsubi[0] = u16::try_from(next & 0xFFFF).expect("sixteen bits");
    xsubi[1] = u16::try_from((next >> 16) & 0xFFFF).expect("sixteen bits");
    xsubi[2] = u16::try_from((next >> 32) & 0xFFFF).expect("sixteen bits");

    // `x * 2^-48` exactly: every 48-bit integer is an `f64` exactly.
    #[allow(clippy::cast_precision_loss)]
    let value = next as f64 / 281_474_976_710_656.0;

    value
}

/// What a name in `AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS` resolved to.
#[derive(Clone, Debug, PartialEq)]
pub enum Interceptor {
    /// `loss` — a fraction of the frames whose type is in a mask.
    Loss(LossParams),
    /// `fixed-loss` — one byte range of one term, dropped once per stream.
    FixedLoss(FixedLossParams),
    /// `multi-gap-loss` — several gaps at radix-chosen offsets.
    MultiGapLoss(MultiGapLossParams),
}

/// `aeron_udp_channel_interceptor_loss_params_t`
/// (`media/aeron_udp_channel_transport_loss.h:22-27`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LossParams {
    /// `rate` — the probability a frame that matches the mask is dropped.
    pub rate: f64,
    /// `seed` — the generator's seed, split into the three halves of a
    /// `erand48` state (`media/aeron_udp_channel_transport_loss.c:85-87`).
    pub seed: u64,
    /// `recv-msg-mask` — one bit per frame type, so `0x9` is DATA and SM.
    pub message_type_mask: u32,
}

/// `aeron_udp_channel_interceptor_fixed_loss_params_t`
/// (`media/aeron_udp_channel_transport_fixed_loss.h:22-28`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FixedLossParams {
    /// `term-id` — the term the range is in.
    pub term_id: i32,
    /// `term-offset` — where the range starts.
    pub term_offset: i32,
    /// `length` — how many bytes of the term are lost.
    pub length: usize,
}

/// `aeron_udp_channel_interceptor_multi_gap_loss_params_t`
/// (`media/aeron_udp_channel_transport_multi_gap_loss.h:22-33`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MultiGapLossParams {
    /// `term-id` — the term the gaps are in.
    pub term_id: i32,
    /// `gap-radix` rounded up to a power of two, and the two derived from it
    /// (`media/aeron_udp_channel_transport_multi_gap_loss.c:269-272`).
    pub gap_radix_bits: u32,
    /// `~(radix - 1)`, which masks an offset down to its gap's base.
    pub gap_radix_mask: u32,
    /// `gap-length` — how many bytes each gap is.
    pub gap_length: usize,
    /// `total-gaps * gap-radix + gap-length`: past this offset no gap is made
    /// (`:272`), which is what keeps the loss inside the term the test named.
    pub last_gap_limit: i32,
}

/// Why an interceptor list could not be turned into interceptors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InterceptorError {
    /// The list is longer than the reference's buffer (`:129-138`). A
    /// `String` rather than a count because the list is what a caller has to
    /// look at.
    ListTooLong,
    /// More names than the reference's array holds (`:147-151`).
    TooManyNames {
        /// How many were named.
        count: usize,
        /// The list, as it was written.
        names: String,
    },
    /// A name that is not in the reference's table (`:178-182`).
    Unknown {
        /// The setting the name came from, which is what a reader has to edit.
        setting: &'static str,
        /// The name.
        name: String,
    },
    /// A parameter an interceptor's own parser refused
    /// (`media/aeron_udp_channel_transport_loss.c:203-211` and its siblings).
    NotAParameter {
        /// Which interceptor's arguments.
        interceptor: &'static str,
        /// The key it could not read.
        key: String,
        /// What the key said.
        value: String,
    },
}

impl std::fmt::Display for InterceptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ListTooLong => write!(
                f,
                "Interceptors list too long, must have: < {MAX_INTERCEPTORS_LEN}"
            ),
            Self::TooManyNames { count, names } => write!(
                f,
                "Too many interceptors defined, limit {MAX_INTERCEPTOR_NAMES}, found {count}: {names}"
            ),
            Self::Unknown { setting, name } => write!(
                f,
                "{setting} names a UDP channel interceptor this driver has no \
                 implementation for: {name}"
            ),
            Self::NotAParameter {
                interceptor,
                key,
                value,
            } => write!(f, "Could not parse {interceptor} {key} from: {value}:"),
        }
    }
}

/// Resolve the names in `AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS` against the
/// reference's table, reading each one's arguments from its own variable.
///
/// # Errors
///
/// [`InterceptorError`] for a list the reference refuses — too long, too many
/// names, a name not in the table — or a parameter its parser refuses.
pub fn resolve_incoming(
    env: &impl Fn(&str) -> Option<String>,
) -> Result<Vec<Interceptor>, InterceptorError> {
    let Some(list) = env(INCOMING_INTERCEPTORS_ENV) else {
        return Ok(Vec::new());
    };

    resolve(INCOMING_INTERCEPTORS_ENV, &list, env)
}

/// The same, for a list a caller already has.
///
/// # Errors
///
/// As [`resolve_incoming`].
pub fn resolve(
    setting: &'static str,
    list: &str,
    env: &impl Fn(&str) -> Option<String>,
) -> Result<Vec<Interceptor>, InterceptorError> {
    if list.len() >= MAX_INTERCEPTORS_LEN {
        return Err(InterceptorError::ListTooLong);
    }

    // `aeron_tokenise` splits on commas and drops nothing: an empty name in the
    // list is a name, and the reference fails to load it (`:178-182`).
    let names: Vec<&str> = if list.is_empty() {
        Vec::new()
    } else {
        list.split(',').collect()
    };

    if names.len() > MAX_INTERCEPTOR_NAMES {
        return Err(InterceptorError::TooManyNames {
            count: names.len(),
            names: list.to_owned(),
        });
    }

    names
        .into_iter()
        .map(|name| resolve_one(setting, name, env))
        .collect()
}

fn resolve_one(
    setting: &'static str,
    name: &str,
    env: &impl Fn(&str) -> Option<String>,
) -> Result<Interceptor, InterceptorError> {
    let args = |variable: &str| env(variable).unwrap_or_default();

    match name {
        "loss" => Ok(Interceptor::Loss(parse_loss(&args(LOSS_ARGS_ENV))?)),
        "fixed-loss" => Ok(Interceptor::FixedLoss(parse_fixed_loss(&args(
            FIXED_LOSS_ARGS_ENV,
        ))?)),
        "multi-gap-loss" => Ok(Interceptor::MultiGapLoss(parse_multi_gap_loss(&args(
            MULTI_GAP_LOSS_ARGS_ENV,
        ))?)),
        other => Err(InterceptorError::Unknown {
            setting,
            name: other.to_owned(),
        }),
    }
}

/// The `key=value|key=value` form every interceptor's arguments take.
///
/// A key the interceptor does not know is **not** an error in the reference:
/// each parses by `strncmp` over the keys it wants and returns zero for
/// anything else (`media/aeron_udp_channel_transport_loss.c:178-215`).
fn parameters<'a>(interceptor: &'static str, args: &'a str) -> Vec<(&'a str, &'a str)> {
    let _ = interceptor;

    args.split('|')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| pair.split_once('='))
        .collect()
}

fn refused(interceptor: &'static str, key: &str, value: &str) -> InterceptorError {
    InterceptorError::NotAParameter {
        interceptor,
        key: key.to_owned(),
        value: value.to_owned(),
    }
}

/// `aeron_udp_channel_interceptor_loss_parse_callback` (`:178-215`).
fn parse_loss(args: &str) -> Result<LossParams, InterceptorError> {
    let mut params = LossParams {
        rate: 0.0,
        seed: 0,
        message_type_mask: 0,
    };

    for (key, value) in parameters("loss", args) {
        match key {
            "rate" => match value.parse::<f64>() {
                Ok(rate) => params.rate = rate,
                Err(_) => return Err(refused("loss", key, value)),
            },
            // `strtoull(value, &endptr, 10)`, and the seed is split into the
            // three halves of the generator's state where it is used.
            "seed" => match value.parse::<u64>() {
                Ok(seed) => params.seed = seed,
                Err(_) => return Err(refused("loss", key, value)),
            },
            // `strtoul(value, &endptr, 16)`, which is where the `0x` of the
            // mask the harness sends is allowed rather than refused.
            "recv-msg-mask" => {
                let digits = value
                    .strip_prefix("0x")
                    .or_else(|| value.strip_prefix("0X"))
                    .unwrap_or(value);

                match u32::from_str_radix(digits, 16) {
                    Ok(mask) => params.message_type_mask = mask,
                    Err(_) => return Err(refused("loss", key, value)),
                }
            }
            _ => {}
        }
    }

    Ok(params)
}

/// `aeron_udp_channel_interceptor_fixed_loss_parse_callback` (`:243-296`).
fn parse_fixed_loss(args: &str) -> Result<FixedLossParams, InterceptorError> {
    // `term_offset` starts at `-1`, which is what makes a `fixed-loss` with no
    // `term-offset` name a range that never starts (`:104-106`).
    let mut params = FixedLossParams {
        term_id: 0,
        term_offset: -1,
        length: 0,
    };

    for (key, value) in parameters("fixed-loss", args) {
        match key {
            "term-id" => match value.parse::<i32>() {
                Ok(term_id) => params.term_id = term_id,
                Err(_) => return Err(refused("fixed-loss", key, value)),
            },
            "term-offset" => match value.parse::<i32>() {
                Ok(term_offset) => params.term_offset = term_offset,
                Err(_) => return Err(refused("fixed-loss", key, value)),
            },
            "length" => match value.parse::<usize>() {
                Ok(length) => params.length = length,
                Err(_) => return Err(refused("fixed-loss", key, value)),
            },
            _ => {}
        }
    }

    Ok(params)
}

/// `aeron_udp_channel_interceptor_multi_gap_loss_parse_callback` (`:278-336`)
/// and the derivation that follows it (`:266-276`).
fn parse_multi_gap_loss(args: &str) -> Result<MultiGapLossParams, InterceptorError> {
    let (mut term_id, mut gap_radix, mut gap_length, mut total_gaps) = (0i32, 0i32, 0usize, 0i32);

    for (key, value) in parameters("multi-gap-loss", args) {
        match key {
            "term-id" => match value.parse::<i32>() {
                Ok(parsed) => term_id = parsed,
                Err(_) => return Err(refused("multi-gap-loss", key, value)),
            },
            "gap-radix" => match value.parse::<i32>() {
                Ok(parsed) => gap_radix = parsed,
                Err(_) => return Err(refused("multi-gap-loss", key, value)),
            },
            "gap-length" => match value.parse::<usize>() {
                Ok(parsed) => gap_length = parsed,
                Err(_) => return Err(refused("multi-gap-loss", key, value)),
            },
            "total-gaps" => match value.parse::<i32>() {
                Ok(parsed) => total_gaps = parsed,
                Err(_) => return Err(refused("multi-gap-loss", key, value)),
            },
            _ => {}
        }
    }

    // `aeron_find_next_power_of_two` (`util/aeron_bitutil.h:165-178`), then the
    // bit count and the mask off it (`:269-271`).
    let power_of_two = gap_radix.wrapping_sub(1);
    let mut rounded = power_of_two;
    let mut shift = 1;
    while shift < i32::BITS {
        rounded |= rounded >> shift;
        shift *= 2;
    }
    let rounded = rounded.wrapping_add(1);

    #[allow(clippy::cast_sign_loss)] // a rounded power of two is positive here
    let gap_radix_bits = rounded.trailing_zeros();
    #[allow(clippy::cast_sign_loss)]
    let gap_radix_mask = !(rounded.wrapping_sub(1)) as u32;

    Ok(MultiGapLossParams {
        term_id,
        gap_radix_bits,
        gap_radix_mask,
        gap_length,
        last_gap_limit: total_gaps
            .wrapping_mul(gap_radix)
            .wrapping_add(i32::try_from(gap_length).unwrap_or(i32::MAX)),
    })
}

/// Whether the reference would refuse an **outgoing** interceptor list, and
/// what to say about it.
///
/// None of the three compiled-in interceptors has an outgoing half
/// (`outgoing_init_func` is `NULL` in all three `_load` functions), so a list
/// here names something the reference cannot resolve — the table lookup fails
/// and its driver does not start (`aeron_driver_context.c:1274-1281`).
///
/// # Errors
///
/// [`InterceptorError::Unknown`] for the first name, which is the one the
/// reference would have failed on.
pub fn refuse_outgoing(env: &impl Fn(&str) -> Option<String>) -> Result<(), InterceptorError> {
    let Some(list) = env(OUTGOING_INTERCEPTORS_ENV) else {
        return Ok(());
    };

    let Some(name) = list.split(',').find(|name| !name.is_empty()) else {
        return Ok(());
    };

    Err(InterceptorError::Unknown {
        setting: OUTGOING_INTERCEPTORS_ENV,
        name: name.to_owned(),
    })
}

/// The interceptors one transport reads through: the chain, with the state each
/// entry needs.
///
/// The reference allocates this per transport
/// (`aeron_udp_channel_data_paths_init`, `:186-260`), and so does this: the two
/// loss interceptors keep a per-stream offset map, and a map shared between two
/// channels would make each channel's loss depend on the other's traffic.
#[derive(Debug, Default)]
pub struct Incoming {
    entries: Vec<Entry>,
}

#[derive(Debug)]
enum Entry {
    Loss(LossState),
    FixedLoss {
        params: FixedLossParams,
        tracking: Tracking,
    },
    MultiGapLoss {
        params: MultiGapLossParams,
        tracking: Tracking,
    },
}

/// The `erand48` state, split the way the reference's seed is
/// (`media/aeron_udp_channel_transport_loss.c:85-87`).
#[derive(Debug)]
struct LossState {
    params: LossParams,
    xsubi: [u16; 3],
}

/// `stream_and_session_id_to_offset_map` — what each `(stream, session)` has
/// been dropped through so far.
///
/// A short `Vec` rather than a map: a driver serves a handful of streams on a
/// channel, and the entry is only pushed when a new pair appears, so the lookup
/// on the hot path touches no allocator.
#[derive(Debug, Default)]
struct Tracking {
    offsets: Vec<((i32, i32), i64)>,
}

impl Tracking {
    fn get(&self, stream_id: i32, session_id: i32) -> Option<i64> {
        self.offsets
            .iter()
            .find(|(key, _)| *key == (stream_id, session_id))
            .map(|(_, offset)| *offset)
    }

    fn set(&mut self, stream_id: i32, session_id: i32, offset: i64) {
        if let Some((_, held)) = self
            .offsets
            .iter_mut()
            .find(|(key, _)| *key == (stream_id, session_id))
        {
            *held = offset;
            return;
        }

        self.offsets.push(((stream_id, session_id), offset));
    }
}

impl Incoming {
    /// The chain for one transport, from the list a driver resolved.
    #[must_use]
    pub fn new(interceptors: &[Interceptor]) -> Self {
        let entries = interceptors
            .iter()
            .map(|interceptor| match interceptor {
                Interceptor::Loss(params) => Entry::Loss(LossState {
                    params: *params,
                    xsubi: [
                        u16::try_from((params.seed >> 32) & 0xFFFF).expect("sixteen bits"),
                        u16::try_from((params.seed >> 16) & 0xFFFF).expect("sixteen bits"),
                        u16::try_from(params.seed & 0xFFFF).expect("sixteen bits"),
                    ],
                }),
                Interceptor::FixedLoss(params) => Entry::FixedLoss {
                    params: *params,
                    tracking: Tracking::default(),
                },
                Interceptor::MultiGapLoss(params) => Entry::MultiGapLoss {
                    params: *params,
                    tracking: Tracking::default(),
                },
            })
            .collect();

        Self { entries }
    }

    /// Whether this datagram is one the chain drops.
    ///
    /// The first entry that says yes ends it, which is the reference's chain
    /// and not a shortcut: an interceptor that drops does not call its
    /// delegate, so the ones behind it never see the frame.
    pub fn drops(&mut self, datagram: &[u8]) -> bool {
        let Some(header) = FrameHeader::read(datagram) else {
            return false;
        };

        for entry in &mut self.entries {
            let drop = match entry {
                Entry::Loss(state) => should_drop_loss(header.frame_type, state),
                Entry::FixedLoss { params, tracking } => {
                    should_drop_fixed_loss(datagram, params, tracking)
                }
                Entry::MultiGapLoss { params, tracking } => {
                    should_drop_multi_gap_loss(datagram, params, tracking)
                }
            };

            if drop {
                return true;
            }
        }

        false
    }
}

/// `aeron_udp_channel_interceptor_loss_should_drop_frame` (`:128-137`).
///
/// The order matters and is the reference's: the generator is only advanced for
/// a frame that got past the rate and the mask, so a chain that sees only
/// control frames draws nothing.
fn should_drop_loss(frame_type: i16, state: &mut LossState) -> bool {
    let Some(bit) = message_type_bit(frame_type) else {
        return false;
    };

    if 0.0 >= state.params.rate || (bit & state.params.message_type_mask) == 0 {
        return false;
    }

    erand48(&mut state.xsubi) <= state.params.rate
}

/// `aeron_udp_channel_interceptor_fixed_loss_should_drop_frame` (`:159-205`).
fn should_drop_fixed_loss(
    datagram: &[u8],
    params: &FixedLossParams,
    tracking: &mut Tracking,
) -> bool {
    let Some(data) = DataFrame::read(datagram) else {
        return false;
    };

    if params.term_id != data.term_id {
        return false;
    }

    let tracked = match tracking.get(data.stream_id, data.session_id) {
        Some(offset) => offset,
        None => {
            tracking.set(data.stream_id, data.session_id, i64::from(data.term_offset));
            i64::from(data.term_offset)
        }
    };

    let frame_limit =
        i64::from(data.term_offset) + i64::try_from(datagram.len()).unwrap_or(i64::MAX);
    let range_limit =
        i64::from(params.term_offset) + i64::try_from(params.length).unwrap_or(i64::MAX);

    if tracked < range_limit && i64::from(data.term_offset) <= tracked && tracked < frame_limit {
        tracking.set(data.stream_id, data.session_id, frame_limit);
        return true;
    }

    false
}

/// `aeron_udp_channel_interceptor_multi_gap_loss_should_drop_frame`
/// (`:228-290`).
fn should_drop_multi_gap_loss(
    datagram: &[u8],
    params: &MultiGapLossParams,
    tracking: &mut Tracking,
) -> bool {
    let Some(data) = DataFrame::read(datagram) else {
        return false;
    };

    if params.term_id != data.term_id || data.term_offset > params.last_gap_limit {
        return false;
    }

    let tracked = match tracking.get(data.stream_id, data.session_id) {
        Some(offset) => offset,
        None => {
            tracking.set(data.stream_id, data.session_id, i64::from(data.term_offset));
            i64::from(data.term_offset)
        }
    };

    if tracked > i64::from(data.term_offset) {
        return false;
    }

    let frame_limit =
        i64::from(data.term_offset) + i64::try_from(datagram.len()).unwrap_or(i64::MAX);
    let gap_length = i64::try_from(params.gap_length).unwrap_or(i64::MAX);
    let term_offset = i64::from(data.term_offset);

    // The three ways an offset falls in a gap. `goto drop_frame` in the
    // reference, and each falls through to the next when it does not.
    let on_a_gap_boundary =
        data.term_offset != 0 && data.term_offset.trailing_zeros() >= params.gap_radix_bits;

    let previous_gap_offset = i64::from(data.term_offset) & i64::from(params.gap_radix_mask);
    let in_the_previous_gap =
        previous_gap_offset > 0 && term_offset < previous_gap_offset + gap_length;

    let next_gap_offset =
        (data.term_offset >> params.gap_radix_bits).wrapping_add(1) << params.gap_radix_bits;
    let next_gap_limit = i64::from(next_gap_offset) + gap_length;
    let in_the_next_gap = frame_limit > i64::from(next_gap_offset) && term_offset < next_gap_limit;

    if !(on_a_gap_boundary || in_the_previous_gap || in_the_next_gap) {
        return false;
    }

    tracking.set(data.stream_id, data.session_id, frame_limit);

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::protocol::frame_type;
    use deepmsg_core::logbuffer::frame::{
        DATA_HEADER_LENGTH, FLAGS_OFFSET, FRAME_LENGTH_OFFSET, SESSION_ID_FIELD_OFFSET,
        STREAM_ID_FIELD_OFFSET, TERM_ID_FIELD_OFFSET, TERM_OFFSET_FIELD_OFFSET, TYPE_OFFSET,
        VERSION_OFFSET,
    };

    /// A frame of `payload_length` bytes at `term_offset` of `term_id`, on the
    /// stream and session named.
    ///
    /// Written field by field off the layout constants the production code
    /// reads (`deepmsg_core::logbuffer::frame`), because an interceptor sees a
    /// **datagram** and there is no term buffer behind it to write through.
    fn frame(
        type_id: i16,
        term_id: i32,
        term_offset: i32,
        stream_id: i32,
        session_id: i32,
        payload_length: usize,
    ) -> Vec<u8> {
        let total = DATA_HEADER_LENGTH + payload_length;
        let mut buffer = vec![0u8; total];
        let length = i32::try_from(total).expect("a small frame");

        buffer[FRAME_LENGTH_OFFSET..FRAME_LENGTH_OFFSET + 4].copy_from_slice(&length.to_le_bytes());
        buffer[VERSION_OFFSET] = 0;
        buffer[FLAGS_OFFSET] = 0;
        buffer[TYPE_OFFSET..TYPE_OFFSET + 2].copy_from_slice(&type_id.to_le_bytes());
        buffer[TERM_OFFSET_FIELD_OFFSET..TERM_OFFSET_FIELD_OFFSET + 4]
            .copy_from_slice(&term_offset.to_le_bytes());
        buffer[SESSION_ID_FIELD_OFFSET..SESSION_ID_FIELD_OFFSET + 4]
            .copy_from_slice(&session_id.to_le_bytes());
        buffer[STREAM_ID_FIELD_OFFSET..STREAM_ID_FIELD_OFFSET + 4]
            .copy_from_slice(&stream_id.to_le_bytes());
        buffer[TERM_ID_FIELD_OFFSET..TERM_ID_FIELD_OFFSET + 4]
            .copy_from_slice(&term_id.to_le_bytes());

        buffer
    }

    fn data_frame(
        term_id: i32,
        term_offset: i32,
        stream_id: i32,
        session_id: i32,
        payload_length: usize,
    ) -> Vec<u8> {
        frame(
            frame_type::DATA,
            term_id,
            term_offset,
            stream_id,
            session_id,
            payload_length,
        )
    }

    fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();

        move |name: &str| {
            pairs
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        }
    }

    #[test]
    fn a_driver_with_no_interceptors_names_none() {
        assert_eq!(Ok(Vec::new()), resolve_incoming(&env_from(&[])));
    }

    #[test]
    fn the_three_compiled_in_names_resolve_and_a_fourth_does_not() {
        let resolved = resolve(
            INCOMING_INTERCEPTORS_ENV,
            "loss,fixed-loss,multi-gap-loss",
            &env_from(&[
                (LOSS_ARGS_ENV, "rate=0.1|seed=3405691582|recv-msg-mask=0x9"),
                (FIXED_LOSS_ARGS_ENV, "term-id=5|term-offset=102|length=64"),
                (
                    MULTI_GAP_LOSS_ARGS_ENV,
                    "term-id=5|gap-radix=19|gap-length=8|total-gaps=3",
                ),
            ]),
        )
        .expect("the reference's own three");

        assert_eq!(
            vec![
                Interceptor::Loss(LossParams {
                    rate: 0.1,
                    seed: 3_405_691_582,
                    message_type_mask: 0x9,
                }),
                Interceptor::FixedLoss(FixedLossParams {
                    term_id: 5,
                    term_offset: 102,
                    length: 64,
                }),
                Interceptor::MultiGapLoss(MultiGapLossParams {
                    // 19 rounds up to 32, so five bits and the mask above them.
                    gap_radix_bits: 5,
                    gap_radix_mask: !31u32,
                    gap_length: 8,
                    // 3 * 19 + 8, and the radix is the *unrounded* one.
                    last_gap_limit: 65,
                    term_id: 5,
                }),
            ],
            resolved
        );

        // A name outside the table is what stops the reference's driver
        // (`aeron_driver_context.c:1283-1290`).
        assert_eq!(
            Err(InterceptorError::Unknown {
                setting: INCOMING_INTERCEPTORS_ENV,
                name: "aeron_ats_interceptor".to_owned()
            }),
            resolve(
                INCOMING_INTERCEPTORS_ENV,
                "aeron_ats_interceptor",
                &env_from(&[])
            )
        );
    }

    #[test]
    fn a_list_the_reference_refuses_is_refused_here_too() {
        // Eleven names is one past `AERON_MAX_INTERCEPTOR_NAMES` (`:121`).
        let eleven = ["loss"; MAX_INTERCEPTOR_NAMES + 1].join(",");
        assert_eq!(
            Err(InterceptorError::TooManyNames {
                count: MAX_INTERCEPTOR_NAMES + 1,
                names: eleven.clone(),
            }),
            resolve(INCOMING_INTERCEPTORS_ENV, &eleven, &env_from(&[]))
        );

        // And the buffer, which is a different limit (`:129-138`).
        let long = "l".repeat(MAX_INTERCEPTORS_LEN);
        assert_eq!(
            Err(InterceptorError::ListTooLong),
            resolve(INCOMING_INTERCEPTORS_ENV, &long, &env_from(&[]))
        );
    }

    #[test]
    fn a_parameter_an_interceptor_cannot_read_is_refused_with_its_own_words() {
        assert_eq!(
            Err(InterceptorError::NotAParameter {
                interceptor: "loss",
                key: "rate".to_owned(),
                value: "fast".to_owned(),
            }),
            resolve(
                INCOMING_INTERCEPTORS_ENV,
                "loss",
                &env_from(&[(LOSS_ARGS_ENV, "rate=fast")])
            )
        );

        // A key the interceptor does not know is not an error — the reference
        // parses by `strncmp` over the keys it wants and returns zero for the
        // rest (`media/aeron_udp_channel_transport_loss.c:178-215`).
        assert!(
            resolve(
                INCOMING_INTERCEPTORS_ENV,
                "loss",
                &env_from(&[(LOSS_ARGS_ENV, "nonsense=1")])
            )
            .is_ok()
        );
    }

    #[test]
    fn the_loss_interceptor_drops_only_what_the_mask_names() {
        // The mask is one bit per **type**, so DATA — type 1 — is `1 << 1`,
        // which is what the harness's `1 << HeaderFlyweight.HDR_TYPE_DATA`
        // builds (`CTestMediaDriver.java:391-394`). At a rate of one it drops
        // every one of them.
        let mut incoming = Incoming::new(&[Interceptor::Loss(LossParams {
            rate: 1.0,
            seed: 1,
            message_type_mask: 1 << 1,
        })]);

        assert!(incoming.drops(&data_frame(0, 0, 1, 2, 8)));

        // An SM frame is type 3 and not in the mask — and the generator is not
        // advanced for it either, which is what `0.0 < rate && mask && draw`
        // short-circuits into.
        let sm = frame(frame_type::SM, 0, 0, 1, 2, 0);
        assert!(!incoming.drops(&sm));
    }

    #[test]
    fn a_rate_of_zero_drops_nothing_and_the_generator_never_moves() {
        let mut incoming = Incoming::new(&[Interceptor::Loss(LossParams {
            rate: 0.0,
            seed: 7,
            message_type_mask: !0,
        })]);

        for _ in 0..64 {
            assert!(!incoming.drops(&data_frame(0, 0, 1, 2, 8)));
        }
    }

    #[test]
    fn a_fixed_range_is_dropped_once_per_stream_and_session() {
        let params = FixedLossParams {
            term_id: 5,
            term_offset: 102,
            length: 64,
        };

        // The frame whose bytes overlap `[102, 166)` of term 5.
        let frame = data_frame(5, 96, 1, 2, 32);
        let tracking = &mut Tracking::default();

        assert!(should_drop_fixed_loss(&frame, &params, tracking));
        assert!(
            !should_drop_fixed_loss(&frame, &params, tracking),
            "the map has moved past it, which is what makes it *fixed*"
        );

        // Another session on the same stream gets its own.
        let other = data_frame(5, 96, 1, 3, 32);
        assert!(should_drop_fixed_loss(&other, &params, tracking));

        // Another term is not this parameter's business.
        let elsewhere = data_frame(6, 96, 1, 2, 32);
        assert!(!should_drop_fixed_loss(&elsewhere, &params, tracking));

        // Nor is an offset the range does not reach.
        let later = data_frame(5, 4096, 1, 2, 32);
        assert!(!should_drop_fixed_loss(&later, &params, tracking));
    }

    #[test]
    fn a_multi_gap_loss_drops_the_gaps_its_radix_names_and_stops_at_its_limit() {
        // A term id, a radix of 4 (two bits), gaps of 8 bytes, and two gaps.
        let params = MultiGapLossParams {
            term_id: 5,
            gap_radix_bits: 2,
            gap_radix_mask: !3u32,
            gap_length: 8,
            last_gap_limit: 24,
        };
        let tracking = &mut Tracking::default();

        // Offset 0 is a gap boundary and is dropped — and past the first
        // `last_gap_limit` nothing is, which is what keeps the loss inside the
        // stretch the test named.
        assert!(should_drop_multi_gap_loss(
            &data_frame(5, 0, 1, 2, 8),
            &params,
            tracking
        ));

        let beyond = data_frame(5, 64, 1, 2, 8);
        assert!(!should_drop_multi_gap_loss(&beyond, &params, tracking));
    }

    #[test]
    fn the_generator_is_the_one_the_reference_draws_from() {
        // Taken from `erand48` itself, state `(0, 0, 1)`: the first three values
        // it gives, which is what says the three shorts are in the order glibc
        // puts them in and not the other one.
        let mut xsubi = [0u16, 0, 1];
        let drawn: Vec<f64> = (0..3).map(|_| erand48(&mut xsubi)).collect();

        assert!(
            (drawn[0] - 0.900_100_708_007_851_6).abs() < 1e-15,
            "{drawn:?}"
        );
        assert!(
            (drawn[1] - 0.041_650_067_526_212_81).abs() < 1e-15,
            "{drawn:?}"
        );
        assert!(
            (drawn[2] - 0.810_017_842_414_925_6).abs() < 1e-15,
            "{drawn:?}"
        );
    }
}
