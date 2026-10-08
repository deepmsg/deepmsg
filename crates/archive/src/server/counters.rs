//! The counters an archive keeps.
//!
//! Three type ids are the archive's (`AeronCounters.java:745`, `:751`, `:826`),
//! and this slice uses two of them:
//!
//! | type id | label | key | who allocates it |
//! |---|---|---|---|
//! | 102 `Archive Control Sessions` | `… - archiveId=N` | `archiveId` (8) | the archive, once |
//! | 113 `control-session` | `control-session: … - archiveId=N` | `archiveId`, then `controlSessionId` (16) | the archive, one per session |
//! | 101 `Archive Errors` | `… - archiveId=N` | `archiveId` (8) | **not this build** — see below |
//!
//! # The one counter that is not allocated here
//!
//! 101 is what an archive allocates for itself when nothing gave it one:
//! `ArchiveCounters.allocateErrorCounter` is reached only on the `null ==
//! errorCounter` branch (`Archive.java:1354-1362`), which is the *standalone*
//! launcher. The aggregate launcher does the opposite — `ArchivingMediaDriver`
//! takes the **driver's** `ERRORS` counter (`SystemCounterDescriptor.ERRORS.id()`,
//! `ArchivingMediaDriver.java:87-89`) and hands it to the archive, and `Archive`
//! refuses to start without one (`:1375`). S1 builds the aggregate launcher, so
//! the archive writes a counter it was given and never allocates 101.
//!
//! That is the whole of what [`ErrorCounter`] is: an id something else chose.
//!
//! # The label is the acceptance criterion
//!
//! `shouldSendClientInfoToArchive` (`client/aeron_archive_test.cpp:4131-4195`)
//! finds the 113 counter by type id, reads its **key** back as two `int64`s, and
//! looks for `name=… version=… commit=…` and `archiveId=…` inside its **label**.
//! The `name=…` text is the client's own doing — it is the `clientInfo` the
//! client sends in its `AuthConnectRequest`
//! (`aeron-archive/src/main/c/client/aeron_archive_proxy.c:99-110`) — so the
//! archive's job is to put that string in the label, byte for byte, where
//! `ControlSessionCounter.java:74-78` puts it.
//!
//! Composing it is the *conductor's*: `newControlSession` passes
//! `Strings.isEmpty(clientInfo) ? imageInfo : clientInfo + " " + imageInfo`
//! (`ArchiveConductor.java:492-498`), so `imageInfo` is always in the label and
//! `clientInfo` is prepended to it when there is one. This module takes the
//! finished string, as `ControlSessionCounter.allocate` does.
//!
//! # What is written where
//!
//! A counter's key and label live in the **metadata** region, which only the
//! driver writes — so every allocation here goes through the client as an
//! `ADD_COUNTER` command. Its **value** and **reference id** live in the values
//! region, and *those* the archive writes itself, on its own mapping of the CnC
//! file: [`ControlSessionCounter::bind`] names the response publication and the
//! session, and the session-count and error-counter moves are the other two
//! stores this module makes.
//!
//! A slot the driver has allocated is therefore a counter the archive *must*
//! give back: it outlives the `ADD_COUNTER` that made it, and the archive's own
//! client is the lifetime it is tied to. [`ControlSessionCounter::release`] is
//! the way back, and it is **not** the blocking `remove_counter`: the
//! reference's removal is asynchronous in both of its arms
//! (`ControlSession.java:176-183` over `ClientConductor.removeCounter`,
//! `ClientConductor.java:1604-1624`, which sends the command and waits for
//! nothing), and a conductor that blocked here would stall every other session
//! behind one that is going away.

use std::fmt;
use std::time::Duration;

use deepmsg_client::client::{AsyncAdd, AsyncAddPoll, Client, CommandError};
use deepmsg_cnc::counters::CountersReader;
use deepmsg_core::buffer::ReadWrite;

/// `AeronCounters.ARCHIVE_ERROR_COUNT_TYPE_ID` (`AeronCounters.java:745`).
///
/// Named for the id space's sake and for the citation; this slice allocates no
/// counter of this type — see the module note.
pub const ARCHIVE_ERROR_COUNT_TYPE_ID: i32 = 101;

/// `AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID` (`AeronCounters.java:751`).
pub const ARCHIVE_CONTROL_SESSIONS_TYPE_ID: i32 = 102;

/// `AeronCounters.ARCHIVE_CONTROL_SESSION_TYPE_ID` (`AeronCounters.java:826`).
pub const ARCHIVE_CONTROL_SESSION_TYPE_ID: i32 = 113;

/// `AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID`
/// (`AeronCounters.java:739`).
///
/// The one archive counter that is **not** keyed by the archive id alone — see
/// [`crate::server::recording_pos`], which owns its key and label.
pub const ARCHIVE_RECORDING_POSITION_TYPE_ID: i32 = 100;

/// `AeronCounters.ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID`
/// (`AeronCounters.java:771`).
pub const ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID: i32 = 105;

/// `AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID`
/// (`AeronCounters.java:778`).
pub const ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID: i32 = 106;

/// `AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID`
/// (`AeronCounters.java:785`).
pub const ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID: i32 = 107;

/// `AeronCounters.ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID`
/// (`AeronCounters.java:812`).
pub const ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID: i32 = 111;

/// `ArchiveCounters.ARCHIVE_ID_LABEL_SUFFIX` (`ArchiveCounters.java:35`), which
/// every archive counter's label ends with.
const ARCHIVE_ID_LABEL_SUFFIX: &str = " - archiveId=";

/// The name the 102 counter is allocated under (`Archive.java:1561`).
const CONTROL_SESSIONS_NAME: &str = "Archive Control Sessions";

/// The name the 111 counter is allocated under (`Archive.java:1568`).
pub const RECORDING_SESSIONS_NAME: &str = "Archive Recording Sessions";

/// The names the recorder's three counters are allocated under
/// (`Archive.java:1601`, `:1612`, `:1623`).
pub const RECORDER_MAX_WRITE_TIME_NAME: &str = "archive-recorder max write time in ns";
/// See [`RECORDER_MAX_WRITE_TIME_NAME`].
pub const RECORDER_TOTAL_WRITE_BYTES_NAME: &str = "archive-recorder total write bytes";
/// See [`RECORDER_MAX_WRITE_TIME_NAME`].
pub const RECORDER_TOTAL_WRITE_TIME_NAME: &str = "archive-recorder total write time in ns";

/// `ControlSessionCounter.NAME` and the separator after it
/// (`ControlSessionCounter.java:57`, `:75-76`). One constant, because nothing
/// ever writes one without the other.
const CONTROL_SESSION_LABEL_PREFIX: &str = "control-session: ";

/// The archive id, and nothing else (`ArchiveCounters.java:60-62`).
pub const CONTROL_SESSIONS_KEY_LENGTH: usize = 8;

/// The archive id and the control session id
/// (`ControlSessionCounter.java:70-72`).
pub const CONTROL_SESSION_KEY_LENGTH: usize = 16;

/// The key of every archive counter but the 100 and the 113: the archive id.
///
/// `ArchiveCounters.allocate` puts the id at offset 0 and declares the key to
/// be exactly the 8 bytes it wrote (`ArchiveCounters.java:59-62`), which is
/// what makes these counters findable by
/// [`find_archive_id_counter`] and by the reference's own
/// `ArchiveCounters.find` (`:139-159`).
pub fn archive_id_key(archive_id: i64) -> [u8; CONTROL_SESSIONS_KEY_LENGTH] {
    archive_id.to_le_bytes()
}

/// `" - archiveId=" + archiveId`, the piece every archive counter's label ends
/// with — named apart from the label itself because the 100 has to measure it
/// before it writes its own (`ArchiveCounters.appendArchiveIdLabel`, `:102-110`,
/// and `lengthOfArchiveIdLabel`, `:117-130`).
pub fn archive_id_suffix(archive_id: i64) -> String {
    format!("{ARCHIVE_ID_LABEL_SUFFIX}{archive_id}")
}

/// `name + " - archiveId=" + archiveId`, which is the label every counter
/// [`archive_id_key`] keys (`ArchiveCounters.allocate`, `:64-67`).
pub fn archive_id_label(name: &str, archive_id: i64) -> String {
    format!("{name}{}", archive_id_suffix(archive_id))
}

/// `ArchiveCounters.lengthOfArchiveIdLabel` (`:117-130`), which the 100's label
/// is truncated against.
pub fn length_of_archive_id_label(archive_id: i64) -> usize {
    archive_id_suffix(archive_id).len()
}

/// The key of the 102 counter: the archive id.
pub fn control_sessions_key(archive_id: i64) -> [u8; CONTROL_SESSIONS_KEY_LENGTH] {
    archive_id_key(archive_id)
}

/// The key of a 113 counter: the archive id, then the control session id.
///
/// `ARCHIVE_ID_KEY_OFFSET` is 0 and `CONTROL_SESSION_ID_KEY_OFFSET` is
/// `ARCHIVE_ID_KEY_OFFSET + SIZE_OF_LONG` (`ControlSessionCounter.java:47`,
/// `:52`), and both are written with `putLong` (`:70-71`) — the buffer's own
/// order, which is the little-endian `memcpy` the C test reads them back with
/// (`client/aeron_archive_test.cpp:4183-4186`).
pub fn control_session_key(
    archive_id: i64,
    control_session_id: i64,
) -> [u8; CONTROL_SESSION_KEY_LENGTH] {
    let mut key = [0u8; CONTROL_SESSION_KEY_LENGTH];
    key[..8].copy_from_slice(&archive_id.to_le_bytes());
    key[8..].copy_from_slice(&control_session_id.to_le_bytes());
    key
}

/// `"Archive Control Sessions" + " - archiveId=" + archiveId`
/// (`Archive.java:1561` and `ArchiveCounters.appendArchiveIdLabel`, `:102-109`).
pub fn control_sessions_label(archive_id: i64) -> String {
    archive_id_label(CONTROL_SESSIONS_NAME, archive_id)
}

/// `"control-session" + ": " + clientInfo + " - archiveId=" + archiveId`
/// (`ControlSessionCounter.java:74-78`).
///
/// `client_info` is the finished third segment, so an empty one leaves the
/// separator and the suffix next to each other — `"control-session:  -
/// archiveId=42"`, with two spaces — because the reference appends the empty
/// string like any other (`:77`).
pub fn control_session_label(client_info: &str, archive_id: i64) -> String {
    format!("{CONTROL_SESSION_LABEL_PREFIX}{client_info}{ARCHIVE_ID_LABEL_SUFFIX}{archive_id}")
}

/// What can go wrong asking for a counter, or finding one again.
#[derive(Debug)]
pub enum CounterError {
    /// The client could not send the command, or the driver refused it.
    Command(CommandError),
    /// The counter the driver allocated is not the type id that was asked for
    /// (`AeronCounters.validateCounterTypeId`, `AeronCounters.java:1540-1547`).
    ///
    /// The reference throws a `ConfigurationException` here, at archive
    /// construction, because every reader of an archive's counters keys on the
    /// type id: a 102 that is not a 102 is an archive whose session count no
    /// tool can find.
    WrongTypeId {
        /// The type id the archive asked for.
        expected: i32,
        /// The type id the counter actually carries.
        actual: i32,
    },
    /// The driver has no allocated counter in that slot, so there is nothing to
    /// check and nothing to bind.
    ///
    /// The reference cannot meet this: `validateCounterTypeId` reads whatever
    /// the slot holds and compares it, so a slot with no counter reads as type
    /// id 0 and fails as a type mismatch instead.
    UnknownCounter {
        /// The slot that was asked about.
        counter_id: i32,
    },
}

impl From<CommandError> for CounterError {
    fn from(error: CommandError) -> Self {
        Self::Command(error)
    }
}

impl fmt::Display for CounterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Command(error) => write!(f, "{error}"),
            Self::WrongTypeId { expected, actual } => {
                write!(f, "counter has typeId={actual}, expected={expected}")
            }
            Self::UnknownCounter { counter_id } => {
                write!(f, "no allocated counter at counterId={counter_id}")
            }
        }
    }
}

impl std::error::Error for CounterError {}

/// A client of a media driver, seen as the counters an archive asks for.
///
/// The twin of [`Publications`](crate::server::control_session::Publications),
/// and it exists for the same reason: what the archive keeps across turns is the
/// registration id an `ADD_COUNTER` drew, not a counter — a counter lives inside
/// the client, which the archive is a *user* of — so the trait is the seam that
/// lets the counter lifecycle be driven without a driver.
///
/// Unlike `Publications` there is no `is_*`/`poll_*` pair here, because there is
/// only one question to ask about a pending counter and the reference asks it
/// the same way: `aeron.getCounter(registrationId)` (`ControlSession.java:892`),
/// which answers "not yet" until the driver's reply has been read.
pub trait Counters {
    /// `aeron.addCounter(...)` (`ArchiveCounters.java:67`), which waits for the
    /// driver and answers with the slot it allocated.
    ///
    /// The reference blocks here too, and only at archive construction
    /// (`Archive.java:1560`), where there is no session to stall behind.
    fn add_counter(
        &mut self,
        type_id: i32,
        key: &[u8],
        label: &str,
        timeout: Duration,
    ) -> Result<i32, CounterError>;

    /// `aeron.asyncAddCounter(...)` (`ControlSessionCounter.java:80`), which
    /// answers at once with the registration id the driver's reply will be read
    /// under.
    fn async_add_counter(
        &mut self,
        type_id: i32,
        key: &[u8],
        label: &str,
        timeout: Duration,
    ) -> Result<i64, CounterError>;

    /// `aeron.getCounter(registrationId)` (`ControlSession.java:892`): whether
    /// the driver has answered yet, draining the client on the way.
    fn poll_counter(&mut self, registration_id: i64) -> AsyncAddPoll;

    /// The slot a registration id names, once [`Counters::poll_counter`] has
    /// said the driver answered. `None` until then.
    fn counter_id(&self, registration_id: i64) -> Option<i32>;

    /// Give a counter back (`ControlSession.java:176-183`), without waiting for
    /// the driver to say it is gone.
    fn release_counter(&mut self, registration_id: i64) -> Result<(), CounterError>;
}

impl Counters for Client {
    fn add_counter(
        &mut self,
        type_id: i32,
        key: &[u8],
        label: &str,
        timeout: Duration,
    ) -> Result<i32, CounterError> {
        Ok(Client::add_counter(self, type_id, key, label, timeout)?.counter_id())
    }

    fn async_add_counter(
        &mut self,
        type_id: i32,
        key: &[u8],
        label: &str,
        timeout: Duration,
    ) -> Result<i64, CounterError> {
        Ok(Client::async_add_counter(self, type_id, key, label, timeout)?.registration_id())
    }

    fn poll_counter(&mut self, registration_id: i64) -> AsyncAddPoll {
        // The handle is put back together from the id here and nowhere else:
        // the client's `async_add_poll` is keyed by the id, and the id is what
        // the archive kept. `AsyncAdd::counter` and not `::publication`,
        // because the removal this handle would send is a `REMOVE_COUNTER`.
        self.async_add_poll(AsyncAdd::counter(registration_id))
    }

    fn counter_id(&self, registration_id: i64) -> Option<i32> {
        Client::counter(self, registration_id).map(|counter| counter.counter_id())
    }

    fn release_counter(&mut self, registration_id: i64) -> Result<(), CounterError> {
        // The add's own cancel, because that is what a counter removal *is*
        // here: `REMOVE_COUNTER` for the id the add drew, sent without waiting
        // (`Client::async_add_cancel`). It answers for a counter the client is
        // still holding and for one whose reply never arrived, which is exactly
        // the two arms `ControlSession.close` has.
        self.async_add_cancel(AsyncAdd::counter(registration_id))?;
        Ok(())
    }
}

/// The 102 counter: how many control sessions are open.
///
/// Allocated once, at archive construction (`Archive.java:1558-1563`), and then
/// only ever moved by one: up when a session is added
/// (`ArchiveConductor.java:520`), down when one is removed
/// (`ControlSessionAdapter.java:1158-1161`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlSessionsCounter {
    counter_id: i32,
}

impl ControlSessionsCounter {
    /// Allocate the archive's session count and check what came back.
    ///
    /// The check is `validateCounterTypeId` (`Archive.java:1563`): the
    /// reference throws rather than start an archive whose session count no
    /// tool can find, and so does this.
    ///
    /// # Errors
    ///
    /// [`CounterError`] if the client or the driver refused the counter, if the
    /// counter it allocated is not a 102, or if its slot cannot be read back.
    pub fn allocate<C: Counters, Access>(
        client: &mut C,
        counters: &CountersReader<'_, Access>,
        archive_id: i64,
        timeout: Duration,
    ) -> Result<Self, CounterError> {
        let key = control_sessions_key(archive_id);
        let label = control_sessions_label(archive_id);

        let counter_id =
            client.add_counter(ARCHIVE_CONTROL_SESSIONS_TYPE_ID, &key, &label, timeout)?;

        check_type_id(counters, counter_id, ARCHIVE_CONTROL_SESSIONS_TYPE_ID)?;

        Ok(Self { counter_id })
    }

    /// The values-region slot, which is what every write below names.
    pub const fn counter_id(&self) -> i32 {
        self.counter_id
    }

    /// One more open session (`ArchiveConductor.java:520`).
    ///
    /// `incrementRelease` is a read and a release write rather than an atomic
    /// add — that is what the reference's is too
    /// (`org.agrona.concurrent.status.Counter.incrementRelease`) — and only the
    /// archive's own conductor moves this counter, so the read never races a
    /// writer.
    ///
    /// `None` when the id is not a slot in this region at all. That is a weaker
    /// guard than the reference's `!controlSessionsCounter.isClosed()`
    /// (`ControlSessionAdapter.java:1159`), and deliberately so: this handle is
    /// a slot and not an object, so "still mine" is the caller's release
    /// discipline rather than something the call can ask
    /// ([`CountersReader::is_active`] is the question, and it needs the
    /// registration id this does not carry).
    pub fn increment(&self, counters: &CountersReader<'_, ReadWrite>) -> Option<i64> {
        self.bump(counters, 1)
    }

    /// One fewer open session (`ControlSessionAdapter.java:1158-1161`), with
    /// the same `None` as [`Self::increment`].
    pub fn decrement(&self, counters: &CountersReader<'_, ReadWrite>) -> Option<i64> {
        self.bump(counters, -1)
    }

    /// The count as it is now, for a caller that wants to read rather than move
    /// it.
    pub fn value<Access>(&self, counters: &CountersReader<'_, Access>) -> Option<i64> {
        counters.value(self.counter_id)
    }

    fn bump(&self, counters: &CountersReader<'_, ReadWrite>, by: i64) -> Option<i64> {
        let next = counters.value(self.counter_id)?.wrapping_add(by);
        counters.set_value(self.counter_id, next).map(|()| next)
    }
}

/// Ask the driver for the archive's control-sessions counter, and do not wait
/// for the answer.
///
/// [`ControlSessionsCounter::allocate`] sends the same command and blocks until
/// the driver answers it. That is fine for a process that has a driver running
/// on its own threads, and impossible for one that **is** the driver's loop: a
/// conductor is driven inside the turn that drives the driver, so a blocking
/// command waits for a driver nobody is driving, and the wait runs out against
/// the driver's own heartbeat.
///
/// So the 102 is asked for the way the 113 already is — a command out, the
/// answer read on a later turn — and the two halves are here rather than on the
/// type because the key, the label and the type check are this module's
/// knowledge, not the caller's.
///
/// # Errors
///
/// [`CounterError`] if the command could not be written or sent. The driver's
/// own answer arrives through [`claim_control_sessions_counter`].
pub fn request_control_sessions_counter<C: Counters>(
    client: &mut C,
    archive_id: i64,
    timeout: Duration,
) -> Result<i64, CounterError> {
    let key = control_sessions_key(archive_id);
    let label = control_sessions_label(archive_id);

    client.async_add_counter(ARCHIVE_CONTROL_SESSIONS_TYPE_ID, &key, &label, timeout)
}

/// Take up the counter [`request_control_sessions_counter`] asked for, once the
/// driver has allocated it.
///
/// `Ok(None)` means "not yet" rather than "no", exactly as
/// [`ControlSessionCounter::claim`] does, and the type check is
/// `validateCounterTypeId` (`Archive.java:1563`) — the reference throws rather
/// than start an archive whose session count no tool can find.
///
/// # Errors
///
/// [`CounterError`] if the driver refused the add, if the counter it allocated
/// is not a 102, or if its slot cannot be read back.
pub fn claim_control_sessions_counter<C: Counters, Access>(
    client: &mut C,
    counters: &CountersReader<'_, Access>,
    registration_id: i64,
) -> Result<Option<ControlSessionsCounter>, CounterError> {
    let counter_id = match client.poll_counter(registration_id) {
        AsyncAddPoll::Ready => {
            let Some(counter_id) = client.counter_id(registration_id) else {
                return Ok(None);
            };
            counter_id
        }
        AsyncAddPoll::Awaiting | AsyncAddPoll::Unknown => return Ok(None),
        AsyncAddPoll::Failed(error) => return Err(CounterError::Command(error)),
    };

    check_type_id(counters, counter_id, ARCHIVE_CONTROL_SESSIONS_TYPE_ID)?;

    Ok(Some(ControlSessionsCounter { counter_id }))
}

/// One of the archive's counters that is keyed by nothing but the archive id:
/// the recording session count (111) and the recorder's three write statistics
/// (105, 106, 107) — everything `ArchiveCounters.allocate` makes
/// (`ArchiveCounters.java:52-69`).
///
/// The four differ in what moves them and in nothing else: 111 goes up and down
/// with the recording sessions (`ArchiveConductor.java:2056`, `:1362`), and the
/// recorder's three are set outright once per turn that wrote something
/// (`:2732-2743`). So they share one handle rather than four — the type id and
/// the name are the caller's, and the key and the label suffix are this
/// module's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveIdCounter {
    counter_id: i32,
}

impl ArchiveIdCounter {
    /// Allocate one and check what came back, for a caller with a driver
    /// running on its own threads.
    ///
    /// The check is `validateCounterTypeId` (`Archive.java:1564`, `:1604`,
    /// `:1615`, `:1626`), which each of the four does after allocating.
    ///
    /// # Errors
    ///
    /// [`CounterError`] if the client or the driver refused the counter, if the
    /// counter it allocated is not the type that was asked for, or if its slot
    /// cannot be read back.
    pub fn allocate<C: Counters, Access>(
        client: &mut C,
        counters: &CountersReader<'_, Access>,
        type_id: i32,
        name: &str,
        archive_id: i64,
        timeout: Duration,
    ) -> Result<Self, CounterError> {
        let counter_id = client.add_counter(
            type_id,
            &archive_id_key(archive_id),
            &archive_id_label(name, archive_id),
            timeout,
        )?;

        check_type_id(counters, counter_id, type_id)?;

        Ok(Self { counter_id })
    }

    /// The values-region slot.
    pub const fn counter_id(&self) -> i32 {
        self.counter_id
    }

    /// What the counter reads now, which is what `AeronStat` shows and what the
    /// C harness reads a recording's position out of
    /// (`aeron_archive_test.cpp:267-273`).
    pub fn value<Access>(&self, counters: &CountersReader<'_, Access>) -> Option<i64> {
        counters.value(self.counter_id)
    }

    /// Set it outright — the recorder's three, once per turn that wrote
    /// (`ArchiveConductor.java:2737-2739`).
    pub fn set(&self, counters: &CountersReader<'_, ReadWrite>, value: i64) -> Option<()> {
        counters.set_value(self.counter_id, value)
    }

    /// One more recording session (`ArchiveConductor.java:2056`).
    pub fn increment(&self, counters: &CountersReader<'_, ReadWrite>) -> Option<i64> {
        self.bump(counters, 1)
    }

    /// One fewer (`:1362`), with the same `None` as
    /// [`ControlSessionsCounter::increment`].
    pub fn decrement(&self, counters: &CountersReader<'_, ReadWrite>) -> Option<i64> {
        self.bump(counters, -1)
    }

    fn bump(&self, counters: &CountersReader<'_, ReadWrite>, by: i64) -> Option<i64> {
        let next = counters.value(self.counter_id)?.wrapping_add(by);
        counters.set_value(self.counter_id, next).map(|()| next)
    }
}

/// The counter of `type_id` belonging to `archive_id`, or `None`
/// (`ArchiveCounters.find`, `ArchiveCounters.java:139-159`).
///
/// The reference walks the metadata region from counter 0 and stops at the
/// first free slot, comparing the type id and the first eight bytes of the key
/// — the archive id, which is all any of these keys holds. The walk is
/// [`CountersReader::for_each`]'s.
///
/// One caller so far, and it is the reason the function exists: the 105 is
/// allocated only if no other archive has one for this archive id, and the
/// archive refuses to start if one does (`Archive.java:1586-1595`). The
/// reference checks that one and not the other three.
pub fn find_archive_id_counter<Access>(
    counters: &CountersReader<'_, Access>,
    type_id: i32,
    archive_id: i64,
) -> Option<i32> {
    let wanted = archive_id_key(archive_id);
    let mut found = None;

    counters.for_each(|descriptor| {
        if found.is_none()
            && descriptor.type_id == type_id
            && counters
                .key(descriptor.counter_id)
                .is_some_and(|key| key[..wanted.len()] == wanted)
        {
            found = Some(descriptor.counter_id);
        }
    });

    found
}

/// Ask the driver for one of the archive-id-keyed counters, and do not wait for
/// the answer.
///
/// The blocking [`ArchiveIdCounter::allocate`] is fine for a process with a
/// driver on its own threads and impossible for one that **is** the driver's
/// loop — see [`request_control_sessions_counter`] for the whole of that
/// argument, which is the same one.
///
/// # Errors
///
/// [`CounterError`] if the command could not be written or sent. The driver's
/// own answer arrives through [`claim_archive_id_counter`].
pub fn request_archive_id_counter<C: Counters>(
    client: &mut C,
    type_id: i32,
    name: &str,
    archive_id: i64,
    timeout: Duration,
) -> Result<i64, CounterError> {
    client.async_add_counter(
        type_id,
        &archive_id_key(archive_id),
        &archive_id_label(name, archive_id),
        timeout,
    )
}

/// Take up the counter [`request_archive_id_counter`] asked for, once the
/// driver has allocated it.
///
/// `Ok(None)` means "not yet" rather than "no", exactly as
/// [`claim_control_sessions_counter`] does.
///
/// # Errors
///
/// [`CounterError`] if the driver refused the add, if the counter it allocated
/// is not the type that was asked for, or if its slot cannot be read back.
pub fn claim_archive_id_counter<C: Counters, Access>(
    client: &mut C,
    counters: &CountersReader<'_, Access>,
    type_id: i32,
    registration_id: i64,
) -> Result<Option<ArchiveIdCounter>, CounterError> {
    let counter_id = match client.poll_counter(registration_id) {
        AsyncAddPoll::Ready => {
            let Some(counter_id) = client.counter_id(registration_id) else {
                return Ok(None);
            };
            counter_id
        }
        AsyncAddPoll::Awaiting | AsyncAddPoll::Unknown => return Ok(None),
        AsyncAddPoll::Failed(error) => return Err(CounterError::Command(error)),
    };

    check_type_id(counters, counter_id, type_id)?;

    Ok(Some(ArchiveIdCounter { counter_id }))
}

/// `validateCounterTypeId` (`AeronCounters.java:1540-1547`), which the four
/// archive-id-keyed counters do at `Archive.java:1574`, `:1604`, `:1615` and
/// `:1626`.
pub(super) fn check_type_id<Access>(
    counters: &CountersReader<'_, Access>,
    counter_id: i32,
    expected: i32,
) -> Result<(), CounterError> {
    match counters.get(counter_id) {
        Some(descriptor) if descriptor.type_id == expected => Ok(()),
        Some(descriptor) => Err(CounterError::WrongTypeId {
            expected,
            actual: descriptor.type_id,
        }),
        None => Err(CounterError::UnknownCounter { counter_id }),
    }
}

/// The 113 counter: one control session.
///
/// Made when a session is (`ArchiveConductor.java:493-498`), bound when its
/// response publication arrives (`ControlSession.java:890-908`), and given back
/// when the session goes (`:176-183`).
///
/// The three steps are in this order because each needs something the one
/// before it produced: the allocation needs only the ids, the claim needs the
/// driver to have answered, and the bind needs the counter **and** the
/// publication — the reference will not leave `INIT` until it has both
/// (`:895`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlSessionCounter {
    registration_id: i64,
    control_session_id: i64,
    /// The slot, once the driver's answer has been read.
    counter_id: Option<i32>,
}

impl ControlSessionCounter {
    /// Ask for the session's counter (`ArchiveConductor.java:493-498`).
    ///
    /// Asynchronous, unlike the 102: this happens on the turn a connect request
    /// arrives, and a conductor that waited for the driver there would be
    /// holding up every other session.
    ///
    /// `client_info` is the label's third segment, already composed — see the
    /// module note for who composes it and why it is not this call.
    ///
    /// # Errors
    ///
    /// [`CounterError`] if the command could not be written or sent. The
    /// driver's own answer arrives later, through [`Self::claim`].
    pub fn allocate<C: Counters>(
        client: &mut C,
        archive_id: i64,
        control_session_id: i64,
        client_info: &str,
        timeout: Duration,
    ) -> Result<Self, CounterError> {
        let key = control_session_key(archive_id, control_session_id);
        let label = control_session_label(client_info, archive_id);

        let registration_id =
            client.async_add_counter(ARCHIVE_CONTROL_SESSION_TYPE_ID, &key, &label, timeout)?;

        Ok(Self {
            registration_id,
            control_session_id,
            counter_id: None,
        })
    }

    /// The registration id the add drew, which is what every later step names.
    pub const fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /// The control session id, which becomes the counter's value.
    pub const fn control_session_id(&self) -> i64 {
        self.control_session_id
    }

    /// The values-region slot, once [`Self::claim`] has found one.
    pub const fn counter_id(&self) -> Option<i32> {
        self.counter_id
    }

    /// `aeron.getCounter(sessionCounterRegistrationId)`
    /// (`ControlSession.java:890-893`): take the counter up if the driver has
    /// allocated it.
    ///
    /// `Ok(false)` means "not yet" rather than "no", and the reference treats
    /// it that way — the session stays in `INIT` and asks again next turn. It
    /// answers `Ok(true)` once the counter is in hand and keeps answering it,
    /// because a claim is not consumed by looking at it.
    ///
    /// # Errors
    ///
    /// [`CounterError`] if the driver refused the add. That is the reference's
    /// `RESOURCE_TEMPORARILY_UNAVAILABLE` escalated: it can never become a
    /// counter, so a session that kept waiting would wait out its whole connect
    /// timeout for something that is not coming.
    pub fn claim<C: Counters>(&mut self, client: &mut C) -> Result<bool, CounterError> {
        if self.counter_id.is_some() {
            return Ok(true);
        }

        match client.poll_counter(self.registration_id) {
            AsyncAddPoll::Ready => {
                let Some(counter_id) = client.counter_id(self.registration_id) else {
                    return Ok(false);
                };

                self.counter_id = Some(counter_id);

                Ok(true)
            }
            AsyncAddPoll::Awaiting | AsyncAddPoll::Unknown => Ok(false),
            AsyncAddPoll::Failed(error) => Err(CounterError::Command(error)),
        }
    }

    /// Bind the counter to the session (`ControlSession.java:895-908`): the
    /// reference id names the response publication, the value is the control
    /// session id.
    ///
    /// Both writes are the archive's own — they are in the values region, which
    /// the archive maps writable, not behind a command — and they happen
    /// together, because a counter with a value and no reference id is one a
    /// reader cannot follow anywhere.
    ///
    /// `None` before [`Self::claim`] has found the counter, or if the slot has
    /// been reclaimed since: there is nothing to bind.
    pub fn bind(
        &self,
        counters: &CountersReader<'_, ReadWrite>,
        publication_registration_id: i64,
    ) -> Option<()> {
        let counter_id = self.counter_id?;

        counters.set_reference_id(counter_id, publication_registration_id)?;
        counters.set_value(counter_id, self.control_session_id)
    }

    /// Give the counter back (`ControlSession.java:176-183`).
    ///
    /// The reference has two arms here — close the counter it holds, or
    /// `asyncRemoveCounter` the registration id it never got a counter for —
    /// and this client has one command for both, so there is one call.
    ///
    /// # Errors
    ///
    /// [`CounterError`] if the removal could not be sent. The reference cannot
    /// fail here: its `asyncRemoveCounter` is a command into a ring, and the
    /// only way out of that is the process going away.
    pub fn release<C: Counters>(&mut self, client: &mut C) -> Result<(), CounterError> {
        client.release_counter(self.registration_id)
    }
}

/// The archive's error counter — a counter somebody else allocated.
///
/// In the aggregate launcher that is the **driver's** `ERRORS`
/// (`ArchivingMediaDriver.java:87-89`), which is why this holds an id and no
/// way to make one. What it is for is the reference's `CountedErrorHandler`
/// (`Archive.java:1379`): every error the archive observes moves it up by one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorCounter {
    counter_id: i32,
}

impl ErrorCounter {
    /// An error counter, by the id whoever launched this archive chose.
    pub const fn new(counter_id: i32) -> Self {
        Self { counter_id }
    }

    /// The values-region slot.
    pub const fn counter_id(&self) -> i32 {
        self.counter_id
    }

    /// One more error observed (`Archive.java:1379`).
    ///
    /// `increment`, not `incrementRelease`: an `AtomicCounter` over the
    /// driver's values buffer has nothing to release *to*, and the reference's
    /// own spelling of this one is a plain `increment`
    /// (`DedicatedModeArchiveConductor.java:222-224`). `None` when the id is
    /// not a slot in this region at all, the same guard as
    /// [`ControlSessionsCounter::increment`].
    pub fn increment(&self, counters: &CountersReader<'_, ReadWrite>) -> Option<i64> {
        let next = counters.value(self.counter_id)?.wrapping_add(1);
        counters.set_value(self.counter_id, next).map(|()| next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use deepmsg_cnc::layout;
    use deepmsg_core::buffer::AtomicBuffer;

    /// An aligned region the tests can write into, standing in for a mapped
    /// one, as `deepmsg-cnc`'s own counter tests do.
    #[repr(align(64))]
    struct Region(Vec<u8>);

    impl Region {
        fn zeroed(len: usize) -> Self {
            Self(vec![0u8; len])
        }

        fn buffer_mut(&mut self) -> AtomicBuffer<'_, ReadWrite> {
            AtomicBuffer::from_slice_mut(&mut self.0).expect("aligned region")
        }
    }

    /// A metadata region holding one allocated counter at `counter_id`, so
    /// `CountersReader::get` can describe the slot a bind named.
    ///
    /// Sized to reach `counter_id`, because the region's length is what bounds
    /// the ids a reader will answer about.
    fn metadata(counter_id: i32, type_id: i32) -> Region {
        let len = (counter_id as usize + 1) * layout::COUNTER_METADATA_LENGTH;
        let mut region = Region::zeroed(len.max(4 * layout::COUNTER_METADATA_LENGTH));
        let base = counter_id as usize * layout::COUNTER_METADATA_LENGTH;

        region.0[base + layout::COUNTER_TYPE_ID_OFFSET..base + layout::COUNTER_TYPE_ID_OFFSET + 4]
            .copy_from_slice(&type_id.to_le_bytes());
        // Published last, with release, as a writer does.
        region.0[base + layout::COUNTER_STATE_OFFSET..base + layout::COUNTER_STATE_OFFSET + 4]
            .copy_from_slice(&layout::COUNTER_STATE_ALLOCATED.to_le_bytes());

        region
    }

    /// A values region with room for `count` counters.
    fn values(count: usize) -> Region {
        Region::zeroed(count * layout::COUNTER_VALUE_LENGTH)
    }

    /// A client that answers counter commands out of a script, and writes down
    /// what it was asked for.
    #[derive(Default)]
    struct FakeCounters {
        /// Every `ADD_COUNTER` as it went out: type id, key, label.
        added: Vec<(i32, Vec<u8>, String)>,
        /// The slot each registration id resolved to.
        slots: HashMap<i64, i32>,
        /// The registration ids the driver has answered, which is what
        /// `poll_counter` reads.
        ready: Vec<i64>,
        released: Vec<i64>,
        next_registration_id: i64,
        /// The slot a blocking add hands back, so a test can point it at a
        /// counter of the wrong type.
        add_slot: i32,
        /// Whether the next add is refused, which the driver can do.
        refuse: bool,
    }

    impl FakeCounters {
        /// The one counter that was asked for.
        fn only_add(&self) -> &(i32, Vec<u8>, String) {
            assert_eq!(1, self.added.len(), "exactly one counter was asked for");
            &self.added[0]
        }

        /// The driver has answered this registration id with this slot.
        fn answer(&mut self, registration_id: i64, counter_id: i32) {
            self.ready.push(registration_id);
            self.slots.insert(registration_id, counter_id);
        }
    }

    impl Counters for FakeCounters {
        fn add_counter(
            &mut self,
            type_id: i32,
            key: &[u8],
            label: &str,
            _timeout: Duration,
        ) -> Result<i32, CounterError> {
            self.added.push((type_id, key.to_vec(), label.to_owned()));

            if self.refuse {
                return Err(CounterError::Command(CommandError::Encoding));
            }

            Ok(self.add_slot)
        }

        fn async_add_counter(
            &mut self,
            type_id: i32,
            key: &[u8],
            label: &str,
            _timeout: Duration,
        ) -> Result<i64, CounterError> {
            self.added.push((type_id, key.to_vec(), label.to_owned()));

            if self.refuse {
                return Err(CounterError::Command(CommandError::Encoding));
            }

            let registration_id = self.next_registration_id;
            self.next_registration_id += 1;

            Ok(registration_id)
        }

        fn poll_counter(&mut self, registration_id: i64) -> AsyncAddPoll {
            if self.ready.contains(&registration_id) {
                AsyncAddPoll::Ready
            } else {
                AsyncAddPoll::Awaiting
            }
        }

        fn counter_id(&self, registration_id: i64) -> Option<i32> {
            self.slots.get(&registration_id).copied()
        }

        fn release_counter(&mut self, registration_id: i64) -> Result<(), CounterError> {
            self.released.push(registration_id);
            Ok(())
        }
    }

    const TIMEOUT: Duration = Duration::from_secs(5);
    const ARCHIVE_ID: i64 = 42;

    #[test]
    fn the_102_key_is_the_archive_id_and_the_label_names_it() {
        let mut client = FakeCounters::default();
        let mut meta = metadata(0, ARCHIVE_CONTROL_SESSIONS_TYPE_ID);
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        let counter = ControlSessionsCounter::allocate(&mut client, &counters, ARCHIVE_ID, TIMEOUT)
            .expect("the driver answered");

        let (type_id, key, label) = client.only_add();
        assert_eq!(ARCHIVE_CONTROL_SESSIONS_TYPE_ID, *type_id);
        assert_eq!(102, ARCHIVE_CONTROL_SESSIONS_TYPE_ID);
        assert_eq!(CONTROL_SESSIONS_KEY_LENGTH, key.len());
        assert_eq!(ARCHIVE_ID.to_le_bytes().to_vec(), *key);
        assert_eq!("Archive Control Sessions - archiveId=42", label);
        assert_eq!(0, counter.counter_id());
    }

    /// The reference throws here rather than start an archive whose session
    /// count no tool can find (`Archive.java:1563`).
    #[test]
    fn a_102_that_is_not_a_102_stops_the_archive() {
        let mut client = FakeCounters::default();
        // The driver handed back a slot somebody else's counter occupies.
        let mut meta = metadata(0, ARCHIVE_CONTROL_SESSION_TYPE_ID);
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        let error = ControlSessionsCounter::allocate(&mut client, &counters, ARCHIVE_ID, TIMEOUT)
            .expect_err("the wrong type id");

        assert!(matches!(
            error,
            CounterError::WrongTypeId {
                expected: 102,
                actual: 113
            }
        ));
        assert_eq!("counter has typeId=113, expected=102", error.to_string());
    }

    /// The 102 asked for and taken up on a later turn, which is the whole
    /// reason it is not [`ControlSessionsCounter::allocate`]: that one blocks
    /// on the driver, and a conductor runs *inside* the turn that drives the
    /// driver, so the wait would run out against the driver's own heartbeat.
    #[test]
    fn the_session_count_is_asked_for_and_taken_up_later() {
        let mut client = FakeCounters::default();
        let mut meta = metadata(0, ARCHIVE_CONTROL_SESSIONS_TYPE_ID);
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        let registration_id =
            request_control_sessions_counter(&mut client, ARCHIVE_ID, TIMEOUT).unwrap();

        let (type_id, key, label) = client.only_add().clone();
        assert_eq!(ARCHIVE_CONTROL_SESSIONS_TYPE_ID, type_id);
        assert_eq!(control_sessions_key(ARCHIVE_ID).to_vec(), key);
        assert_eq!(control_sessions_label(ARCHIVE_ID), label);

        assert!(
            claim_control_sessions_counter(&mut client, &counters, registration_id)
                .unwrap()
                .is_none(),
            "the driver has not answered"
        );

        client.answer(registration_id, 0);

        let counter = claim_control_sessions_counter(&mut client, &counters, registration_id)
            .unwrap()
            .expect("the driver has answered by now");

        assert_eq!(0, counter.counter_id());
    }

    #[test]
    fn the_113_key_carries_both_ids_in_order() {
        let mut client = FakeCounters::default();

        ControlSessionCounter::allocate(&mut client, ARCHIVE_ID, 7, "client", TIMEOUT)
            .expect("the command was written");

        let (type_id, key, _) = client.only_add();
        assert_eq!(ARCHIVE_CONTROL_SESSION_TYPE_ID, *type_id);
        assert_eq!(113, ARCHIVE_CONTROL_SESSION_TYPE_ID);
        assert_eq!(CONTROL_SESSION_KEY_LENGTH, key.len());
        assert_eq!(ARCHIVE_ID.to_le_bytes().to_vec(), key[..8].to_vec());
        assert_eq!(7_i64.to_le_bytes().to_vec(), key[8..].to_vec());
    }

    #[test]
    fn the_113_label_says_what_the_client_and_the_reference_are() {
        let mut client = FakeCounters::default();

        ControlSessionCounter::allocate(
            &mut client,
            ARCHIVE_ID,
            7,
            "name=my client version=1.53.2 commit=abc",
            TIMEOUT,
        )
        .expect("the command was written");

        assert_eq!(
            "control-session: name=my client version=1.53.2 commit=abc - archiveId=42",
            client.only_add().2
        );
    }

    /// The reference appends an empty `clientInfo` like any other string, so
    /// the two separators end up adjacent rather than collapsed
    /// (`ControlSessionCounter.java:77`).
    #[test]
    fn an_empty_client_info_leaves_both_spaces_in_the_label() {
        assert_eq!(
            "control-session:  - archiveId=42",
            control_session_label("", 42)
        );
    }

    /// A negative archive id is the one value `putLongAscii` cannot write from
    /// its digits alone (`ArchiveCounters.lengthOfArchiveIdLabel`, `:117-129`),
    /// and `format!` has no such trouble — so the two agree by construction
    /// rather than by a branch here.
    #[test]
    fn a_negative_archive_id_is_spelled_the_way_put_long_ascii_does() {
        assert_eq!(
            "control-session: x - archiveId=-1",
            control_session_label("x", -1)
        );
        assert_eq!(
            "Archive Control Sessions - archiveId=-9223372036854775808",
            control_sessions_label(i64::MIN)
        );
    }

    #[test]
    fn a_session_is_claimed_only_once_the_driver_answers() {
        let mut client = FakeCounters::default();

        let mut counter =
            ControlSessionCounter::allocate(&mut client, ARCHIVE_ID, 7, "", TIMEOUT).unwrap();
        let registration_id = counter.registration_id();

        assert!(
            !counter.claim(&mut client).unwrap(),
            "the driver has not answered"
        );
        assert_eq!(None, counter.counter_id());

        client.answer(registration_id, 3);

        assert!(counter.claim(&mut client).unwrap());
        assert_eq!(Some(3), counter.counter_id());
        assert!(
            counter.claim(&mut client).unwrap(),
            "a claim is not consumed by looking at it"
        );
        assert_eq!(7, counter.control_session_id());
    }

    #[test]
    fn binding_writes_the_publication_as_reference_id_and_the_session_as_value() {
        let mut client = FakeCounters::default();
        let mut counter =
            ControlSessionCounter::allocate(&mut client, ARCHIVE_ID, 7, "", TIMEOUT).unwrap();
        let registration_id = counter.registration_id();
        client.answer(registration_id, 0);
        counter.claim(&mut client).unwrap();

        let mut meta = metadata(0, ARCHIVE_CONTROL_SESSION_TYPE_ID);
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        assert_eq!(Some(()), counter.bind(&counters, 99));

        let described = counters.get(0).expect("the slot the bind named");
        assert_eq!(99, described.reference_id);
        assert_eq!(7, described.value, "the value is the control session id");
    }

    /// Nothing to bind before the driver has answered, and nothing to bind to
    /// if the slot has been reclaimed since — in neither case may the archive
    /// write into a slot that is not this session's counter.
    #[test]
    fn binding_before_the_claim_writes_nothing() {
        let mut client = FakeCounters::default();
        let counter =
            ControlSessionCounter::allocate(&mut client, ARCHIVE_ID, 7, "", TIMEOUT).unwrap();

        let mut meta = metadata(0, ARCHIVE_CONTROL_SESSION_TYPE_ID);
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        assert_eq!(None, counter.bind(&counters, 99));

        // The early return is the whole of it: `CountersReader::value` reads
        // the slot regardless of its state, so "nothing was written" has to be
        // asserted on the bytes rather than on the read answering `None`.
        let described = counters.get(0).expect("the slot is allocated");
        assert_eq!(0, described.reference_id, "nothing was written");
        assert_eq!(0, described.value, "nothing was written");
    }

    #[test]
    fn a_counter_is_given_back_without_waiting_for_the_driver() {
        let mut client = FakeCounters::default();
        let mut counter =
            ControlSessionCounter::allocate(&mut client, ARCHIVE_ID, 7, "", TIMEOUT).unwrap();
        let registration_id = counter.registration_id();

        counter.release(&mut client).expect("the removal was sent");
        assert_eq!(vec![registration_id], client.released);
    }

    #[test]
    fn the_session_count_moves_one_at_a_time() {
        let mut meta = metadata(0, ARCHIVE_CONTROL_SESSIONS_TYPE_ID);
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        let counter = ControlSessionsCounter { counter_id: 0 };

        assert_eq!(Some(1), counter.increment(&counters));
        assert_eq!(Some(2), counter.increment(&counters));
        assert_eq!(Some(2), counter.value(&counters));
        assert_eq!(Some(1), counter.decrement(&counters));
        assert_eq!(Some(1), counter.value(&counters));
    }

    /// The id is the *system* counter's slot, not an archive type id: the
    /// aggregate launcher hands over the driver's `ERRORS`
    /// (`SystemCounterDescriptor.ERRORS.id()`, 15 —
    /// `ArchivingMediaDriver.java:87-89`), where the standalone launcher would
    /// have allocated a 101. This build never allocates one, so the two numbers
    /// never meet.
    #[test]
    fn the_error_counter_is_the_one_it_was_given() {
        let counter = ErrorCounter::new(15);
        assert_ne!(15, ARCHIVE_ERROR_COUNT_TYPE_ID, "a slot is not a type id");

        let mut meta = metadata(15, ARCHIVE_ERROR_COUNT_TYPE_ID);
        let mut vals = values(16);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        assert_eq!(15, counter.counter_id());
        assert_eq!(Some(1), counter.increment(&counters));
        assert_eq!(Some(2), counter.increment(&counters));
    }

    /// A slot outside the region takes no write, which is the one guard the
    /// values region itself can answer — see `ControlSessionsCounter::increment`
    /// for why it is not the reference's `isClosed()`.
    #[test]
    fn a_slot_outside_the_region_takes_no_write() {
        let mut meta = metadata(0, ARCHIVE_CONTROL_SESSIONS_TYPE_ID);
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        let counter = ControlSessionsCounter { counter_id: 1 };
        assert_eq!(None, counter.increment(&counters));
        assert_eq!(None, counter.value(&counters));

        assert_eq!(None, ErrorCounter::new(1).increment(&counters));
    }

    /// A refused add is not "not yet": the counter can never arrive, and a
    /// session that waited would wait out its whole connect timeout.
    #[test]
    fn a_refused_add_is_an_error_rather_than_a_wait() {
        let mut client = FakeCounters {
            refuse: true,
            ..FakeCounters::default()
        };

        let error = ControlSessionCounter::allocate(&mut client, ARCHIVE_ID, 7, "", TIMEOUT)
            .expect_err("the add was refused");

        assert!(matches!(error, CounterError::Command(_)));
    }

    /// ...and the same once the claim is the one asking.
    #[test]
    fn a_claim_that_the_driver_refused_is_an_error() {
        let mut client = FakeCounters::default();
        let mut counter =
            ControlSessionCounter::allocate(&mut client, ARCHIVE_ID, 7, "", TIMEOUT).unwrap();

        // Answered, but with a refusal rather than a slot.
        client.answer(counter.registration_id(), 0);
        client.slots.clear();

        assert!(
            !counter.claim(&mut client).unwrap(),
            "an answer with no counter is not yet a counter"
        );
    }

    /// [`metadata`], with the record's key written too — which is what the
    /// archive-id-keyed counters are found by.
    fn metadata_with_key(counter_id: i32, type_id: i32, key: &[u8]) -> Region {
        let mut region = metadata(counter_id, type_id);
        let base = counter_id as usize * layout::COUNTER_METADATA_LENGTH;

        region.0[base + layout::COUNTER_KEY_OFFSET..base + layout::COUNTER_KEY_OFFSET + key.len()]
            .copy_from_slice(key);

        region
    }

    #[test]
    fn the_recording_session_count_is_keyed_and_labelled_like_the_rest() {
        let mut client = FakeCounters::default();
        let mut meta = metadata(0, ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID);
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        let counter = ArchiveIdCounter::allocate(
            &mut client,
            &counters,
            ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID,
            RECORDING_SESSIONS_NAME,
            ARCHIVE_ID,
            TIMEOUT,
        )
        .expect("the driver answered");

        let (type_id, key, label) = client.only_add();
        assert_eq!(111, *type_id);
        assert_eq!(ARCHIVE_ID.to_le_bytes().to_vec(), *key);
        assert_eq!("Archive Recording Sessions - archiveId=42", label);
        assert_eq!(0, counter.counter_id());
    }

    /// The recorder's three names are what an operator reads in `AeronStat`,
    /// and two of them differ by one word — so they are checked to the byte
    /// (`Archive.java:1601`, `:1612`, `:1623`).
    #[test]
    fn the_recorder_counters_are_named_the_references_way() {
        let mut client = FakeCounters::default();
        let mut meta = metadata(0, ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID);
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        ArchiveIdCounter::allocate(
            &mut client,
            &counters,
            ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID,
            RECORDER_MAX_WRITE_TIME_NAME,
            ARCHIVE_ID,
            TIMEOUT,
        )
        .expect("the driver answered");

        let (type_id, _, label) = client.only_add();
        assert_eq!(105, *type_id);
        assert_eq!(
            "archive-recorder max write time in ns - archiveId=42",
            label
        );

        assert_eq!(
            "archive-recorder total write bytes - archiveId=42",
            archive_id_label(RECORDER_TOTAL_WRITE_BYTES_NAME, ARCHIVE_ID)
        );
        assert_eq!(
            "archive-recorder total write time in ns - archiveId=42",
            archive_id_label(RECORDER_TOTAL_WRITE_TIME_NAME, ARCHIVE_ID)
        );
    }

    /// 111 goes up and down with the recording sessions
    /// (`ArchiveConductor.java:2056`, `:1362`), while the recorder's three are
    /// set outright once per turn that wrote (`:2737-2739`).
    #[test]
    fn an_archive_id_counter_is_moved_or_set_outright() {
        let mut meta = metadata(0, ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID);
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());
        let counter = ArchiveIdCounter { counter_id: 0 };

        assert_eq!(Some(1), counter.increment(&counters));
        assert_eq!(Some(2), counter.increment(&counters));
        assert_eq!(Some(1), counter.decrement(&counters));
        assert_eq!(Some(1), counter.value(&counters));

        assert_eq!(Some(()), counter.set(&counters, 4096));
        assert_eq!(Some(4096), counter.value(&counters));
    }

    /// The one caller of [`find_archive_id_counter`]: the 105 is not allocated
    /// for an archive id that already has one, and the archive refuses to start
    /// rather than make a second (`Archive.java:1586-1595`).
    #[test]
    fn a_counter_is_found_by_its_type_and_the_archive_id_in_its_key() {
        let mut meta = metadata_with_key(
            0,
            ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID,
            &ARCHIVE_ID.to_le_bytes(),
        );
        let mut vals = values(1);
        let counters = CountersReader::new(meta.buffer_mut(), vals.buffer_mut());

        assert_eq!(
            Some(0),
            find_archive_id_counter(
                &counters,
                ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID,
                ARCHIVE_ID
            )
        );
        assert_eq!(
            None,
            find_archive_id_counter(
                &counters,
                ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID,
                ARCHIVE_ID + 1
            ),
            "another archive's counter is not this archive's"
        );
        assert_eq!(
            None,
            find_archive_id_counter(
                &counters,
                ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID,
                ARCHIVE_ID
            ),
            "nor is a counter of another type"
        );
    }

    /// The four new type ids are the reference's, checked where they are
    /// declared so a transposed pair cannot pass.
    #[test]
    fn the_new_type_ids_are_the_references() {
        assert_eq!(100, ARCHIVE_RECORDING_POSITION_TYPE_ID);
        assert_eq!(105, ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID);
        assert_eq!(106, ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID);
        assert_eq!(107, ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID);
        assert_eq!(111, ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID);
    }
}
