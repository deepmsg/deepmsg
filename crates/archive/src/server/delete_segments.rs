//! Deleting an archive's own segment files.
//!
//! This is the one place in the archive that **removes** what it wrote. Every
//! other write — a recording's frames, a catalog row, the mark file — is an
//! append or an overwrite in place; a file that goes away takes with it the only
//! copy of what it held, and the reference's care about it shows in the shape of
//! the thing that does it: a **session** that runs over several turns rather
//! than a loop that runs inside one.
//!
//! Three reasons, and all three are load-bearing
//! (`DeleteSegmentsSession.java:27-177`):
//!
//! * **A replay may still be reading the files.** The session has an
//!   `AWAIT_REPLAYS_STOP` state for exactly that, and it is entered by the
//!   conductor passing `awaitReplaysStop` — which a truncate does and a purge
//!   does not (`ArchiveConductor.java:1245` against `:1262`).
//! * **A delete can fail.** A file that will not go is renamed to
//!   `<name>.del` and tried again, and a file that is *gone* is not a failure.
//!   That rename is why the listing that follows a failed delete still finds it.
//! * **The client is told when it is over**, not when it is asked: the DELETE
//!   recording signal is sent from the session's `close()`
//!   (`DeleteSegmentsSession.java:76-80`), several turns after the OK that
//!   answered the request.
//!
//! The session also carries `maxDeletePosition`, and it is not decoration: a
//! replay that would read past a delete still in flight is **refused** while it
//! is in flight (`ArchiveConductor.java:880-886`), which is a question the
//! conductor asks of this session.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use crate::segment::{SUFFIX, segment_file_base_position, segment_file_name};

/// `ArchiveConductor.DELETE_SUFFIX`: what a file that would not delete is
/// renamed to before being tried again.
pub const DELETE_SUFFIX: &str = ".del";

/// What one turn of a delete session did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    /// Waiting for the replays of this recording to stop
    /// (`DeleteSegmentsSession.java:130-138`).
    AwaitingReplays,
    /// The state moved from waiting to deleting; nothing was deleted yet, and
    /// the reference spends a turn on that move too (`:118-123`).
    StoppedWaiting,
    /// One file went.
    Deleted,
    /// There is nothing left to delete: the session is over.
    Finished,
    /// A file is there and will not go (`:157-161`).
    Failed(String),
}

/// `DeleteSegmentsSession` (`DeleteSegmentsSession.java:27-177`).
///
/// The reference holds a `ControlSession` and sends the DELETE signal itself on
/// the way out. This one does not: the control sessions live with the conductor
/// in this build, so what the session answers with is [`Progress::Finished`] and
/// the conductor is what sends the signal — the same split every other session
/// here has (`create_replay_publication.rs`, `replay_session.rs`).
#[derive(Debug)]
pub struct DeleteSegmentsSession {
    /// The recording whose files these are.
    recording_id: i64,
    /// The request being answered, which the signal echoes.
    correlation_id: i64,
    /// The highest segment base position the files name
    /// (`DeleteSegmentsSession.java:53-64`).
    ///
    /// It is what a later `startReplay` compares against (`:880-886`) to refuse
    /// a replay that would read past a delete that has not finished.
    max_delete_position: i64,
    files: VecDeque<PathBuf>,
    waiting_for_replays: bool,
}

impl DeleteSegmentsSession {
    /// A session over `files`, which the caller has already chosen.
    ///
    /// `await_replays_stop` is the conductor's decision, not this one's: a
    /// truncate passes `true` because it has just moved the recording's stop
    /// (so a replay may be mid-flight over bytes about to go) and every other
    /// caller passes `false`.
    #[must_use]
    pub fn new(
        recording_id: i64,
        correlation_id: i64,
        files: Vec<PathBuf>,
        await_replays_stop: bool,
    ) -> Self {
        Self {
            recording_id,
            correlation_id,
            max_delete_position: max_segment_position(&files, recording_id).unwrap_or(i64::MIN),
            files: files.into(),
            waiting_for_replays: await_replays_stop,
        }
    }

    /// The recording whose files are going.
    #[must_use]
    pub const fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// The request the DELETE signal will echo.
    #[must_use]
    pub const fn correlation_id(&self) -> i64 {
        self.correlation_id
    }

    /// The highest segment base position in the list.
    ///
    /// `Long.MIN_VALUE` for an empty list, which the reference would give a
    /// `NullPointerException`-free `Math.max` over nothing — and an empty list
    /// never reaches a session anyway (`ArchiveConductor.java:1749-1752`).
    #[must_use]
    pub const fn max_delete_position(&self) -> i64 {
        self.max_delete_position
    }

    /// Whether there is nothing left to delete.
    ///
    /// **This is the reference's own `isDone`** (`DeleteSegmentsSession.java:98-101`)
    /// — the list, not a flag beside it. A flag would have to be set in the one
    /// place the list is emptied, and the two could disagree.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.files.is_empty()
    }

    /// One turn (`DeleteSegmentsSession.java:112-124`).
    ///
    /// `replays_in_progress` is the conductor's answer for **this** recording,
    /// and it is a parameter rather than a field because the conductor is what
    /// can see the replay sessions.
    pub fn do_work(&mut self, replays_in_progress: bool) -> Progress {
        if self.waiting_for_replays {
            if replays_in_progress {
                return Progress::AwaitingReplays;
            }

            self.waiting_for_replays = false;

            // The reference's switch has already chosen its arm for this turn,
            // so the move costs one: `doAwaitReplaysStop` returns without
            // deleting anything (`:130-138`).
            return Progress::StoppedWaiting;
        }

        let Some(path) = self.files.pop_front() else {
            return Progress::Finished;
        };

        if let Err(error) = delete_segment_file(&path) {
            return Progress::Failed(error);
        }

        // The reference reports the turn as a delete whether or not that was the
        // last file: the worker notices `isDone` on its own (`:118-124`).
        Progress::Deleted
    }
}

/// Remove one segment file, with the reference's `.del` fallback
/// (`DeleteSegmentsSession.java:144-168`).
///
/// A file that will not delete and **is still there** is an error. A file that
/// will not delete and is **not** there is not one — the delete succeeded
/// somewhere else, or the file was never there. And a file still there under
/// another name is renamed to `<name>.del` and that is what is deleted, which is
/// what keeps a failed delete visible to the next listing rather than silent.
fn delete_segment_file(path: &Path) -> Result<(), String> {
    if std::fs::remove_file(path).is_ok() {
        return Ok(());
    }

    if path.exists() {
        return Err(format!("unable to delete segment file: {}", path.display()));
    }

    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(DELETE_SUFFIX))
    {
        // Already renamed once, and gone: nothing left to do.
        return Ok(());
    }

    let renamed = rename_target(path);

    if std::fs::remove_file(&renamed).is_err() && renamed.exists() {
        return Err(format!(
            "unable to delete segment file: {}",
            renamed.display()
        ));
    }

    Ok(())
}

/// `<name>.del`, which is what a file that will not delete is renamed to.
fn rename_target(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();

    name.push(DELETE_SUFFIX);

    PathBuf::from(name)
}

/// Every segment file this archive holds for `recording_id`
/// (`ArchiveConductor.listSegmentFiles`, `:1974-1989`).
///
/// Both suffixes count: a file that would not delete is a `.del` and is still
/// this archive's to remove. Nothing else in the directory is looked at — the
/// prefix is the recording id and a dash, so `1-` does not match `10-`.
#[must_use]
pub fn list_segment_files(directory: &Path, recording_id: i64) -> Vec<PathBuf> {
    let prefix = format!("{recording_id}-");
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };

    let mut files: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_name().to_str().is_some_and(|name| {
                name.starts_with(&prefix)
                    && (name.ends_with(SUFFIX) || name.ends_with(DELETE_SUFFIX))
            })
        })
        .map(|entry| entry.path())
        .collect();

    // The reference takes them in `File.list()`'s order, which is the file
    // system's; sorting makes a delete's order the same on every run, and the
    // only thing that depends on the order is which file goes first.
    files.sort();

    files
}

/// The segment files a recording has **stopped claiming**
/// (`ArchiveConductor.findDetachedSegments`, `:1715-1738`).
///
/// A file is detached when its base position is below the recording's start:
/// the file is still in the directory and the recording no longer begins there.
/// The walk is downwards from the segment *before* the current start, and it
/// stops at `previous_start_position`'s segment — which is where the caller says
/// the claiming used to begin.
#[must_use]
pub fn find_detached_segments(
    directory: &Path,
    recording_id: i64,
    start_position: i64,
    previous_start_position: i64,
    term_buffer_length: i32,
    segment_file_length: i32,
) -> Vec<PathBuf> {
    let previous_base = segment_file_base_position(
        previous_start_position,
        previous_start_position,
        term_buffer_length,
        segment_file_length,
    );
    let start_base = segment_file_base_position(
        start_position,
        start_position,
        term_buffer_length,
        segment_file_length,
    );

    let mut files = Vec::new();
    let mut base = start_base - i64::from(segment_file_length);

    while base >= previous_base {
        files.push(directory.join(segment_file_name(recording_id, base)));
        base -= i64::from(segment_file_length);
    }

    files
}

/// The `{recordingId}-{base}` prefix of a segment file's name, parsed back
/// (`DeleteSegmentsSession.java:53-64`).
///
/// The reference takes `digitCount(recordingId) + 1` characters off the front
/// and reads up to the first `.` — so `12-4096.rec` and `12-4096.rec.del` both
/// answer `4096`, which is what lets a renamed file still be found by the
/// listing.
fn segment_position_of(path: &Path) -> Option<i64> {
    let name = path.file_name()?.to_str()?;
    let dot = name.find('.')?;
    let dash = name.find('-')?;

    if dash >= dot {
        return None;
    }

    name[dash + 1..dot].parse().ok()
}

/// The highest base position in a list of file names, or `None` for an empty
/// one.
fn max_segment_position(files: &[PathBuf], recording_id: i64) -> Option<i64> {
    let prefix = format!("{recording_id}-");

    files
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix))
        })
        .filter_map(|path| segment_position_of(path))
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::mark::tests::TempDir;

    const RECORDING_ID: i64 = 7;
    const TERM_LENGTH: i32 = 64 * 1024;
    const SEGMENT_LENGTH: i32 = 128 * 1024;

    /// A directory with the named segment files in it, each one byte.
    fn with_files(names: &[&str]) -> TempDir {
        let dir = TempDir::new();

        for name in names {
            std::fs::write(dir.path().join(name), b"x").expect("the file");
        }

        dir
    }

    fn files(dir: &TempDir, names: &[&str]) -> Vec<PathBuf> {
        names.iter().map(|name| dir.path().join(name)).collect()
    }

    /// The session takes the highest base position off the names, which is what
    /// a later replay is refused by (`:53-64`, `:880-886`).
    #[test]
    fn the_highest_position_is_read_out_of_the_file_names() {
        let dir = with_files(&["7-0.rec", "7-131072.rec", "7-262144.rec.del"]);
        let session = DeleteSegmentsSession::new(
            RECORDING_ID,
            99,
            files(&dir, &["7-0.rec", "7-131072.rec", "7-262144.rec.del"]),
            false,
        );

        assert_eq!(262_144, session.max_delete_position());
        assert_eq!(RECORDING_ID, session.recording_id());
        assert_eq!(99, session.correlation_id());
    }

    /// One file a turn, and the session is over when the list is.
    #[test]
    fn one_file_goes_a_turn() {
        let dir = with_files(&["7-0.rec", "7-131072.rec"]);
        let mut session = DeleteSegmentsSession::new(
            RECORDING_ID,
            99,
            files(&dir, &["7-0.rec", "7-131072.rec"]),
            false,
        );

        assert_eq!(Progress::Deleted, session.do_work(false));
        assert!(!dir.path().join("7-0.rec").exists(), "the first is gone");
        assert!(
            dir.path().join("7-131072.rec").exists(),
            "and the second is not: one a turn, as the reference does it"
        );

        assert_eq!(Progress::Deleted, session.do_work(false));
        assert_eq!(Progress::Finished, session.do_work(false));
        assert!(session.is_done());
    }

    /// `awaitReplaysStop` costs a turn and does not delete on it
    /// (`:130-138`).
    #[test]
    fn waiting_for_replays_comes_first() {
        let dir = with_files(&["7-0.rec"]);
        let mut session =
            DeleteSegmentsSession::new(RECORDING_ID, 99, files(&dir, &["7-0.rec"]), true);

        assert_eq!(Progress::AwaitingReplays, session.do_work(true));
        assert!(dir.path().join("7-0.rec").exists(), "nothing went");

        assert_eq!(Progress::StoppedWaiting, session.do_work(false));
        assert!(
            dir.path().join("7-0.rec").exists(),
            "the move costs its own turn, so this one deleted nothing either"
        );

        assert_eq!(Progress::Deleted, session.do_work(false));
        assert!(!dir.path().join("7-0.rec").exists());
    }

    /// A file that is already gone is not a failure, and one that will not go is
    /// renamed and tried again (`:144-168`).
    #[test]
    fn a_file_that_is_gone_is_not_an_error() {
        let dir = with_files(&["7-0.rec"]);
        let path = dir.path().join("7-0.rec");
        std::fs::remove_file(&path).expect("removed behind the session's back");

        assert_eq!(Ok(()), delete_segment_file(&path));
        assert_eq!(Ok(()), delete_segment_file(&dir.path().join("7-0.rec.del")));
    }

    /// The listing is this recording's files and only this recording's, under
    /// both suffixes (`:1974-1989`).
    #[test]
    fn the_listing_is_this_recordings_files() {
        let dir = with_files(&[
            "7-0.rec",
            "7-131072.rec.del",
            "70-0.rec",
            "7-0.txt",
            "catalog.dat",
        ]);

        let listed: Vec<String> = list_segment_files(dir.path(), RECORDING_ID)
            .iter()
            .map(|path| {
                path.file_name()
                    .expect("a name")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();

        assert_eq!(vec!["7-0.rec", "7-131072.rec.del"], listed);
    }

    /// The detached files are the ones below the recording's start, down to
    /// where the claiming used to begin (`:1715-1738`).
    #[test]
    fn the_detached_files_are_the_ones_below_the_start() {
        let dir = with_files(&["7-0.rec", "7-131072.rec", "7-262144.rec", "7-393216.rec"]);

        // A recording that began at 0 and now begins at 262 144 has two files
        // it no longer claims.
        let detached: Vec<String> = find_detached_segments(
            dir.path(),
            RECORDING_ID,
            262_144,
            0,
            TERM_LENGTH,
            SEGMENT_LENGTH,
        )
        .iter()
        .map(|path| {
            path.file_name()
                .expect("a name")
                .to_string_lossy()
                .into_owned()
        })
        .collect();

        assert_eq!(vec!["7-131072.rec", "7-0.rec"], detached);
    }
}
