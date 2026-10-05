//! Stands in for `java` where the reference's C archive tests were compiled to
//! call it, so that the process they spawn can be one of ours.
//!
//! The reference's archive tests reach the archive through a **compile-time
//! constant**: `JAVA_EXECUTABLE` is injected by CMake
//! (`aeron-archive/src/test/c/CMakeLists.txt:22`) and every `posix_spawn` of an
//! archive uses it (`TestArchive.h:122`, `TestStandaloneArchive.h:144`,
//! `TestMediaDriver.h:66`). Configure a second build directory with
//! `-DJava_JAVA_EXECUTABLE=<this program>` and the whole suite spawns this
//! instead — no reference source is touched.
//!
//! **The suites are the acceptance instrument, not this repository's tests.**
//! They answer "can our archive replace the Java one", and they are worth that
//! only because their cases are not ours. This program exists so that question
//! can be asked; `crates/archive/tests/` is where our own tests live.
//!
//! # The failure this program is built around
//!
//! The obvious rule — "intercept the main classes we know, forward everything
//! else" — fails silently. The reference spawns **three** main classes, not one
//! (measured: 214 × `ArchivingMediaDriver`, 56 × `Archive`, 6 × `MediaDriver`),
//! so a table with two entries forwards a third to the real Java archive, the
//! suite runs, everything is green, and nothing was tested. So:
//!
//! * A main class in the archive or driver namespace that is **not** in the
//!   table is refused rather than forwarded — but **only in the modes where
//!   forwarding would be a lie**. Under `transparent` every invocation is
//!   supposed to reach the real java, so forwarding an unknown class is the
//!   correct answer there, and refusing would break the very run that proves
//!   the shim invisible.
//! * Every invocation is written to the log with the main class it saw, so the
//!   runner can check afterwards that the set of classes seen is a subset of
//!   the table. That check does not depend on the namespace heuristic above,
//!   which is what makes it the real one.
//! * A refusal writes a marker line *and* exits non-zero, because neither alone
//!   is enough: the reference's readiness wait has **no timeout**
//!   (`TestArchive.h:144-152` polls for the mark file), so a dead process is a
//!   hang and not a failure, and the exit status is invisible to the runner
//!   anyway — the process it spawned is the grandchild of a C test binary.
//!
//! # Modes
//!
//! | main class | `transparent` | `hybrid` | `deepmsg` |
//! |---|---|---|---|
//! | `…archive.ArchivingMediaDriver` | forward | our driver + real java `Archive`, supervised | our archiving media driver |
//! | `…archive.Archive` | forward | forward | our archive |
//! | `…driver.MediaDriver` | forward | our driver | our driver |
//!
//! `transparent` is what proves the shim invisible: it must behave as `java`
//! does, and the suite's result under it is the control every other mode is
//! read against. `hybrid` is the entry physical exam for the archive track —
//! our driver carrying the reference's archive. `deepmsg` is the acceptance
//! criterion itself, and its targets do not exist yet.
//!
//! Only `transparent` is executed today. A mode that is configured but not
//! wired **refuses**; it never falls back to forwarding, because a fallback is
//! the silent green this program exists to prevent.

use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

/// Written to the log when an invocation is refused, on its own line so the
/// runner can find it with a plain `grep` rather than by parsing the table.
const REFUSAL_MARKER: &str = "DEEPMSG-SHIM-REFUSED";

/// The namespaces the reference spawns archives and drivers from. A main class
/// here that the table does not know is the mistake this program is built to
/// catch; anywhere else is none of its business.
const WATCHED_NAMESPACES: &[&str] = &["io.aeron.archive.", "io.aeron.driver."];

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();

    let config = match Config::from_env() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("archive-shim: {message}");
            return ExitCode::from(1);
        }
    };

    match run(&args, &config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(failure) => {
            eprintln!("archive-shim: {}", failure.message);
            match failure.refused.as_deref() {
                Some(reason) => config.refuse(&failure.main, reason),
                None => ExitCode::from(1),
            }
        }
    }
}

#[derive(Debug)]
struct Failure {
    message: String,
    /// The main class, when the failure is a refusal about one. Present means
    /// the marker line is owed.
    main: String,
    refused: Option<String>,
}

impl Failure {
    fn plain(message: String) -> Self {
        Self {
            message,
            main: String::new(),
            refused: None,
        }
    }

    fn refused(main: &str, reason: String) -> Self {
        Self {
            message: reason.clone(),
            main: main.to_string(),
            refused: Some(reason),
        }
    }
}

fn run(args: &[OsString], config: &Config) -> Result<(), Failure> {
    let invocation = Invocation::parse(args);
    let main = invocation.main_class.as_deref().unwrap_or("");

    // `java -version` and its siblings carry no main class at all: the suite's
    // CMake configuration runs five of them before anything is built
    // (`CMakeLists.txt:17`), and refusing those would make the second build
    // directory impossible to configure.
    let decision = match main.is_empty() {
        true => Ok(Decision::Forward),
        false => decide(main, config.mode),
    };

    // Every invocation is logged whatever becomes of it. The run's
    // completeness check is over the classes the shim *saw*, not the ones that
    // got through, and a refused one is exactly what it is looking for.
    let described = match &decision {
        Ok(decision) => describe(*decision),
        Err(_) => "refuse".to_string(),
    };
    config.log(main, &described).map_err(Failure::plain)?;

    match decision? {
        Decision::Forward => config.exec_java(args),
        // Wired in the commit that adds the modes. Refusing rather than
        // forwarding is the whole point: a `hybrid` run that quietly spawned
        // the real java archive would report on a system nobody configured.
        Decision::Replace(_) | Decision::Supervise => Err(Failure::refused(
            main,
            format!(
                "mode {} is configured but not wired yet, and forwarding would test the real \
                 java archive by accident; invoke `{}` deliberately instead",
                config.mode.name(),
                main
            ),
        )),
    }
}

/// What the shim will do with an invocation, before anything is executed.
///
/// Kept apart from executing it so the table below can be read and tested on
/// its own: the decision is what must be right, and it is checkable without a
/// reference checkout, a CMake build or a JDK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision {
    /// Hand the whole invocation to the real java, unchanged.
    Forward,
    /// Run one of this repository's binaries where the java process would have
    /// been.
    Replace(Replacement),
    /// Run the real java archive beside one of our drivers and supervise both.
    Supervise,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Replacement {
    Driver,
    Archive,
    ArchivingMediaDriver,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Transparent,
    Hybrid,
    Deepmsg,
}

impl Mode {
    fn parse(text: &str) -> Result<Self, String> {
        match text {
            "transparent" => Ok(Self::Transparent),
            "hybrid" => Ok(Self::Hybrid),
            "deepmsg" => Ok(Self::Deepmsg),
            other => Err(format!(
                "DEEPMSG_SHIM_MODE is {other:?}; it is one of transparent, hybrid, deepmsg"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Transparent => "transparent",
            Self::Hybrid => "hybrid",
            Self::Deepmsg => "deepmsg",
        }
    }
}

/// The three main classes the reference's C tests spawn, with what each mode
/// makes of them.
///
/// Three rather than one is a measurement, not a reading of the plan: a
/// rehearsal that logged every spawn of the whole suite counted 276 of them,
/// and only 214 were `ArchivingMediaDriver`. The other two come from
/// `TestStandaloneArchive` and `TestMediaDriver`, the "standalone driver beside
/// a standalone archive" topology the two largest suites use.
struct Row {
    main_class: &'static str,
    transparent: Decision,
    hybrid: Decision,
    deepmsg: Decision,
}

const TABLE: &[Row] = &[
    Row {
        main_class: "io.aeron.archive.ArchivingMediaDriver",
        transparent: Decision::Forward,
        hybrid: Decision::Supervise,
        deepmsg: Decision::Replace(Replacement::ArchivingMediaDriver),
    },
    Row {
        main_class: "io.aeron.archive.Archive",
        transparent: Decision::Forward,
        hybrid: Decision::Forward,
        deepmsg: Decision::Replace(Replacement::Archive),
    },
    Row {
        main_class: "io.aeron.driver.MediaDriver",
        transparent: Decision::Forward,
        hybrid: Decision::Replace(Replacement::Driver),
        deepmsg: Decision::Replace(Replacement::Driver),
    },
];

fn decide(main_class: &str, mode: Mode) -> Result<Decision, Failure> {
    if !looks_like_a_class_name(main_class) {
        return match mode {
            Mode::Transparent => Ok(Decision::Forward),
            _ => Err(Failure::refused(
                main_class,
                format!(
                    "{main_class} cannot be a class java would run, so the invocation was not \
                     understood; refusing rather than forwarding something nobody asked for"
                ),
            )),
        };
    }

    let known = TABLE.iter().find(|row| row.main_class == main_class);
    let decision = match known {
        Some(row) => match mode {
            Mode::Transparent => row.transparent,
            Mode::Hybrid => row.hybrid,
            Mode::Deepmsg => row.deepmsg,
        },
        // Under `transparent` this is the right answer: that mode's whole
        // definition is that everything reaches the real java. The run's
        // completeness check is what catches a table that has fallen behind.
        None if mode == Mode::Transparent => Decision::Forward,
        None if WATCHED_NAMESPACES
            .iter()
            .any(|ns| main_class.starts_with(ns)) =>
        {
            return Err(Failure::refused(
                main_class,
                format!(
                    "{} is in a namespace this suite spawns archives and drivers from, and the \
                     table does not know it; forwarding it would run the real java one and report \
                     on a system nobody configured",
                    main_class
                ),
            ));
        }
        None => Decision::Forward,
    };
    Ok(decision)
}

/// One `java` invocation, taken apart.
struct Invocation {
    /// The class java would run, or `None` for an invocation that only carries
    /// java's own options — `-version` above all, which the suite's CMake
    /// configuration runs before it will accept the shim as a JVM.
    main_class: Option<String>,
}

impl Invocation {
    /// The first argument that is neither an option nor an option's value,
    /// which is java's own rule.
    ///
    /// **The list of value-taking options below is not java's grammar.** It is
    /// what this suite's spawn sites build by hand (`TestArchive.h:80-107`,
    /// `TestStandaloneArchive.h:110-142`, `TestMediaDriver.h:40-66`) plus the
    /// module options a JDK 17+ invocation cannot avoid — and it was one entry
    /// short until a test with the rehearsal's real argv read
    /// `--add-opens`'s *value* as the class. Rather than carry the whole
    /// grammar, an argument that cannot be a class name is not treated as one:
    /// see `looks_like_a_class_name`.
    ///
    /// Scanning for the last argument without a leading `-` would agree with
    /// this on every invocation the suite makes today — the capture ends at the
    /// main class — and would be wrong the first time a test passed a program
    /// argument.
    fn parse(args: &[OsString]) -> Self {
        let mut rest = args.iter();
        while let Some(arg) = rest.next() {
            let text = arg.to_string_lossy();
            if text.starts_with('-') {
                if VALUE_OPTIONS.contains(&text.as_ref()) {
                    // Its value is an argument, not the class.
                    rest.next();
                }
                continue;
            }
            return Self {
                main_class: Some(text.into_owned()),
            };
        }
        Self { main_class: None }
    }
}

/// Options that consume the argument after them.
const VALUE_OPTIONS: &[&str] = &[
    "-cp",
    "-classpath",
    "--class-path",
    "-p",
    "--module-path",
    "-m",
    "--module",
    "-jar",
    "--add-opens",
    "--add-exports",
    "--add-reads",
    "--add-modules",
    "--patch-module",
    "--limit-modules",
    "--upgrade-module-path",
];

/// Whether java could run this.
///
/// An option the list above does not know hands its value over as if it were
/// the class, and the shape of such a value is unmistakable —
/// `java.base/jdk.internal.misc=ALL-UNNAMED` for `--add-opens`. Checking the
/// shape is how the heuristic fails *loudly*: a name that cannot be a class is
/// never forwarded where forwarding would be a lie.
fn looks_like_a_class_name(text: &str) -> bool {
    !text.is_empty()
        && text.contains('.')
        && text.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '$'))
}

/// Everything the shim is told, all of it through the environment: the shim is
/// baked into the reference's build as a compile-time constant, so it cannot
/// read anything out of this repository.
struct Config {
    mode: Mode,
    /// The java it replaced. `Java_JAVA_EXECUTABLE` is overwritten with the
    /// shim's own path, so after that nobody else remembers where java is.
    java: PathBuf,
    log: PathBuf,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let mode = match std::env::var("DEEPMSG_SHIM_MODE") {
            Ok(text) => Mode::parse(&text)?,
            Err(_) => Mode::Transparent,
        };

        let java = required_path("DEEPMSG_SHIM_JAVA")?;
        let log = match std::env::var_os("DEEPMSG_SHIM_LOG") {
            Some(path) => PathBuf::from(path),
            None => std::env::temp_dir().join("deepmsg-archive-shim.log"),
        };

        Ok(Self { mode, java, log })
    }

    /// `exec`, not spawn-and-wait: the reference signals the pid it spawned and
    /// waits for it to exit (`TestArchive.h:165-171`), so replacing the process
    /// image — same pid — is what makes the teardown work unchanged.
    fn exec_java(&self, args: &[OsString]) -> Result<(), Failure> {
        use std::os::unix::process::CommandExt;

        let error = Command::new(&self.java).args(args).exec();
        Err(Failure::plain(format!(
            "could not exec {}: {error}",
            self.java.display()
        )))
    }

    fn log(&self, main_class: &str, decision: &str) -> Result<(), String> {
        let line = format!(
            "{}\tpid={}\tmode={}\tmain={}\tdecision={decision}\n",
            epoch_millis(),
            std::process::id(),
            self.mode.name(),
            main_class,
        );
        self.append(&line)
    }

    /// The marker *and* the exit status. The runner greps for the first; the
    /// second is for whoever runs one binary by hand.
    fn refuse(&self, main_class: &str, reason: &str) -> ExitCode {
        let line = format!("{REFUSAL_MARKER}\tmain={main_class}\t{reason}\n");
        if let Err(e) = self.append(&line) {
            eprintln!("archive-shim: the refusal could not be logged: {e}");
        }
        ExitCode::from(1)
    }

    fn append(&self, line: &str) -> Result<(), String> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .map_err(|e| format!("{}: {e}", self.log.display()))?;
        file.write_all(line.as_bytes())
            .map_err(|e| format!("{}: {e}", self.log.display()))
    }
}

fn describe(decision: Decision) -> String {
    match decision {
        Decision::Forward => "forward".to_string(),
        Decision::Supervise => "supervise".to_string(),
        Decision::Replace(Replacement::Driver) => "replace:driver".to_string(),
        Decision::Replace(Replacement::Archive) => "replace:archive".to_string(),
        Decision::Replace(Replacement::ArchivingMediaDriver) => {
            "replace:archiving-media-driver".to_string()
        }
    }
}

fn required_path(variable: &str) -> Result<PathBuf, String> {
    let value = std::env::var_os(variable)
        .ok_or_else(|| format!("${variable} is not set; the shim cannot run java without it"))?;
    let path = PathBuf::from(value);
    if !is_executable(&path) {
        return Err(format!(
            "${variable} is {}, which is not an executable file",
            path.display()
        ));
    }
    Ok(path)
}

fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    path.metadata()
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

fn epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The suite's replay of what the shim was handed, from the rehearsal's
    /// capture of `TestArchive`'s spawn: options, then `-cp <jar>`, then the
    /// class, and nothing after it.
    fn captured_argv() -> Vec<OsString> {
        [
            "--add-opens",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED",
            "-Daeron.archive.id=42",
            "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:8010",
            "-Daeron.dir=/dev/shm/aeron-gavin",
            "-cp",
            "/home/gavin/trader/aeron/aeron-all/build/libs/aeron-all-1.53.2.jar",
            "io.aeron.archive.ArchivingMediaDriver",
        ]
        .iter()
        .map(OsString::from)
        .collect()
    }

    #[test]
    fn the_main_class_is_the_last_argument_and_the_jar_is_not_it() {
        let invocation = Invocation::parse(&captured_argv());
        assert_eq!(
            Some("io.aeron.archive.ArchivingMediaDriver".to_string()),
            invocation.main_class
        );
    }

    #[test]
    fn a_program_argument_after_the_main_class_is_not_the_main_class() {
        let mut args = captured_argv();
        args.push(OsString::from("and-an-argument"));

        // The rehearsal never saw one, which is exactly why scanning from the
        // end would have looked right and been wrong.
        assert_eq!(
            Some("io.aeron.archive.ArchivingMediaDriver".to_string()),
            Invocation::parse(&args).main_class
        );
    }

    /// `find_package(Java 17 REQUIRED)` runs five of these before it will
    /// accept the shim as a JVM, so there are no main classes here by design.
    #[test]
    fn javas_own_options_carry_no_main_class() {
        for args in [
            vec!["-version"],
            vec!["-XshowSettings:properties", "-version"],
        ] {
            let args: Vec<OsString> = args.iter().map(OsString::from).collect();
            assert_eq!(None, Invocation::parse(&args).main_class);
        }
    }

    #[test]
    fn transparent_forwards_every_class_the_table_knows() {
        for row in TABLE {
            assert_eq!(
                Decision::Forward,
                decide(row.main_class, Mode::Transparent).unwrap(),
                "{}",
                row.main_class
            );
        }
    }

    /// The namespace check is a heuristic and this is where it would mislead:
    /// under `transparent` an unknown class is *supposed* to reach the real
    /// java. What catches a stale table there is the run's completeness check
    /// over the log, not this function.
    #[test]
    fn transparent_forwards_a_class_the_table_has_never_heard_of() {
        assert_eq!(
            Decision::Forward,
            decide(
                "io.aeron.archive.SomethingNobodyRegistered",
                Mode::Transparent
            )
            .unwrap()
        );
    }

    /// The one that stops a green run from meaning nothing.
    #[test]
    fn a_watched_class_that_is_not_in_the_table_is_refused_outside_transparent() {
        for mode in [Mode::Hybrid, Mode::Deepmsg] {
            for main_class in [
                "io.aeron.archive.SomethingNobodyRegistered",
                "io.aeron.driver.SomethingNew",
            ] {
                let failure = decide(main_class, mode).expect_err(main_class);
                assert!(
                    failure.refused.is_some(),
                    "{main_class} must carry the marker"
                );
                assert_eq!(main_class, failure.main);
            }
        }
    }

    /// Refusing everything unknown would be its own kind of wrong: the suite
    /// spawns things that are none of this program's business, and a table is
    /// not a whitelist for the whole reference tree.
    #[test]
    fn a_stranger_outside_the_watched_namespaces_is_forwarded() {
        for mode in [Mode::Transparent, Mode::Hybrid, Mode::Deepmsg] {
            assert_eq!(
                Decision::Forward,
                decide("io.aeron.samples.SomethingElse", mode).unwrap()
            );
        }
    }

    #[test]
    fn each_mode_makes_its_own_reading_of_the_table() {
        assert_eq!(
            Decision::Supervise,
            decide(TABLE[0].main_class, Mode::Hybrid).unwrap()
        );
        assert_eq!(
            Decision::Forward,
            decide(TABLE[0].main_class, Mode::Transparent).unwrap()
        );
        assert_eq!(
            Decision::Replace(Replacement::Driver),
            decide(TABLE[2].main_class, Mode::Deepmsg).unwrap()
        );
        // The standalone archive is the reference's even where ours is the
        // server: `hybrid` is about the driver, and swapping both halves at
        // once would leave nothing to compare against.
        assert_eq!(
            Decision::Forward,
            decide(TABLE[1].main_class, Mode::Hybrid).unwrap()
        );
    }

    /// What an option the list does not know looks like: its value arrives
    /// where the class should be. Refusing is the point — forwarding there is
    /// the silent green this program exists to prevent.
    #[test]
    fn something_that_cannot_be_a_class_is_refused_outside_transparent() {
        let not_a_class = "java.base/jdk.internal.misc=ALL-UNNAMED";
        assert!(!looks_like_a_class_name(not_a_class));

        assert_eq!(
            Decision::Forward,
            decide(not_a_class, Mode::Transparent).unwrap()
        );
        for mode in [Mode::Hybrid, Mode::Deepmsg] {
            let failure = decide(not_a_class, mode).expect_err("must refuse");
            assert!(failure.refused.is_some());
        }
    }

    #[test]
    fn the_three_real_class_names_look_like_class_names() {
        for row in TABLE {
            assert!(
                looks_like_a_class_name(row.main_class),
                "{}",
                row.main_class
            );
        }
    }

    #[test]
    fn a_mode_that_is_not_one_is_refused_by_name() {
        let message = Mode::parse("dedicated").unwrap_err();
        assert!(message.contains("dedicated"), "{message}");
        assert_eq!(Mode::Transparent, Mode::parse("transparent").unwrap());
    }
}
