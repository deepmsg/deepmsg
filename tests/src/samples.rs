//! The reference's own sample programs, driven as child processes.
//!
//! `BasicPublisher`, `BasicSubscriber` and the rest come out of the reference
//! build (`docs/reference.md`), and a test that runs one is asking a question
//! no test of ours can: does a program that shares no code with this build
//! agree with it about the bytes on the ring? They are looked for rather than
//! assumed — a workspace without the reference checkout skips the tests that
//! need them — and their output goes to a file so that a test can watch what
//! one has received *while* it is still running, which is the only way to ask
//! a subscriber what it has.
//!
//! Their command-line takes `-p` for the aeron directory and `-c`/`-s` for the
//! channel and stream, which is what points a sample at one driver rather than
//! at whichever one happens to be running.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use crate::driver;

/// Where the reference's sample binaries are built to, beside the `aeronmd`
/// and `AeronStat` this crate already looks for.
pub const SAMPLES_DIR: &str = "../../aeron/cppbuild/Release/binaries";

/// Find a reference sample, or `None` when there is no reference build.
pub fn locate(name: &str) -> Option<PathBuf> {
    let env = format!("DEEPMSG_REF_{}", name.to_uppercase());
    let default = format!("{SAMPLES_DIR}/{name}");

    driver::locate_tool(&env, &default)
}

/// A reference sample process, with its output in a file so that the test can
/// watch it *while* it runs — the subscriber runs until it is told to stop, and
/// what it has received so far is the assertion.
pub struct Sample {
    name: String,
    child: Child,
    output: PathBuf,
}

impl Sample {
    pub fn start(binary: &Path, name: &str, dir: &Path, args: &[&str]) -> Self {
        Self::spawn(binary, name, dir, args, Stdio::inherit())
    }

    /// Start a sample with **nothing on stdin**.
    ///
    /// The reference's C++ samples ask `Execute again? (y/n)` when they finish
    /// and read the answer (`aeron-client/src/main/cpp_wrapper/util/StringUtil.h:172-185`),
    /// so a sample started with a terminal waits for a person and one started
    /// with nothing reads EOF, answers "no" and exits — which is what a caller
    /// that runs a sample to completion wants. A test run from a terminal is
    /// the case that makes the difference visible.
    ///
    /// Not the default, because [`Sample::terminate`] is how several tests end
    /// a *running* sample, and a sample that exits on its own as soon as its
    /// first pass is over is not one those tests can ask anything of.
    pub fn start_silent(binary: &Path, name: &str, dir: &Path, args: &[&str]) -> Self {
        Self::spawn(binary, name, dir, args, Stdio::null())
    }

    fn spawn(binary: &Path, name: &str, dir: &Path, args: &[&str], stdin: Stdio) -> Self {
        let output = dir.with_extension(format!("{name}.out"));
        let log = std::fs::File::create(&output).expect("the sample's log file");
        let log_err = log.try_clone().expect("a second handle");

        let child = Command::new(binary)
            // `-p` is the aeron directory for every sample, and it is the only
            // thing that points them at this driver rather than at whichever
            // one happens to be running.
            .arg("-p")
            .arg(dir)
            .args(args)
            .stdin(stdin)
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err))
            .spawn()
            .expect("start the sample");

        Self {
            name: name.to_string(),
            child,
            output,
        }
    }

    /// Everything the sample has written so far.
    pub fn output(&self) -> String {
        std::fs::read_to_string(&self.output).unwrap_or_default()
    }

    /// Wait for the sample to exit on its own.
    pub fn await_exit(&mut self, within: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + within;

        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Ask the sample to stop, and wait for it.
    ///
    /// `Child::kill` is SIGKILL, and a sample that is killed does not exit
    /// cleanly — which is half of what this test asserts. The signal is
    /// **SIGINT**, not the SIGTERM a driver gets: the samples install a handler
    /// for SIGINT only (`aeron-samples/src/main/c/basic_subscriber.c:141`), so
    /// SIGTERM would end the process by its default disposition and the test
    /// would be asserting on the kernel rather than on the sample.
    pub fn terminate(&mut self, within: Duration) -> Option<ExitStatus> {
        let _ = Command::new("kill")
            .arg("-INT")
            .arg(self.child.id().to_string())
            .status();

        let deadline = Instant::now() + within;

        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// The sample's output once `predicate` holds, or a panic naming what was
    /// waited for.
    pub fn await_output(
        &self,
        within: Duration,
        what: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> String {
        let deadline = Instant::now() + within;

        loop {
            let output = self.output();
            if predicate(&output) {
                return output;
            }
            assert!(
                Instant::now() < deadline,
                "{} never showed {what}:\n{output}",
                self.name
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
