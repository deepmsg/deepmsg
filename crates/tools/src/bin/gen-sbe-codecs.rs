//! Regenerate the SBE codecs from `schemas/` — the `just gen` step of ADR-0004.
//!
//! The generator is the reference's own `sbe-tool`, run with its Rust target,
//! because that is the tool the reference's Java codecs come from and the whole
//! point of the golden fixtures is that both sides of a byte comparison were
//! produced by the same program from the same schema.
//!
//! Four things this does that a bare `java -cp … SbeTool …` does not, each for
//! a reason found by running it:
//!
//! * **Pins the versions.** `sbe-tool` and `agrona` are resolved by name and
//!   version rather than taken from whatever is on a classpath, because a
//!   different generator version is a different answer to the same schema, and
//!   the golden tests would report that as our defect.
//! * **Fails on `[Error]`.** The XSD validation reports a malformed schema and
//!   then exits zero, with the codecs still emitted. Left alone it is a log
//!   line; the schema it complains about produces codecs that quietly disagree
//!   with the wire.
//! * **Checks which packages came out.** The set of generated crates is
//!   asserted rather than assumed, so a schema renamed upstream cannot leave a
//!   stale crate behind and a puzzle in the diff.
//! * **Formats the result.** The Rust target's output is not `rustfmt`-clean,
//!   and the workspace's first gate is `cargo fmt --check`. Formatting here is
//!   what makes a regeneration cheap to review.
//!
//! It writes `src/` and nothing else: each crate's `Cargo.toml` is ours and
//! follows the workspace conventions, which the generator's would not.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

/// The generator and the runtime it needs on its classpath. Both match the
/// reference's `gradle/libs.versions.toml`; a mismatch is refused rather than
/// tolerated.
const SBE_TOOL_VERSION: &str = "1.40.2";
const AGRONA_VERSION: &str = "2.6.1";

/// Each schema, and the crate the generator derives from its `package`
/// attribute — dots to underscores, which is what `sbe-tool` does on its way
/// to a directory name.
const SCHEMAS: &[(&str, &str)] = &[
    ("aeron-archive-codecs.xml", "io_aeron_archive_codecs"),
    (
        "aeron-archive-mark-codecs.xml",
        "io_aeron_archive_codecs_mark",
    ),
    ("aeron-cluster-codecs.xml", "io_aeron_cluster_codecs"),
    (
        "aeron-cluster-mark-codecs.xml",
        "io_aeron_cluster_codecs_mark",
    ),
    (
        "aeron-cluster-node-state-codecs.xml",
        "io_aeron_cluster_codecs_node",
    ),
];

/// The XSD the schemas are validated against, relative to the repository root.
const VALIDATION_XSD: &str = "schemas/fpl/sbe.xsd";

const USAGE: &str = "\
usage: gen-sbe-codecs [OPTIONS]

Regenerate the SBE codecs from schemas/ and place them in their crates.

  --dest DIR       where the crates live (default: crates)
  -h, --help       show this message

The generator (`sbe-tool` 1.40.2) and its runtime (`agrona` 2.6.1) are found
by looking at $SBE_TOOL_JAR and $AGRONA_JAR first, then in the Gradle module
cache ($GRADLE_USER_HOME, else ~/.gradle).

Exits 0 on success, 1 if generation or placement failed, 2 on a usage error.";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let options = match Options::parse(&args) {
        Ok(Some(options)) => options,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("gen-sbe-codecs: {message}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    match run(&options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("gen-sbe-codecs: {message}");
            ExitCode::from(1)
        }
    }
}

fn run(options: &Options) -> Result<(), String> {
    let root = repository_root()?;
    let schemas_dir = root.join("schemas");
    let xsd = root.join(VALIDATION_XSD);
    if !xsd.is_file() {
        return Err(format!("{} is missing", xsd.display()));
    }

    let tool = locate_jar(
        "SBE_TOOL_JAR",
        "uk.co.real-logic",
        "sbe-tool",
        SBE_TOOL_VERSION,
    )?;
    let agrona = locate_jar("AGRONA_JAR", "org.agrona", "agrona", AGRONA_VERSION)?;
    println!("sbe-tool   {}", tool.display());
    println!("agrona     {}", agrona.display());

    // Staged under `target/`, which is ignored and on the same filesystem as
    // the crates — so the final move is a rename rather than a copy.
    let stage = root.join("target").join("sbe-codecs");
    if stage.exists() {
        fs::remove_dir_all(&stage).map_err(|e| format!("{}: {e}", stage.display()))?;
    }
    fs::create_dir_all(&stage).map_err(|e| format!("{}: {e}", stage.display()))?;

    let mut sources = Vec::new();
    for (schema, _) in SCHEMAS {
        let path = schemas_dir.join(schema);
        if !path.is_file() {
            return Err(format!("{} is missing", path.display()));
        }
        sources.push(path.as_os_str().to_os_string());
    }

    let output = Command::new("java")
        .arg("-cp")
        .arg(format!("{}:{}", tool.display(), agrona.display()))
        .arg(format!("-Dsbe.output.dir={}", stage.display()))
        .arg("-Dsbe.target.language=Rust")
        .arg(format!("-Dsbe.validation.xsd={}", xsd.display()))
        .arg("-Dsbe.validation.stop.on.error=true")
        .arg("uk.co.real_logic.sbe.SbeTool")
        .args(&sources)
        .output()
        .map_err(|e| format!("could not run java: {e}"))?;

    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    print!("{transcript}");

    if !output.status.success() {
        return Err(format!("the generator exited {}", output.status));
    }
    if !transcript_is_clean(&transcript) {
        return Err(
            "the generator reported [Error]; the schemas are malformed. The codecs it went on \
             to emit are not trustworthy."
                .to_string(),
        );
    }

    let produced = generated_packages(&stage)?;
    let expected: Vec<String> = SCHEMAS.iter().map(|(_, p)| (*p).to_string()).collect();
    if produced != expected {
        return Err(format!(
            "expected the generator to produce {expected:?}, it produced {produced:?}"
        ));
    }
    println!("generated  {} crate(s)", produced.len());

    for package in &produced {
        stage_manifest(&stage.join(package), package)?;
        format_crate(&stage.join(package))?;
    }

    // Every destination is checked before any of them is touched. Dying on the
    // third crate with the first two already replaced is a half-applied
    // regeneration, and the diff it leaves is harder to read than a refusal.
    for package in &produced {
        let manifest = options.dest.join(package).join("Cargo.toml");
        if !manifest.is_file() {
            return Err(format!(
                "{} does not exist — a crate is only ever given a generated `src/`, its \
                 manifest is ours (ADR-0004)",
                manifest.display()
            ));
        }
    }

    for package in &produced {
        let src = options.dest.join(package).join("src");
        if src.exists() {
            fs::remove_dir_all(&src).map_err(|e| format!("{}: {e}", src.display()))?;
        }
        fs::rename(stage.join(package).join("src"), &src)
            .map_err(|e| format!("{}: {e}", src.display()))?;
        println!("placed     {}", src.display());
    }

    fs::remove_dir_all(&stage).map_err(|e| format!("{}: {e}", stage.display()))?;
    Ok(())
}

/// The generator reports a schema the XSD rejects and then carries on: it
/// exits zero and emits codecs anyway, so the message is the only signal there
/// is. Matched on the prefix the tool uses for every validation diagnostic.
fn transcript_is_clean(transcript: &str) -> bool {
    !transcript.contains("[Error]")
}

/// The crate directories the generator finished, sorted.
///
/// Two different accidents, told apart. A directory holding a manifest that no
/// schema names means upstream has renamed something, and the caller compares
/// this list against [`SCHEMAS`] to find the ones that went missing — which is
/// how a stale crate fails loudly instead of being left in the tree.
fn generated_packages(stage: &Path) -> Result<Vec<String>, String> {
    let known: Vec<&str> = SCHEMAS.iter().map(|(_, package)| *package).collect();
    let mut produced = Vec::new();
    let mut unexpected = Vec::new();

    let entries = fs::read_dir(stage).map_err(|e| format!("{}: {e}", stage.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("{}: {e}", stage.display()))?;
        if !entry.path().is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if !known.contains(&name.as_str()) {
            unexpected.push(name);
        } else if stage.join(&name).join("Cargo.toml").is_file() {
            produced.push(name);
        }
    }

    if !unexpected.is_empty() {
        unexpected.sort();
        return Err(format!(
            "the generator produced {unexpected:?}, which no schema in schemas/ names"
        ));
    }

    produced.sort();
    Ok(produced)
}

/// Replace the generator's manifest with a throwaway one, for formatting only.
///
/// Two reasons and neither is optional. The staging directory sits inside this
/// workspace, and cargo refuses to touch a manifest that neither declares
/// itself a member nor opts out — which the generator's does neither of. And
/// the edition decides the formatting: the generator writes 2021, the crates
/// these sources are about to land in are 2024, and `cargo fmt --check` is a
/// gate. Formatting under the wrong edition is a red gate tomorrow.
///
/// This manifest is never placed. Every destination crate's manifest is ours,
/// and `run` refuses to write into a crate directory that has none.
fn stage_manifest(crate_dir: &Path, package: &str) -> Result<(), String> {
    let manifest = [
        "# Throwaway: written so cargo will format the generated sources.",
        "[package]",
        &format!("name = \"{package}\""),
        "version = \"0.0.0\"",
        "edition = \"2024\"",
        "",
        "[lib]",
        "path = \"src/lib.rs\"",
        "",
        "[workspace]",
        "",
    ]
    .join("\n");

    let path = crate_dir.join("Cargo.toml");
    fs::write(&path, manifest).map_err(|e| format!("{}: {e}", path.display()))
}

fn format_crate(crate_dir: &Path) -> Result<(), String> {
    let status = Command::new("cargo")
        .arg("fmt")
        .arg("--manifest-path")
        .arg(crate_dir.join("Cargo.toml"))
        .status()
        .map_err(|e| format!("could not run cargo fmt: {e}"))?;
    if !status.success() {
        return Err(format!("cargo fmt failed in {}", crate_dir.display()));
    }
    Ok(())
}

/// `$VAR`, else the Gradle module cache. The cache path carries the artifact's
/// sha, so the jar is found by name rather than by assembling the path.
fn locate_jar(
    env_var: &str,
    group: &str,
    artifact: &str,
    version: &str,
) -> Result<PathBuf, String> {
    if let Some(value) = std::env::var_os(env_var) {
        let path = PathBuf::from(value);
        if !path.is_file() {
            return Err(format!(
                "${env_var} is set to {}, which is not a file",
                path.display()
            ));
        }
        return Ok(path);
    }

    let gradle_home = std::env::var_os("GRADLE_USER_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".gradle")))
        .ok_or_else(|| "neither $GRADLE_USER_HOME nor $HOME is set".to_string())?;

    let version_dir = gradle_home
        .join("caches/modules-2/files-2.1")
        .join(group)
        .join(artifact)
        .join(version);
    let wanted = format!("{artifact}-{version}.jar");

    let mut found = Vec::new();
    let shas = fs::read_dir(&version_dir).map_err(|_| {
        format!(
            "{artifact} {version} is not in the Gradle cache ({}); set ${env_var} to its jar",
            version_dir.display()
        )
    })?;
    for sha in shas {
        let sha = sha.map_err(|e| format!("{}: {e}", version_dir.display()))?;
        let candidate = sha.path().join(&wanted);
        if candidate.is_file() {
            found.push(candidate);
        }
    }
    found.sort();
    found.into_iter().next().ok_or_else(|| {
        format!(
            "{wanted} is not in {}; set ${env_var} to its jar",
            version_dir.display()
        )
    })
}

fn repository_root() -> Result<PathBuf, String> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest.join("../..");
    root.canonicalize()
        .map_err(|e| format!("{}: {e}", root.display()))
}

struct Options {
    dest: PathBuf,
}

impl Options {
    /// `Ok(None)` is a request for the usage message, not a failure.
    fn parse(args: &[String]) -> Result<Option<Self>, String> {
        let mut dest = PathBuf::from("crates");
        let mut rest = args.iter();
        while let Some(arg) = rest.next() {
            match arg.as_str() {
                "-h" | "--help" => return Ok(None),
                "--dest" => {
                    let value = rest
                        .next()
                        .ok_or_else(|| "--dest needs a directory".to_string())?;
                    dest = PathBuf::from(value);
                }
                other => return Err(format!("unrecognised argument {other}")),
            }
        }
        Ok(Some(Self { dest }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_validation_error_is_not_clean_output() {
        let transcript = "[Error] mutA.xml:10:33: cvc-complex-type.2.4.a: Invalid content was \
                          found starting with element 'bogusElement'.";
        assert!(!transcript_is_clean(transcript));
    }

    #[test]
    fn ordinary_progress_is_clean_output() {
        assert!(transcript_is_clean(""));
        assert!(transcript_is_clean("Generating Rust codecs...\n"));
    }

    #[test]
    fn the_schema_set_is_five_distinct_packages() {
        let packages: Vec<&str> = SCHEMAS.iter().map(|(_, package)| *package).collect();
        assert_eq!(packages.len(), 5);

        let mut sorted = packages.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), packages.len(), "a package is named twice");
    }

    #[test]
    fn only_finished_crates_are_produced_and_strays_are_refused() {
        let stage =
            std::env::temp_dir().join(format!("gen-sbe-codecs-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&stage);
        fs::create_dir_all(stage.join("io_aeron_archive_codecs")).unwrap();
        fs::write(stage.join("io_aeron_archive_codecs/Cargo.toml"), "").unwrap();

        // A schema's directory with no manifest is a crate that did not finish,
        // not a stranger: it is reported as missing by comparing against the
        // schema list, so the diff between the two is what the caller acts on.
        fs::create_dir_all(stage.join("io_aeron_cluster_codecs")).unwrap();
        fs::write(stage.join("README"), "").unwrap();

        let produced = generated_packages(&stage).unwrap();
        assert_eq!(produced, vec!["io_aeron_archive_codecs".to_string()]);
        assert!(!produced.contains(&"io_aeron_cluster_codecs".to_string()));

        // A finished crate no schema names is the other accident, and it stops
        // the run: upstream has renamed a package and the old crate is stale.
        fs::create_dir_all(stage.join("leftover")).unwrap();
        fs::write(stage.join("leftover/Cargo.toml"), "").unwrap();
        assert!(generated_packages(&stage).is_err());

        fs::remove_dir_all(&stage).unwrap();
    }

    #[test]
    fn dest_defaults_to_crates_and_is_overridable() {
        let options = Options::parse(&[]).unwrap().unwrap();
        assert_eq!(options.dest, PathBuf::from("crates"));

        let args = vec!["--dest".to_string(), "/tmp/elsewhere".to_string()];
        let options = Options::parse(&args).unwrap().unwrap();
        assert_eq!(options.dest, PathBuf::from("/tmp/elsewhere"));
    }

    #[test]
    fn help_is_not_a_failure_and_a_stray_argument_is() {
        assert!(Options::parse(&["--help".to_string()]).unwrap().is_none());
        assert!(Options::parse(&["--nonsense".to_string()]).is_err());
        assert!(Options::parse(&["--dest".to_string()]).is_err());
    }
}
