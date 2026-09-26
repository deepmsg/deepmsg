//! Dump the error-log section of a CnC file, or a rescued error log
//! (M17: a dead driver's error log is rescued to `<date>-error.log` when
//! the directory is reclaimed). Implemented alongside P0.

fn main() {
    eprintln!("errlog-dump: not implemented yet (P0, see docs/roadmap.md)");
    std::process::exit(1);
}
