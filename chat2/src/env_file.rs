//! # env_file — configuration source with file-then-process-environment precedence
//!
//! ## Role in the system
//! Credentials and endpoint selection must never be compiled into the binary.
//! This module supplies the single lookup path every configuration read goes
//! through: an optional operator-supplied env file consulted first, with the
//! process environment as fallback. The file name arrives as a command-line
//! argument (see `main.rs`), so one binary serves many endpoints — OpenAI,
//! NVIDIA NIM, a local mock — by pointing at different files.
//!
//! ## Invariants
//! - INV-CHAT-01: No secret (API key) is ever logged, printed, or embedded in
//!   an error message by this module. Errors name the *variable*, never its value.
//! - INV-CHAT-02: Lookup precedence is exactly: env file entry, then process
//!   environment, then None. No other source exists.
//!
//! ## Decisions
//! - DEC-CHAT-01: Hand-rolled parser instead of a `dotenv` crate dependency.
//!   Rationale: the format needed is ~20 lines to parse; every added crate is
//!   another "usual suspect" in defect attribution and another pin for the
//!   cargo 1.75 build. Revisit only if quoting/interpolation needs grow.
//! - DEC-CHAT-02: File syntax accepted: `KEY=VALUE` lines, `#` comments,
//!   blank lines, optional leading `export `, optional single/double quotes
//!   around VALUE (stripped only when they match at both ends). No
//!   interpolation, no multi-line values, no escapes. Unknown syntax is a
//!   hard error at load time — fail loudly at startup, not quietly at use.
//!
//! (INV-/DEC- identifiers are session-minted DRAFT series `CHAT`; renumber at
//! registry merge per the standing ADR/INDEX allocation law.)

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::path::Path;

/// A configuration source. Construct with [`EnvSource::from_file`] when the
/// operator passed an env-file argument, or [`EnvSource::process_only`] when
/// they did not. Either way, callers use one method: [`EnvSource::get`].
#[derive(Debug, Default)]
pub struct EnvSource {
    file_entries: HashMap<String, String>,
    /// Path the entries came from, for diagnostics. Never contains secrets.
    pub origin: Option<String>,
}

impl EnvSource {
    /// A source backed only by the process environment.
    pub fn process_only() -> Self {
        Self::default()
    }

    /// Load `path` as a KEY=VALUE file (syntax per DEC-CHAT-02).
    ///
    /// Errors are load-time and name the offending line number — a malformed
    /// config should stop the program before any network call is attempted.
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read env file {}", path.display()))?;
        let mut file_entries = HashMap::new();
        for (idx, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
            let Some((key, value)) = line.split_once('=') else {
                // INV-CHAT-01: report position, not content (the line could
                // be a mistyped secret).
                bail!(
                    "{}:{}: expected KEY=VALUE, comment, or blank line",
                    path.display(),
                    idx + 1
                );
            };
            let key = key.trim();
            if key.is_empty() || key.contains(char::is_whitespace) {
                bail!("{}:{}: malformed key", path.display(), idx + 1);
            }
            let value = strip_matching_quotes(value.trim());
            file_entries.insert(key.to_string(), value.to_string());
        }
        Ok(Self {
            file_entries,
            origin: Some(path.display().to_string()),
        })
    }

    /// INV-CHAT-02: the one lookup path — file first, then process env.
    pub fn get(&self, key: &str) -> Option<String> {
        if let Some(v) = self.file_entries.get(key) {
            return Some(v.clone());
        }
        std::env::var(key).ok()
    }
}

/// Strip one layer of quotes only when the same quote character opens and
/// closes the value. `"abc"` -> `abc`; `"abc` stays `"abc` (unbalanced is
/// probably intentional or an error the operator should see verbatim).
fn strip_matching_quotes(v: &str) -> &str {
    let b = v.as_bytes();
    if b.len() >= 2 && (b[0] == b'"' || b[0] == b'\'') && b[b.len() - 1] == b[0] {
        &v[1..v.len() - 1]
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn load(content: &str) -> Result<EnvSource> {
        let mut f = tempfile_path();
        std::fs::File::create(&f.0).unwrap().write_all(content.as_bytes()).unwrap();
        let r = EnvSource::from_file(&f.0);
        let _ = std::fs::remove_file(&f.0);
        f.1 = true;
        r
    }
    fn tempfile_path() -> (std::path::PathBuf, bool) {
        let p = std::env::temp_dir().join(format!("envtest-{}-{:?}", std::process::id(), std::time::Instant::now()));
        (p, false)
    }

    #[test]
    fn parses_basic_pairs_comments_blanks() {
        let s = load("# comment\n\nOPENAI_MODEL=meta/llama-3.3-70b-instruct\nexport OPENAI_BASE_URL=https://integrate.api.nvidia.com/v1\n").unwrap();
        assert_eq!(s.get("OPENAI_MODEL").as_deref(), Some("meta/llama-3.3-70b-instruct"));
        assert_eq!(s.get("OPENAI_BASE_URL").as_deref(), Some("https://integrate.api.nvidia.com/v1"));
    }

    #[test]
    fn strips_matching_quotes_only() {
        let s = load("A=\"quoted\"\nB='single'\nC=\"unbalanced\n").unwrap();
        assert_eq!(s.get("A").as_deref(), Some("quoted"));
        assert_eq!(s.get("B").as_deref(), Some("single"));
        assert_eq!(s.get("C").as_deref(), Some("\"unbalanced"));
    }

    #[test]
    fn file_wins_over_process_env() {
        std::env::set_var("ENVFILE_TEST_PRECEDENCE", "from-process");
        let s = load("ENVFILE_TEST_PRECEDENCE=from-file\n").unwrap();
        assert_eq!(s.get("ENVFILE_TEST_PRECEDENCE").as_deref(), Some("from-file"));
        std::env::remove_var("ENVFILE_TEST_PRECEDENCE");
    }

    #[test]
    fn rejects_malformed_line_with_position() {
        let err = load("GOOD=1\nthis is not a pair\n").unwrap_err().to_string();
        assert!(err.contains(":2:"), "error should carry the line number: {err}");
    }
}
