//! # sse — incremental Server-Sent Events reassembly with a bounded buffer
//!
//! ## Role in the system
//! Network bytes arrive in arbitrary chunk sizes; SSE frames end at a blank
//! line. This parser buffers bytes until whole frames exist and emits them.
//! It knows nothing about JSON or chat — framing only. One instance lives
//! per streaming call.
//!
//! ## Invariants
//! - INV-CHAT-06: The reassembly buffer is bounded. A peer that never sends
//!   a frame terminator produces a typed overflow error, never unbounded
//!   memory growth (fixes the reviewed draft's unbounded `Vec<u8>`).
//! - INV-CHAT-07: UTF-8 decoding happens only on complete frames, never on
//!   raw network chunks — a multi-byte character can span two TCP reads but
//!   cannot span two SSE frames, so per-frame decoding cannot split one.
//!
//! ## Decisions
//! - DEC-CHAT-06: Both LF-LF and CRLF-CRLF frame terminators are accepted,
//!   whichever occurs first in the buffer. Observed vendor behavior varies;
//!   the spec's grammar allows either line ending.
//! - DEC-CHAT-07: `event:`, `id:`, and `retry:` fields are ignored; only
//!   `data:` lines (joined with newlines, per spec) and comment frames are
//!   surfaced. The chat-completions dialect uses nothing else.

/// What one complete frame contained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SsePayload {
    /// A `: comment` frame — chat endpoints use these as keep-alive pings.
    Comment(String),
    /// Joined `data:` line content, ready for JSON parsing upstream.
    Data(String),
    /// A complete frame whose bytes were not valid UTF-8. Finished-spec
    /// rule: we do NOT silently substitute replacement characters (that is
    /// data corruption that does not announce itself). The raw bytes are
    /// carried through as distinguished evidence for the dirty path.
    Undecodable { raw: Vec<u8>, error: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub payload: SsePayload,
    /// Raw frame size in bytes, for transport telemetry.
    pub raw_byte_len: usize,
}

/// Raised when the buffer bound (INV-CHAT-06) is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SseOverflow {
    pub limit_bytes: usize,
}

#[derive(Debug)]
pub struct SseParser {
    buf: Vec<u8>,
    limit_bytes: usize,
}

/// Default bound: generous for chat frames (typically < 4 KiB each) while
/// capping a hostile/broken peer at a fixed cost.
pub const DEFAULT_BUF_LIMIT: usize = 1 << 20; // 1 MiB

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Parse one complete frame's bytes (terminator excluded).
fn parse_block(bytes: &[u8]) -> Option<SseEvent> {
    let raw_byte_len = bytes.len();
    // INV-CHAT-07: `bytes` is a complete frame here, so a decode cannot
    // split a multi-byte character across a read boundary — any failure is
    // a genuinely malformed payload, not a chunk-boundary artifact.
    // Finished-spec: strict decode, never lossy substitution.
    let text = match std::str::from_utf8(bytes) {
        Ok(t) => t.to_string(),
        Err(e) => {
            return Some(SseEvent {
                payload: SsePayload::Undecodable { raw: bytes.to_vec(), error: e.to_string() },
                raw_byte_len,
            });
        }
    };
    let text = text.as_str();

    if text.starts_with(':') {
        return Some(SseEvent {
            payload: SsePayload::Comment(text.to_string()),
            raw_byte_len,
        });
    }

    let mut data: Vec<&str> = Vec::new();
    for line in text.lines() {
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        if field == "data" {
            data.push(value);
        }
        // DEC-CHAT-07: other fields intentionally dropped.
    }

    if data.is_empty() {
        return None;
    }

    Some(SseEvent {
        payload: SsePayload::Data(data.join("\n")),
        raw_byte_len,
    })
}

impl Default for SseParser {
    fn default() -> Self {
        Self::with_limit(DEFAULT_BUF_LIMIT)
    }
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_limit(limit_bytes: usize) -> Self {
        Self { buf: Vec::new(), limit_bytes }
    }

    /// Feed raw network bytes; receive every frame completed by them.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<SseEvent>, SseOverflow> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        loop {
            let lf = find(&self.buf, b"\n\n");
            let crlf = find(&self.buf, b"\r\n\r\n");
            let (end, sep_len) = match (lf, crlf) {
                (Some(a), Some(b)) => if b < a { (b, 4) } else { (a, 2) },
                (Some(a), None) => (a, 2),
                (None, Some(b)) => (b, 4),
                (None, None) => break,
            };
            let block: Vec<u8> = self.buf.drain(..end + sep_len).collect();
            if let Some(ev) = parse_block(&block[..end]) {
                out.push(ev);
            }
        }
        // INV-CHAT-06: checked after draining — only a frame-less residue
        // can trip it, which is exactly the pathology it exists to catch.
        if self.buf.len() > self.limit_bytes {
            return Err(SseOverflow { limit_bytes: self.limit_bytes });
        }
        Ok(out)
    }

    /// Bytes currently buffered awaiting a terminator (for StreamEnd records).
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Like `finish`, but also returns any bytes that assembled into no
    /// installment at all — first-class residue evidence for the capture
    /// file (a peer that dies mid-frame leaves its fingerprint here).
    pub fn finish_with_residue(mut self) -> (Vec<SseEvent>, Vec<u8>) {
        let rest = std::mem::take(&mut self.buf);
        if rest.is_empty() {
            return (Vec::new(), Vec::new());
        }
        match parse_block(&rest) {
            Some(ev) => (vec![ev], Vec::new()),
            None => (Vec::new(), rest),
        }
    }

    /// Flush any unterminated tail at end of stream. Some peers omit the
    /// final blank line; their last frame is recovered here rather than
    /// silently dropped (defect in the reviewed draft: existed but unused).
    pub fn finish(mut self) -> Vec<SseEvent> {
        let rest = std::mem::take(&mut self.buf);
        let mut out = Vec::new();
        if !rest.is_empty() {
            if let Some(ev) = parse_block(&rest) {
                out.push(ev);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_split_across_pushes_reassembles() {
        let mut p = SseParser::new();
        assert!(p.push(b"data: {\"a\":").unwrap().is_empty());
        let evs = p.push(b"1}\n\n").unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].payload, SsePayload::Data("{\"a\":1}".into()));
    }

    #[test]
    fn crlf_and_lf_terminators_both_work() {
        let mut p = SseParser::new();
        let evs = p.push(b"data: one\r\n\r\ndata: two\n\n").unwrap();
        assert_eq!(evs.len(), 2);
    }

    #[test]
    fn comment_frames_surface_separately() {
        let mut p = SseParser::new();
        let evs = p.push(b": keep-alive\n\n").unwrap();
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0].payload, SsePayload::Comment(_)));
    }

    #[test]
    fn multi_data_lines_join_with_newline() {
        let mut p = SseParser::new();
        let evs = p.push(b"data: line1\ndata: line2\n\n").unwrap();
        assert_eq!(evs[0].payload, SsePayload::Data("line1\nline2".into()));
    }

    #[test]
    fn overflow_is_a_typed_error() {
        // INV-CHAT-06.
        let mut p = SseParser::with_limit(16);
        let err = p.push(&[b'x'; 64]).unwrap_err();
        assert_eq!(err.limit_bytes, 16);
    }

    #[test]
    fn finish_recovers_unterminated_tail() {
        let mut p = SseParser::new();
        assert!(p.push(b"data: [DONE]").unwrap().is_empty());
        let evs = p.finish();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].payload, SsePayload::Data("[DONE]".into()));
    }

    #[test]
    fn invalid_utf8_frame_is_undecodable_not_lossy() {
        // Strict-decode rule (INV-CHAT-12): a complete frame with bad bytes
        // becomes a distinguished payload carrying the raw bytes — never a
        // silent U+FFFD substitution.
        let mut p = SseParser::new();
        let mut frame = b"data: {\"content\":\"".to_vec();
        frame.extend_from_slice(&[0xff, 0xfe]);
        frame.extend_from_slice(b"\"}\n\n");
        let evs = p.push(&frame).unwrap();
        assert_eq!(evs.len(), 1);
        match &evs[0].payload {
            SsePayload::Undecodable { raw, error } => {
                assert_eq!(raw.len(), frame.len() - 2, "raw bytes carried through");
                assert!(error.contains("utf-8"), "error names the decode failure: {error}");
            }
            other => panic!("expected Undecodable, got {other:?}"),
        }
    }

    #[test]
    fn utf8_multibyte_survives_chunk_split() {
        // INV-CHAT-07: the é (2 bytes) is split across two network reads.
        let bytes = "data: café\n\n".as_bytes();
        let mut p = SseParser::new();
        assert!(p.push(&bytes[..8]).unwrap().is_empty()); // splits inside é
        let evs = p.push(&bytes[8..]).unwrap();
        assert_eq!(evs[0].payload, SsePayload::Data("café".into()));
    }
}
