use crate::app_config::AppConfig;
use crate::crud::task_run_attempt_output::TaskRunAttemptOutputStream;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc::UnboundedSender;


/// One coalescible piece of one stream, already validated as UTF-8.
///
/// That validation is the contract with TaskRunAttemptMonitor: it concatenates chunks with
/// a String push and never converts bytes itself, so a character split across two reads is
/// this reader's problem and nobody else's.
pub struct TaskRunAttemptOutputChunk {
    pub stream: TaskRunAttemptOutputStream,
    pub content: String,
}


/// Reads one stream of one attempt until it ends, sending on what it reads.
///
/// Reads with a plain await and no timeout, unlike the drain on the poll pass this
/// replaces: "is there more" is answered by the OS waking this task rather than by a clock
/// the pass has to wait out, which is what takes the per-attempt cost off the pass.
///
/// **Keeps reading after the cap.** The pipe is 64 KiB, so a reader that stopped would
/// block the child on a full one for ever and turn a noisy task into a hung one. Sending
/// stops; reading does not.
///
/// Ends only at EOF, which is what makes the channel closing mean "both readers are done".
/// A reader whose EOF never comes — a grandchild that escaped the process group still
/// holds the pipe — is aborted by `TaskRunAttemptChild::abort_readers`, since it will not
/// return on its own.
/// `[orchestrator]`'s `max_stream_bytes` is the most output one stream of one attempt
/// records, kept as a head and a tail — see `StreamCapture`. That bounds the table, and the
/// memory in flight with it: the channel is unbounded, so what this reader declines to send
/// is what caps it, and the tail it holds back is `max_stream_bytes / 2` at most.
pub async fn read_task_run_attempt_stream<R>(
    mut reader: R,
    stream: TaskRunAttemptOutputStream,
    chunks: UnboundedSender<TaskRunAttemptOutputChunk>,
    app_config: AppConfig,
)
where
    R: AsyncRead + Unpin,
{
    let mut buf = vec![0u8; app_config.orchestrator.read_buffer_bytes];
    let mut carry: Vec<u8> = Vec::new();
    let mut capture = StreamCapture::new(app_config.orchestrator.max_stream_bytes);

    loop {
        let read = match reader.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };

        carry.extend_from_slice(&buf[..read]);

        let content = take_valid_utf8(&mut carry);

        if content.is_empty() {
            continue;
        }

        if record(stream, &chunks, &mut capture, content).is_err() {
            return;
        }
    }

    // Whatever is left is a sequence the stream ended in the middle of, which is garbage
    // rather than a boundary waiting for bytes that are never coming.
    if !carry.is_empty()
        && record(stream, &chunks, &mut capture, char::REPLACEMENT_CHARACTER.to_string()).is_err()
    {
        return;
    }

    flush_tail(stream, &chunks, capture);
}


/// Splits off everything in `carry` that is complete UTF-8, leaving at most the three bytes
/// of a trailing incomplete sequence behind for the next read.
///
/// The carry is what makes chunking safe at all: the cumulative `from_utf8_lossy` this
/// replaces healed a split character on the next pass, because it re-converted the whole
/// buffer every time. Independent chunks get no next pass, so a split left unhandled here
/// is corrupted in the table for good.
///
/// Loops rather than checking once: one buffer can hold several invalid sequences, and a
/// single step would leave the rest in the carry. Invalid bytes are consumed and replaced,
/// never carried — carrying them is a livelock, since no number of later bytes can make
/// them valid.
fn take_valid_utf8(carry: &mut Vec<u8>) -> String {

    let mut content = String::new();
    let mut consumed = 0;

    loop {
        match std::str::from_utf8(&carry[consumed..]) {
            Ok(valid) => {
                content.push_str(valid);
                consumed = carry.len();
                break;
            },
            Err(e) => {
                let valid_up_to = e.valid_up_to();

                // Safe: from_utf8 just reported this prefix valid.
                content.push_str(
                    std::str::from_utf8(&carry[consumed..consumed + valid_up_to]).unwrap()
                );

                consumed += valid_up_to;

                match e.error_len() {
                    // The buffer ended mid-sequence: keep it for the next read.
                    None => break,
                    Some(invalid_len) => {
                        content.push(char::REPLACEMENT_CHARACTER);
                        consumed += invalid_len;
                    },
                }
            },
        }
    }

    carry.drain(..consumed);

    content
}


/// One stream's budget, spent from both ends: the head is recorded as it arrives, and once
/// that is full the reader holds a rolling tail of the same size instead, flushed when the
/// stream ends.
///
/// The head alone was the whole budget until it wasn't enough: a reader asks for the last
/// 20 000 bytes of what was recorded, so a command that wrote four megabytes with its answer
/// on the final line handed back four megabytes in, neither the start nor the end. The start
/// of a log says what a command set out to do and the end says how it went; the middle is
/// what a reader can most afford to lose.
///
/// The tail is the one thing here held in memory rather than sent on, so it is what bounds
/// this reader's footprint: half of `max_stream_bytes` per stream.
struct StreamCapture {
    max_bytes: usize,
    head_room: usize,
    tail_bytes: usize,
    tail: String,
    /// Everything past the head, whether or not it is still in the tail - the subtraction
    /// that tells a reader how much went missing.
    held: usize,
    capped: bool,
}

impl StreamCapture {
    fn new(max_stream_bytes: usize) -> Self {
        let head = max_stream_bytes / 2;

        StreamCapture {
            max_bytes: max_stream_bytes,
            head_room: head,
            tail_bytes: max_stream_bytes - head,
            tail: String::new(),
            held: 0,
            capped: false,
        }
    }
}

/// Sends what still fits in the head, and holds the rest in the tail.
///
/// Errs only when the receiver is gone, which is the monitor having dropped this attempt's
/// child - there is nothing left to record to, so the caller stops.
fn record(
    stream: TaskRunAttemptOutputStream,
    chunks: &UnboundedSender<TaskRunAttemptOutputChunk>,
    capture: &mut StreamCapture,
    content: String,
) -> Result<(), ()> {

    if content.len() <= capture.head_room {
        capture.head_room -= content.len();

        return chunks
            .send(TaskRunAttemptOutputChunk { stream, content })
            .map_err(|_| ());
    }

    let head = truncate_at_char_boundary(&content, capture.head_room);

    if !head.is_empty() {
        capture.head_room -= head.len();

        chunks
            .send(TaskRunAttemptOutputChunk { stream, content: head.to_string() })
            .map_err(|_| ())?;
    }

    let rest = &content[head.len()..];

    capture.held += rest.len();
    capture.tail.push_str(rest);

    trim_to_tail(&mut capture.tail, capture.tail_bytes);

    // Only now is the stream known to be over its budget - up to here the tail has been
    // holding everything past the head, and a stream that ends inside the budget is
    // recorded whole, markers and all. Said as it happens rather than at the end, so
    // somebody watching a running attempt is told why its output stopped instead of
    // reading a log that simply breaks off.
    if !capture.capped && capture.tail.len() < capture.held {
        capture.capped = true;

        chunks
            .send(TaskRunAttemptOutputChunk {
                stream,
                content: format!(
                    "\n[flowlite: over {} bytes, so the middle is dropped and the last {} \
                     follow when the command ends]\n",
                    capture.max_bytes,
                    capture.tail_bytes,
                ),
            })
            .map_err(|_| ())?;
    }

    Ok(())
}

/// Sends the tail the stream ended with, under a marker naming what fell out between the
/// halves - the number that tells a reader whether they lost two kilobytes or two gigabytes.
///
/// A stream that stayed inside its budget has no marker: its tail is simply the rest of it,
/// and what lands in the table is byte-for-byte what the command wrote.
fn flush_tail(
    stream: TaskRunAttemptOutputStream,
    chunks: &UnboundedSender<TaskRunAttemptOutputChunk>,
    capture: StreamCapture,
) {

    if capture.capped {
        let _ = chunks.send(TaskRunAttemptOutputChunk {
            stream,
            content: format!("[flowlite: {} bytes dropped]\n", capture.held - capture.tail.len()),
        });
    }

    if !capture.tail.is_empty() {
        let _ = chunks.send(TaskRunAttemptOutputChunk { stream, content: capture.tail });
    }
}

/// Drops from the front until at most `max` bytes are left, on a character boundary - the
/// tail's own version of the cut `truncate_at_char_boundary` makes at the other end.
fn trim_to_tail(tail: &mut String, max: usize) {

    if tail.len() <= max {
        return;
    }

    let cut_at = tail.len() - max;

    let start = (cut_at..=tail.len())
        .find(|&i| tail.is_char_boundary(i))
        .unwrap_or(tail.len());

    tail.drain(..start);
}

/// The longest prefix of at most `max` bytes that is still whole characters. Slicing on a
/// byte count alone panics when it lands inside one.
fn truncate_at_char_boundary(content: &str, max: usize) -> &str {

    if max >= content.len() {
        return content;
    }

    let mut end = max;

    while end > 0 && !content.is_char_boundary(end) {
        end -= 1;
    }

    &content[..end]
}


#[cfg(test)]
mod tests {
    use super::*;
    /// What a data directory with no config.toml reads with, so these tests follow the
    /// default rather than restating it.
    fn max_stream_bytes() -> usize {
        AppConfig::default().orchestrator.max_stream_bytes
    }

    /// Drives the reader over an in-memory buffer and returns everything it recorded.
    /// Generic over AsyncRead is what makes this possible with no process involved.
    async fn read(bytes: Vec<u8>) -> String {

        let (chunks, mut received) = tokio::sync::mpsc::unbounded_channel();

        read_task_run_attempt_stream(
            std::io::Cursor::new(bytes),
            TaskRunAttemptOutputStream::Stdout,
            chunks,
            AppConfig::default(),
        ).await;

        let mut content = String::new();

        while let Some(chunk) = received.recv().await {
            content.push_str(&chunk.content);
        }

        content
    }

    #[tokio::test]
    async fn it_records_what_the_stream_wrote() {
        assert_eq!(read(b"hello\n".to_vec()).await, "hello\n");
    }

    /// A multi-byte character split across a read boundary must survive. The cumulative
    /// from_utf8_lossy this replaces healed such a split on the next pass; independent
    /// chunks get no next pass, so the carry is the only thing that saves it.
    #[tokio::test]
    async fn a_character_split_across_a_read_boundary_survives() {

        // 8191 filler bytes, then a three-byte character straddling the 8192-byte read.
        let mut bytes = vec![b'a'; 8191];
        bytes.extend_from_slice("€".as_bytes());

        let content = read(bytes).await;

        assert_eq!(content.chars().filter(|c| *c == '€').count(), 1);
        assert!(
            !content.contains(char::REPLACEMENT_CHARACTER),
            "the split character was corrupted rather than carried over",
        );
        assert!(content.ends_with('€'));
    }

    /// Invalid bytes are consumed, not carried. Carrying them would stall the reader for
    /// ever on bytes that can never become valid.
    #[tokio::test]
    async fn invalid_bytes_become_one_replacement_character_each() {
        assert_eq!(read(vec![b'a', 0xff, b'b']).await, "a\u{fffd}b");
    }

    /// Several invalid sequences in one buffer are all consumed, which is why the split
    /// loops instead of checking once.
    #[tokio::test]
    async fn several_invalid_sequences_in_one_buffer_are_all_consumed() {
        assert_eq!(read(vec![b'a', 0xff, b'b', 0xfe, b'c']).await, "a\u{fffd}b\u{fffd}c");
    }

    /// A sequence the stream ended in the middle of is garbage, not a boundary.
    #[tokio::test]
    async fn a_truncated_sequence_at_end_of_stream_is_flushed() {

        let mut bytes = b"a".to_vec();
        bytes.push(0xe2);

        assert_eq!(read(bytes).await, "a\u{fffd}");
    }

    /// A stream longer than the head but still inside the budget is recorded whole and
    /// unmarked. The tail holds exactly what the head does not, so nothing is dropped and
    /// there is nothing to say - the case a split budget could most easily have spoiled.
    #[tokio::test]
    async fn a_stream_past_the_head_but_inside_the_budget_is_still_whole() {

        let content = read(vec![b'x'; max_stream_bytes() - 1]).await;

        assert_eq!(content, "x".repeat(max_stream_bytes() - 1));
    }

    /// The point of the whole thing: the end of an over-long stream survives. A command
    /// that writes for an hour and then says how it went used to have the answer dropped,
    /// because the head was the only half kept and a reader asks for the last bytes of it.
    #[tokio::test]
    async fn the_cap_keeps_the_head_and_the_tail_it_ended_with() {

        let mut bytes = vec![b'h'; max_stream_bytes()];
        bytes.extend_from_slice(&vec![b'm'; max_stream_bytes()]);
        bytes.extend_from_slice(b"the answer\n");

        let content = read(bytes).await;

        assert!(content.starts_with(&"h".repeat(max_stream_bytes() / 2)), "the head was not kept");
        assert!(content.ends_with("the answer\n"), "the tail was not kept");
        assert!(!content.contains(&"m".repeat(max_stream_bytes())), "the middle was recorded");
    }

    /// Both halves of the budget, and no more: what is recorded is the cap plus the two
    /// marker lines, however much was written.
    #[tokio::test]
    async fn the_two_halves_together_stay_inside_the_budget() {

        let content = read(vec![b'x'; max_stream_bytes() * 4]).await;

        let bytes: usize = content
            .lines()
            .filter(|line| !line.starts_with("[flowlite:"))
            .map(str::len)
            .sum();

        assert_eq!(bytes, max_stream_bytes());
    }

    /// The count is the whole reason to say anything: it tells a reader whether they lost
    /// two kilobytes or two gigabytes.
    #[tokio::test]
    async fn the_marker_names_how_many_bytes_went_missing() {

        let written = max_stream_bytes() * 3;

        let content = read(vec![b'x'; written]).await;

        assert!(
            content.contains(&format!("[flowlite: {} bytes dropped]", written - max_stream_bytes())),
            "{}",
            content.lines().filter(|line| line.starts_with("[flowlite:")).collect::<Vec<_>>().join(" / "),
        );
    }

    /// Neither cut may split a character in half to reach its byte count exactly - the head
    /// cutting forwards, the tail dropping from the front.
    #[tokio::test]
    async fn neither_half_is_cut_inside_a_character() {

        // Three-byte characters do not divide either half, so the character at each cut
        // straddles it and has to go whole rather than be sliced.
        let mut bytes = Vec::new();

        while bytes.len() < max_stream_bytes() * 2 {
            bytes.extend_from_slice("€".as_bytes());
        }

        let content = read(bytes).await;

        for line in content.lines().filter(|line| !line.starts_with("[flowlite:")) {
            assert!(line.chars().all(|c| c == '€'), "a character was sliced in half");
        }

        let head = content.lines().next().unwrap();
        assert!(head.len() <= max_stream_bytes() / 2);
        assert!(head.len() > max_stream_bytes() / 2 - 3);
    }
}
