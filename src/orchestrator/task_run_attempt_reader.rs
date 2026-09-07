use crate::crud::task_run_attempt_output::TaskRunAttemptOutputStream;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc::UnboundedSender;


/// The most output one stream of one attempt records.
///
/// Past it the reader keeps reading and stops recording, which bounds both the table and
/// the memory in flight: the channel is unbounded, so the reader declining to send is the
/// only thing that caps it.
pub const MAX_STREAM_BYTES: usize = 1024 * 1024;

const READ_BUF_BYTES: usize = 8192;


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
/// block the child on a full one for ever and turn a noisy task into a hung one. Recording
/// stops; reading does not.
///
/// Ends only at EOF, which is what makes the channel closing mean "both readers are done".
/// A reader whose EOF never comes — a grandchild that escaped the process group still
/// holds the pipe — is aborted by `TaskRunAttemptChild::abort_readers`, since it will not
/// return on its own.
pub async fn read_task_run_attempt_stream<R>(
    mut reader: R,
    stream: TaskRunAttemptOutputStream,
    chunks: UnboundedSender<TaskRunAttemptOutputChunk>,
)
where
    R: AsyncRead + Unpin,
{
    let mut buf = [0u8; READ_BUF_BYTES];
    let mut carry: Vec<u8> = Vec::new();
    let mut recorded: usize = 0;
    let mut capped = false;

    loop {
        let read = match reader.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };

        if capped {
            continue;
        }

        carry.extend_from_slice(&buf[..read]);

        let content = take_valid_utf8(&mut carry);

        if content.is_empty() {
            continue;
        }

        if send_within_cap(stream, &chunks, &mut recorded, &mut capped, content).is_err() {
            return;
        }
    }

    // Whatever is left is a sequence the stream ended in the middle of, which is garbage
    // rather than a boundary waiting for bytes that are never coming.
    if !capped && !carry.is_empty() {
        let _ = send_within_cap(
            stream,
            &chunks,
            &mut recorded,
            &mut capped,
            char::REPLACEMENT_CHARACTER.to_string(),
        );
    }
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


/// Sends what fits under the cap, and the marker once when it does not.
///
/// Errs only when the receiver is gone, which is the monitor having dropped this attempt's
/// child — there is nothing left to record to, so the caller stops.
fn send_within_cap(
    stream: TaskRunAttemptOutputStream,
    chunks: &UnboundedSender<TaskRunAttemptOutputChunk>,
    recorded: &mut usize,
    capped: &mut bool,
    content: String,
) -> Result<(), ()> {

    let room = MAX_STREAM_BYTES.saturating_sub(*recorded);

    if content.len() <= room {
        *recorded += content.len();

        return chunks
            .send(TaskRunAttemptOutputChunk { stream, content })
            .map_err(|_| ());
    }

    let head = truncate_at_char_boundary(&content, room);

    if !head.is_empty() {
        *recorded += head.len();

        chunks
            .send(TaskRunAttemptOutputChunk { stream, content: head.to_string() })
            .map_err(|_| ())?;
    }

    *capped = true;

    chunks
        .send(TaskRunAttemptOutputChunk {
            stream,
            content: format!(
                "\n[flowlite: output truncated, exceeded {} bytes]\n",
                MAX_STREAM_BYTES,
            ),
        })
        .map_err(|_| ())
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

    /// Drives the reader over an in-memory buffer and returns everything it recorded.
    /// Generic over AsyncRead is what makes this possible with no process involved.
    async fn read(bytes: Vec<u8>) -> String {

        let (chunks, mut received) = tokio::sync::mpsc::unbounded_channel();

        read_task_run_attempt_stream(
            std::io::Cursor::new(bytes),
            TaskRunAttemptOutputStream::Stdout,
            chunks,
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

    /// The cap keeps the head and says so once.
    #[tokio::test]
    async fn the_cap_keeps_the_head_and_appends_the_marker() {

        let content = read(vec![b'x'; MAX_STREAM_BYTES + 4096]).await;

        assert_eq!(
            content,
            format!(
                "{}\n[flowlite: output truncated, exceeded {} bytes]\n",
                "x".repeat(MAX_STREAM_BYTES),
                MAX_STREAM_BYTES,
            ),
        );
    }

    /// The cap must not split a character in half to reach its byte count exactly.
    #[tokio::test]
    async fn the_cap_truncates_on_a_character_boundary() {

        // Three-byte characters do not divide the cap, so the last one recorded straddles
        // it and has to be dropped whole rather than sliced.
        let mut bytes = Vec::new();

        while bytes.len() < MAX_STREAM_BYTES + 4096 {
            bytes.extend_from_slice("€".as_bytes());
        }

        let content = read(bytes).await;
        let head = content.split('\n').next().unwrap();

        assert!(head.chars().all(|c| c == '€'), "the cap sliced a character in half");
        assert!(head.len() <= MAX_STREAM_BYTES);
        assert!(head.len() > MAX_STREAM_BYTES - 3);
    }
}
