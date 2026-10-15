//! discord-tee: copy stdin to stdout and to a Discord webhook.
//!
//! Usage: some-command | discord-tee [--raw] [--interval SECS] [WEBHOOK_URL]
//!
//! The webhook URL may also be supplied via the DISCORD_WEBHOOK_URL
//! environment variable. Output is batched into messages of at most
//! 2000 characters (Discord's limit) and flushed at least every
//! `--interval` seconds (default 1).

use std::io::{self, BufRead, Write};
use std::process::ExitCode;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::json;

/// Discord's hard limit on message content length, in characters.
const DISCORD_LIMIT: usize = 2000;
/// Characters taken up by the "```\n" ... "```" code-block fence.
const FENCE_OVERHEAD: usize = 8;
const MAX_RETRIES: u32 = 5;

struct Config {
    url: String,
    code_block: bool,
    interval: Duration,
}

fn usage() -> ! {
    eprintln!(
        "usage: discord-tee [--raw] [--interval SECS] [WEBHOOK_URL]\n\
         \n\
         Copies stdin to stdout and posts it to a Discord webhook.\n\
         \n\
         options:\n  \
           --raw            send text as-is instead of wrapping in a code block\n  \
           --interval SECS  max seconds to buffer before sending (default 1)\n  \
           -h, --help       show this help\n\
         \n\
         WEBHOOK_URL defaults to $DISCORD_WEBHOOK_URL."
    );
    std::process::exit(2);
}

fn parse_args() -> Config {
    let mut url = None;
    let mut code_block = true;
    let mut interval = Duration::from_secs(1);

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => usage(),
            "--raw" => code_block = false,
            "--interval" => {
                let secs: f64 = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .filter(|s: &f64| s.is_finite() && *s >= 0.0)
                    .unwrap_or_else(|| usage());
                interval = Duration::from_secs_f64(secs);
            }
            s if s.starts_with('-') => usage(),
            s if url.is_none() => url = Some(s.to_string()),
            _ => usage(),
        }
    }

    let url = url
        .or_else(|| std::env::var("DISCORD_WEBHOOK_URL").ok())
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| {
            eprintln!("discord-tee: no webhook URL given (argument or $DISCORD_WEBHOOK_URL)");
            usage()
        });

    Config {
        url,
        code_block,
        interval,
    }
}

/// Batches incoming lines into Discord-sized messages and posts them.
struct Sender {
    agent: ureq::Agent,
    url: String,
    code_block: bool,
    /// Max characters of payload per message, excluding any fence.
    capacity: usize,
    buf: String,
}

impl Sender {
    fn new(cfg: &Config) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(30))
            // Discord rejects requests without a bot-style User-Agent.
            .user_agent("DiscordBot (https://github.com/discord-tee, 0.1.0)")
            .build();
        let capacity = if cfg.code_block {
            DISCORD_LIMIT - FENCE_OVERHEAD
        } else {
            DISCORD_LIMIT
        };
        Sender {
            agent,
            url: cfg.url.clone(),
            code_block: cfg.code_block,
            capacity,
            buf: String::new(),
        }
    }

    fn push(&mut self, line: &str) {
        let line = if self.code_block {
            // Keep embedded fences from closing our code block early.
            line.replace("```", "`\u{200b}``")
        } else {
            line.to_string()
        };

        for chunk in split_chars(&line, self.capacity) {
            if char_len(&self.buf) + char_len(chunk) > self.capacity {
                self.flush();
            }
            self.buf.push_str(chunk);
        }
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn flush(&mut self) {
        let body = self.buf.trim_end_matches('\n');
        if body.trim().is_empty() {
            self.buf.clear();
            return;
        }
        let content = if self.code_block {
            format!("```\n{body}```")
        } else {
            body.to_string()
        };
        self.post(&content);
        self.buf.clear();
    }

    fn post(&self, content: &str) {
        let payload = json!({
            "content": content,
            // Never ping @everyone/@here/users/roles from piped output.
            "allowed_mentions": { "parse": [] },
        });

        for _ in 0..MAX_RETRIES {
            match self.agent.post(&self.url).send_json(&payload) {
                Ok(resp) => {
                    // Proactively wait out an exhausted rate-limit bucket.
                    if resp.header("X-RateLimit-Remaining") == Some("0") {
                        if let Some(secs) = resp
                            .header("X-RateLimit-Reset-After")
                            .and_then(|s| s.parse::<f64>().ok())
                        {
                            thread::sleep(Duration::from_secs_f64(secs.max(0.0)));
                        }
                    }
                    return;
                }
                Err(ureq::Error::Status(429, resp)) => {
                    let secs = resp
                        .into_json::<serde_json::Value>()
                        .ok()
                        .and_then(|v| v["retry_after"].as_f64())
                        .unwrap_or(1.0);
                    thread::sleep(Duration::from_secs_f64(secs.max(0.0)));
                }
                Err(ureq::Error::Status(code, resp)) => {
                    let text = resp.into_string().unwrap_or_default();
                    eprintln!("discord-tee: webhook returned HTTP {code}: {text}");
                    return;
                }
                Err(e) => {
                    eprintln!("discord-tee: webhook request failed: {e}");
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
        eprintln!("discord-tee: giving up on message after {MAX_RETRIES} attempts");
    }
}

fn char_len(s: &str) -> usize {
    s.chars().count()
}

/// Split `s` into pieces of at most `max` characters each.
fn split_chars(s: &str, max: usize) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut count = 0;
    for (i, _) in s.char_indices() {
        if count == max {
            out.push(&s[start..i]);
            start = i;
            count = 0;
        }
        count += 1;
    }
    if start < s.len() {
        out.push(&s[start..]);
    }
    out
}

fn main() -> ExitCode {
    let cfg = parse_args();
    let (tx, rx) = mpsc::channel::<String>();

    let mut sender = Sender::new(&cfg);
    let interval = cfg.interval;
    let worker = thread::spawn(move || {
        let mut deadline: Option<Instant> = None;
        loop {
            let msg = match deadline {
                Some(d) => rx.recv_timeout(d.saturating_duration_since(Instant::now())),
                None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
            };
            match msg {
                Ok(line) => {
                    sender.push(&line);
                    if deadline.is_none() && !sender.is_empty() {
                        deadline = Some(Instant::now() + interval);
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    sender.flush();
                    deadline = None;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    sender.flush();
                    return;
                }
            }
        }
    });

    let stdin = io::stdin();
    let mut input = stdin.lock();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let mut stdout_ok = true;
    let mut status = ExitCode::SUCCESS;
    let mut line = Vec::new();

    loop {
        line.clear();
        match input.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                eprintln!("discord-tee: read error: {e}");
                status = ExitCode::FAILURE;
                break;
            }
        }

        // Like `tee -p`: if stdout goes away, keep feeding the webhook.
        if stdout_ok && (out.write_all(&line).is_err() || out.flush().is_err()) {
            stdout_ok = false;
            status = ExitCode::FAILURE;
        }

        let text = String::from_utf8_lossy(&line).into_owned();
        if tx.send(text).is_err() {
            break;
        }
    }

    drop(tx);
    let _ = worker.join();
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_respects_char_boundaries() {
        assert_eq!(split_chars("héllo", 2), vec!["hé", "ll", "o"]);
        assert_eq!(split_chars("", 3), Vec::<&str>::new());
        assert_eq!(split_chars("abc", 3), vec!["abc"]);
    }
}
