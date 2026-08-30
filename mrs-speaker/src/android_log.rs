use std::io::{self, Write};
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Clone)]
pub struct MagiskMakeWriter;

pub struct MagiskLineWriter<W: Write> {
    inner: W,
    at_line_start: bool,
}

impl<W: Write> Write for MagiskLineWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut written = 0;
        for chunk in buf.split_inclusive(|&b| b == b'\n') {
            if self.at_line_start && !chunk.is_empty() {
                self.inner.write_all(b"ui_print ")?;
                self.at_line_start = false;
            }
            self.inner.write_all(chunk)?;
            written += chunk.len();
            if chunk.ends_with(b"\n") {
                self.at_line_start = true;
            }
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<'a> MakeWriter<'a> for MagiskMakeWriter {
    type Writer = MagiskLineWriter<io::Stdout>;

    fn make_writer(&'a self) -> Self::Writer {
        MagiskLineWriter {
            inner: io::stdout(),
            at_line_start: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogMode {
    Magisk,
    Standard,
}

pub fn init_tracing(mode: LogMode) {
    let mut directives = vec!["warn".to_string()];
    for name in ["my_remote_speaker", "mrs_speaker"] {
        let target = name.replace('-', "_");
        directives.push(format!("{}=info", target));
    }
    let default_filter = directives.join(",");
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    match mode {
        LogMode::Magisk => {
            fmt()
                .with_env_filter(env_filter)
                .with_writer(MagiskMakeWriter)
                .with_ansi(false)
                .without_time()
                .init();
        }
        LogMode::Standard => {
            fmt()
                .with_env_filter(env_filter)
                .with_writer(io::stderr)
                .with_file(true)
                .with_line_number(true)
                .with_target(true)
                .init();
        }
    }
}

#[macro_export]
macro_rules! magisk_println {
    ($($arg:tt)*) => {{
        use std::io::Write;
        let mut stdout = std::io::stdout().lock();
        let _ = write!(stdout, "ui_print ");
        let _ = writeln!(stdout, $($arg)*);
    }};
}
