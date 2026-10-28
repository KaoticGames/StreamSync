use std::ffi::OsStr;
use std::process::{Command, Output};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// Run a named program with arguments and capture stdout/stderr.
/// On Windows, child processes are created with `CREATE_NO_WINDOW` so console tools
/// do not flash a terminal window during normal operation.
#[cfg_attr(not(windows), allow(dead_code))]
pub fn command_output<I, S>(program: S, args: I) -> std::io::Result<Output>
where
    S: AsRef<OsStr>,
    I: IntoIterator,
    I::Item: AsRef<OsStr>,
{
    #[cfg(windows)]
    {
        Command::new(program)
            .args(args)
            .creation_flags(CREATE_NO_WINDOW)
            .output()
    }
    #[cfg(not(windows))]
    {
        Command::new(program).args(args).output()
    }
}
