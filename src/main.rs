#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    let arguments = std::env::args_os().collect::<Vec<_>>();
    if let Err(error) = agent_session_router::prepare_process(&arguments) {
        eprintln!("{error}");
        return ExitCode::from(u8::try_from(error.exit_code()).unwrap_or(1));
    }
    let Ok(runtime) = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    else {
        eprintln!("runtime_initialization_failed: could not initialize the async runtime");
        return ExitCode::FAILURE;
    };
    let code = runtime.block_on(agent_session_router::run(arguments));
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    code
}
