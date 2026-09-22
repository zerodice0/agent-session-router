#![forbid(unsafe_code)]
#![allow(
    clippy::missing_errors_doc,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::type_complexity
)]

pub mod app;
pub mod bootstrap;
pub mod cli;
pub mod client;
pub mod config;
pub mod credentials;
pub mod hosts;
pub mod install;
pub mod integrations;
pub mod mcp;
pub mod onboarding;
pub mod process;
pub mod protocol;
pub mod providers;
pub mod router;
pub mod store;
pub mod tasks;
pub mod tls;
pub mod tui;

pub use app::{prepare_process, run};
