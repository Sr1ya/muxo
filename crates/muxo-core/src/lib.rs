//! Muxo's engine: downloads files and torrents faster by combining every network connection the
//! computer has. It knows nothing of windows; the app around it implements `Host`.

pub mod bridge;
pub mod core;
pub mod download;
pub mod errors;
pub mod links;
pub mod network;
pub mod schedule;
pub mod settings;
pub mod types;
pub mod util;

pub use crate::core::{Core, CoreConfig, Host};
pub use bridge::{start_bridge, Bridge};
pub use links::{accepted_link, link_from_args, Offer};
pub use network::routes::device_binding_supported;
