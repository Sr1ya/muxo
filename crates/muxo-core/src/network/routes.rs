//! Connections pinned to one network. Linux and macOS pick a socket's outgoing interface from the
//! routing table, so binding to an interface's IP still sends packets out the default route:
//! there the socket is bound to the device itself (SO_BINDTODEVICE / IP_BOUND_IF). Windows routes
//! by source address, so binding the network's own address is enough.

use crate::types::NetworkInterfaceInfo;
use parking_lot::Mutex;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Why a request failed, in the words the window's describeError knows.
#[derive(Debug, Clone)]
pub enum FetchError {
    Aborted,
    /// The connection failed: it couldn't be opened, or it dropped or went silent before the
    /// server had finished. It says something about the network, not the server.
    Connection(String),
    /// The network has no address compatible with any of the host's.
    NoRoute(String),
    /// The server answered with a status that isn't the range asked for.
    Status { status: u16, retry_after_ms: Option<u64> },
    /// A response whose version doesn't match the download's. Nothing from it was written.
    RemoteChanged { detail: String, size: bool, seen: crate::download::file_version::FileVersion },
    Other(String),
}

impl FetchError {
    /// A server that is busy, briefly broken or limiting requests: worth waiting out. The
    /// statuses curl's --retry treats as transient.
    pub fn transient(&self) -> bool {
        matches!(self, FetchError::Status { status: 408 | 429 | 500 | 502 | 503 | 504, .. })
    }
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FetchError::Aborted => write!(f, "Aborted"),
            FetchError::Connection(message) | FetchError::Other(message) => write!(f, "{message}"),
            FetchError::NoRoute(host) => {
                write!(f, "No selected network has an address compatible with {host}")
            }
            FetchError::Status { status, .. } => write!(f, "Unexpected status {status} for range request"),
            FetchError::RemoteChanged { detail, .. } => write!(
                f,
                "The file on the server changed during the download ({detail}). Start the download over."
            ),
        }
    }
}

impl std::error::Error for FetchError {}

/// A reqwest failure as Node would word it (ECONNREFUSED, ENOTFOUND, …), found by walking its
/// causes for the underlying I/O error.
pub fn describe_reqwest(error: &reqwest::Error) -> String {
    use std::error::Error as _;
    let mut source = error.source();
    let mut text = error.to_string();
    while let Some(cause) = source {
        text = format!("{text}: {cause}");
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            let code = match io.kind() {
                std::io::ErrorKind::ConnectionRefused => Some("ECONNREFUSED"),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted => {
                    Some("ECONNRESET")
                }
                std::io::ErrorKind::TimedOut => Some("ETIMEDOUT"),
                _ => None,
            };
            if let Some(code) = code {
                return format!("{code}: {io}");
            }
        }
        source = cause.source();
    }
    if error.is_timeout() {
        return "ETIMEDOUT: the connection timed out".into();
    }
    let lower = text.to_lowercase();
    if lower.contains("dns error") || lower.contains("failed to lookup address") {
        return format!("ENOTFOUND: {text}");
    }
    if lower.contains("certificate") || lower.contains("tls") {
        return format!("TLS: {text}");
    }
    text
}

pub fn connection_error(error: &reqwest::Error) -> FetchError {
    FetchError::Connection(describe_reqwest(error))
}

/// Whether each network can carry its own connections. Always true off Linux; on Linux it needs
/// SO_BINDTODEVICE, unprivileged since 5.7.
pub fn device_binding_supported() -> bool {
    static SUPPORTED: OnceLock<bool> = OnceLock::new();
    *SUPPORTED.get_or_init(|| {
        #[cfg(target_os = "linux")]
        {
            let socket =
                socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None);
            match socket.and_then(|socket| socket.bind_device(Some(b"lo"))) {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!("Per-network binding unavailable, using the default route: {error}");
                    false
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        true
    })
}

/// URL host as sockets take it: brackets off an IPv6 literal.
pub fn target_host(url: &Url) -> String {
    url.host_str().unwrap_or_default().trim_start_matches('[').trim_end_matches(']').to_owned()
}

/// DNS is part of opening a connection, so it must not outlive the connection's deadline.
pub async fn resolve_within(host: &str, port: u16, timeout: Duration) -> Result<Vec<IpAddr>, FetchError> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![ip]);
    }
    match tokio::time::timeout(timeout, tokio::net::lookup_host((host, port))).await {
        Err(_) => Err(FetchError::Connection("Could not resolve the download host in time".into())),
        Ok(Err(error)) => Err(FetchError::Connection(format!("getaddrinfo ENOTFOUND {host}: {error}"))),
        Ok(Ok(addresses)) => Ok(addresses.map(|address| address.ip()).collect()),
    }
}

fn family(ip: &IpAddr) -> u8 {
    if ip.is_ipv4() { 4 } else { 6 }
}

/// The networks with an address of a family one of the host's addresses has.
pub fn compatible_interfaces(
    interfaces: &[NetworkInterfaceInfo],
    remote: &[IpAddr],
) -> Vec<NetworkInterfaceInfo> {
    interfaces
        .iter()
        .filter(|iface| {
            iface.addresses.iter().any(|local| remote.iter().any(|ip| family(ip) == local.family))
        })
        .cloned()
        .collect()
}

fn is_loopback(remote: &[IpAddr]) -> bool {
    !remote.is_empty() && remote.iter().all(IpAddr::is_loopback)
}

/// A client whose every connection goes out through `iface`, to `host`'s addresses in `remote`.
fn build_client(
    iface: &NetworkInterfaceInfo,
    host: &str,
    port: u16,
    remote: &[IpAddr],
    connect_timeout: Duration,
) -> Result<reqwest::Client, FetchError> {
    let compatible: Vec<SocketAddr> = remote
        .iter()
        .filter(|ip| iface.addresses.iter().any(|local| local.family == family(ip)))
        .map(|ip| SocketAddr::new(*ip, port))
        .collect();
    if compatible.is_empty() {
        return Err(FetchError::NoRoute(host.to_owned()));
    }
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(connect_timeout)
        .pool_max_idle_per_host(1)
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_nodelay(true)
        .no_proxy();
    if host.parse::<IpAddr>().is_err() {
        builder = builder.resolve_to_addrs(host, &compatible);
    }
    let local: Option<IpAddr> = iface
        .addresses
        .iter()
        .find(|local| local.family == family(&compatible[0].ip()))
        .and_then(|local| local.address.parse().ok());
    // Loopback (a test server, a local mirror) is reached however the network is set.
    if !is_loopback(remote) {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            if device_binding_supported() {
                builder = builder.interface(&iface.device);
            } else if let Some(local) = local {
                builder = builder.local_address(local);
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        if let Some(local) = local {
            builder = builder.local_address(local);
        }
    }
    builder.build().map_err(|error| FetchError::Other(error.to_string()))
}

struct Pinned {
    key: String,
    client: reqwest::Client,
}

pub type NetworkLookup = Arc<dyn Fn() -> Option<NetworkInterfaceInfo> + Send + Sync>;

/// One download stream's connection to the server, through one network. Requests reuse a single
/// kept-alive socket, so a stream pays for DNS, the TCP and TLS handshakes and TCP slow start once
/// rather than on every block. Every new socket asks for the network as it is now, so one that
/// comes back with a new address is reached at that one.
pub struct StreamConnection {
    network: NetworkLookup,
    timeout: Duration,
    connect_timeout: Duration,
    pinned: Mutex<Option<Pinned>>,
}

impl StreamConnection {
    pub fn new(network: NetworkLookup, timeout: Duration, connect_timeout: Duration) -> Self {
        StreamConnection { network, timeout, connect_timeout, pinned: Mutex::new(None) }
    }

    /// Drops its sockets so the next request opens a new one: they may be bound to an address the
    /// network no longer has, or have died while the computer slept.
    pub fn reconnect(&self) {
        *self.pinned.lock() = None;
    }

    async fn client_for(&self, url: &Url) -> Result<reqwest::Client, FetchError> {
        let iface = (self.network)()
            .ok_or_else(|| FetchError::Connection("The network is not connected".into()))?;
        let host = target_host(url);
        let port = url.port_or_known_default().unwrap_or(80);
        let addresses: Vec<&str> = iface.addresses.iter().map(|a| a.address.as_str()).collect();
        let key = format!("{} {} {host}:{port}", iface.device, addresses.join(","));
        if let Some(pinned) = self.pinned.lock().as_ref().filter(|pinned| pinned.key == key) {
            return Ok(pinned.client.clone());
        }
        let remote = resolve_within(&host, port, self.connect_timeout.min(self.timeout)).await?;
        let client = build_client(&iface, &host, port, &remote, self.connect_timeout)?;
        *self.pinned.lock() = Some(Pinned { key, client: client.clone() });
        Ok(client)
    }

    /// Sends a GET and waits for its response headers, for no longer than the stall timeout.
    pub async fn request(
        &self,
        url: &Url,
        headers: &[(String, String)],
        cancel: &CancellationToken,
    ) -> Result<reqwest::Response, FetchError> {
        let attempt = async {
            let client = self.client_for(url).await?;
            let mut request = client.get(url.clone());
            for (name, value) in headers {
                request = request.header(name.as_str(), value.as_str());
            }
            match tokio::time::timeout(self.timeout, request.send()).await {
                Err(_) => {
                    self.reconnect();
                    Err(FetchError::Connection("Connection stalled: no response from server".into()))
                }
                Ok(Err(error)) => {
                    self.reconnect();
                    Err(connection_error(&error))
                }
                Ok(Ok(response)) => Ok(response),
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => Err(FetchError::Aborted),
            result = attempt => result,
        }
    }
}
