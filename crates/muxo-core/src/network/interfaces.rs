use crate::types::{InterfaceKind, NetworkAddress, NetworkInterfaceInfo};
use parking_lot::RwLock;
use regex::Regex;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_secs(1);

async fn run(program: &str, args: &[&str]) -> Option<String> {
    let mut command = tokio::process::Command::new(program);
    command.args(args).kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    let output = tokio::time::timeout(DISCOVERY_TIMEOUT, command.output()).await.ok()?.ok()?;
    output.status.success().then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

/// macOS device names (en0, en1, …) don't say what they are. `networksetup` knows each device's
/// "Hardware Port" (Wi-Fi, iPhone USB, …). Blocks are read by position, not label, since a Mac in
/// another language translates the labels.
async fn mac_hardware_port_names() -> HashMap<String, String> {
    let mut names = HashMap::new();
    if !cfg!(target_os = "macos") {
        return names;
    }
    static DEVICE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-z]+\d+$").unwrap());
    let Some(stdout) = run("networksetup", &["-listallhardwareports"]).await else { return names };
    for block in stdout.split("\n\n") {
        let fields: Vec<&str> = block
            .lines()
            .filter_map(|line| line.split_once(':').map(|(_, value)| value.trim()))
            .collect();
        if let [port, device, ..] = fields[..] {
            if !port.is_empty() && DEVICE.is_match(device) {
                names.insert(device.to_owned(), port.to_owned());
            }
        }
    }
    names
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct WindowsAdapter {
    name: String,
    interface_description: Option<String>,
    ndis_physical_medium: Option<u32>,
}

/// Adapter aliases ("Wi-Fi", "Ethernet 2") are what Windows shows people; the description and
/// physical medium tell what kind of network a renamed or localized adapter is.
async fn windows_adapters() -> HashMap<String, WindowsAdapter> {
    if !cfg!(windows) {
        return HashMap::new();
    }
    let script = "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; Get-NetAdapter -ErrorAction Stop | Select-Object Name, InterfaceDescription, NdisPhysicalMedium | ConvertTo-Json -Compress";
    let Some(stdout) =
        run("powershell.exe", &["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script]).await
    else {
        return HashMap::new();
    };
    let text = stdout.trim().trim_start_matches('\u{feff}');
    let adapters: Vec<WindowsAdapter> = serde_json::from_str::<Vec<WindowsAdapter>>(text)
        .or_else(|_| serde_json::from_str::<WindowsAdapter>(text).map(|one| vec![one]))
        .unwrap_or_default();
    adapters.into_iter().map(|adapter| (adapter.name.clone(), adapter)).collect()
}

/// Linux names a device by what it is, in systemd's predictable names (wlp2s0, enp3s0,
/// enp0s20f0u1, enx…) or the older ones (wlan0, eth0, usb0).
fn classify_linux_device(device: &str) -> InterfaceKind {
    static USB: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^usb|^enx|^en\w*u\d").unwrap());
    if device.starts_with("wl") {
        InterfaceKind::Wifi
    } else if USB.is_match(device) {
        InterfaceKind::Usb
    } else if device.starts_with("en") || device.starts_with("eth") {
        InterfaceKind::Ethernet
    } else if device.starts_with("br") || device.starts_with("virbr") {
        InterfaceKind::Bridge
    } else {
        InterfaceKind::Other
    }
}

fn classify_interface(hardware_port_name: &str) -> InterfaceKind {
    static WIFI: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"wi-?fi|wireless|wlan|802\.11|airport").unwrap());
    static TETHER: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"rndis|remote ndis|tether|apple mobile device").unwrap());
    let name = hardware_port_name.to_lowercase();
    if WIFI.is_match(&name) {
        InterfaceKind::Wifi
    } else if TETHER.is_match(&name)
        || name.contains("iphone")
        || name.contains("ipad")
        || name.contains("usb")
    {
        InterfaceKind::Usb
    } else if name.contains("bridge") {
        InterfaceKind::Bridge
    } else if name.contains("ethernet") || name.contains("lan") {
        InterfaceKind::Ethernet
    } else {
        InterfaceKind::Other
    }
}

/// The CIDR subnet (e.g. "192.168.1.0/24") of an IPv4 address and netmask.
pub fn ipv4_subnet(address: std::net::Ipv4Addr, netmask: std::net::Ipv4Addr) -> String {
    let network = u32::from(address) & u32::from(netmask);
    format!("{}/{}", std::net::Ipv4Addr::from(network), u32::from(netmask).count_ones())
}

fn usable(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => !v4.is_loopback() && !v4.is_link_local(),
        IpAddr::V6(v6) => !v6.is_loopback() && (v6.segments()[0] & 0xffc0) != 0xfe80,
    }
}

type Labels = (HashMap<String, String>, HashMap<String, WindowsAdapter>);

/// What the OS calls each device, looked up again only when the set of devices changes: it takes
/// a child process, while the addresses themselves are one cheap system call.
static LABELS: LazyLock<tokio::sync::Mutex<Option<(String, Labels)>>> =
    LazyLock::new(Default::default);

/// Active non-loopback interfaces, with every usable local address on each device.
pub async fn list_active_interfaces() -> Vec<NetworkInterfaceInfo> {
    let all = tokio::task::spawn_blocking(if_addrs::get_if_addrs)
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();
    let mut devices: Vec<&str> = all.iter().map(|iface| iface.name.as_str()).collect();
    devices.sort_unstable();
    devices.dedup();
    let key = devices.join("\n");
    let (ports, adapters) = {
        let mut labels = LABELS.lock().await;
        if labels.as_ref().map(|(devices, _)| devices) != Some(&key) {
            let looked_up = tokio::join!(mac_hardware_port_names(), windows_adapters());
            *labels = Some((key, looked_up));
        }
        labels.as_ref().map(|(_, labels)| labels.clone()).unwrap_or_default()
    };

    let mut result: Vec<NetworkInterfaceInfo> = Vec::new();
    for iface in &all {
        let ip = iface.ip();
        if iface.is_loopback() || !usable(&ip) {
            continue;
        }
        let address = match &iface.addr {
            if_addrs::IfAddr::V4(v4) => NetworkAddress {
                address: v4.ip.to_string(),
                family: 4,
                netmask: Some(v4.netmask.to_string()),
                subnet: Some(ipv4_subnet(v4.ip, v4.netmask)),
            },
            if_addrs::IfAddr::V6(v6) => NetworkAddress {
                address: v6.ip.to_string(),
                family: 6,
                netmask: Some(v6.netmask.to_string()),
                subnet: None,
            },
        };
        if let Some(existing) = result.iter_mut().find(|entry| entry.device == iface.name) {
            existing.addresses.push(address);
            continue;
        }
        let device = iface.name.clone();
        let hardware_name = ports.get(&device).cloned();
        let adapter = adapters.get(&device);
        let mut kind = if cfg!(target_os = "linux") {
            classify_linux_device(&device)
        } else {
            classify_interface(
                adapter
                    .and_then(|adapter| adapter.interface_description.as_deref())
                    .or(hardware_name.as_deref())
                    .unwrap_or(&device),
            )
        };
        // NDIS media: 1 = wireless LAN, 9 = native 802.11, 14 = Ethernet (802.3).
        match adapter.and_then(|adapter| adapter.ndis_physical_medium) {
            Some(1) | Some(9) => kind = InterfaceKind::Wifi,
            Some(14) if kind == InterfaceKind::Other => kind = InterfaceKind::Ethernet,
            _ => {}
        }
        result.push(NetworkInterfaceInfo {
            id: device.clone(),
            display_name: hardware_name.unwrap_or_else(|| device.clone()),
            device,
            addresses: vec![address],
            kind,
            mac: None,
        });
    }
    result
}

pub type NetworksListener = Arc<dyn Fn(&[NetworkInterfaceInfo]) + Send + Sync>;

/// The computer's networks, kept current: nothing tells a process when a network appears, drops
/// or gets a new address, so it looks every second. The listener hears of every change.
pub struct NetworkMonitor {
    list: RwLock<Option<Vec<NetworkInterfaceInfo>>>,
    polling: tokio::sync::Mutex<()>,
    listener: RwLock<Option<NetworksListener>>,
}

impl NetworkMonitor {
    pub fn start() -> Arc<Self> {
        let monitor = Arc::new(NetworkMonitor {
            list: RwLock::new(None),
            polling: tokio::sync::Mutex::new(()),
            listener: RwLock::new(None),
        });
        let weak = Arc::downgrade(&monitor);
        tokio::spawn(async move {
            loop {
                let Some(monitor) = weak.upgrade() else { return };
                monitor.refresh().await;
                drop(monitor);
                tokio::time::sleep(POLL).await;
            }
        });
        monitor
    }

    pub fn set_listener(&self, listener: NetworksListener) {
        *self.listener.write() = Some(listener);
    }

    /// None until the first look has finished.
    pub fn current(&self) -> Option<Vec<NetworkInterfaceInfo>> {
        self.list.read().clone()
    }

    pub fn find(&self, id: &str) -> Option<NetworkInterfaceInfo> {
        self.list.read().as_ref()?.iter().find(|iface| iface.id == id).cloned()
    }

    /// Looks now, rather than at the next poll. Looks run one at a time, so the last word is
    /// always the newest.
    pub async fn refresh(&self) -> Vec<NetworkInterfaceInfo> {
        let _turn = self.polling.lock().await;
        let next = list_active_interfaces().await;
        let changed = self.list.read().as_ref() != Some(&next);
        if changed {
            *self.list.write() = Some(next.clone());
            let listener = self.listener.read().clone();
            if let Some(listener) = listener {
                listener(&next);
            }
        }
        next
    }
}
