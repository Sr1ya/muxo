//! Links the OS hands Muxo (a magnet link clicked in a browser, a .torrent opened from the file
//! manager) or the browser extension does. They're never started on their own: the user still
//! sees what the link is and presses Start. Each is kept until it's started or let go, since the
//! browser has already dropped its copy.

use crate::types::{BrowserContext, LinkResumes, PendingLink, ProbeResult};
use parking_lot::Mutex;
use std::path::Path;

/// The browser's sign-in stays in the engine: the window never sees it.
#[derive(Debug, Clone, Default)]
pub struct Offer {
    pub url: String,
    pub probe: Option<ProbeResult>,
    pub browser: Option<BrowserContext>,
    pub from: Option<String>,
    pub signed_in_to: Option<String>,
    pub resumes: Option<LinkResumes>,
}

#[derive(Default)]
pub struct Links {
    links: Mutex<Vec<(String, Offer)>>,
}

/// A magnet link, or the path of a .torrent file that exists; anything else isn't taken.
pub fn accepted_link(candidate: &str) -> Option<String> {
    if candidate.len() > 8 && candidate[..8].eq_ignore_ascii_case("magnet:?") {
        return Some(candidate.to_owned());
    }
    let path = Path::new(candidate);
    (path.is_absolute() && candidate.to_lowercase().ends_with(".torrent") && path.is_file()).then(|| candidate.to_owned())
}

/// The link in a command line (Windows and Linux hand one over as an argument).
pub fn link_from_args(args: &[String]) -> Option<String> {
    args.iter().skip(1).find_map(|argument| accepted_link(argument))
}

impl Links {
    pub fn add(&self, offer: Offer) {
        self.links.lock().push((uuid::Uuid::new_v4().to_string(), offer));
    }

    /// Oldest first.
    pub fn pending(&self) -> Vec<PendingLink> {
        self.links
            .lock()
            .iter()
            .map(|(id, offer)| PendingLink {
                id: id.clone(),
                url: offer.url.clone(),
                probe: offer.probe.clone(),
                from: offer.from.clone(),
                signed_in_to: offer.signed_in_to.clone(),
                resumes: offer.resumes.clone(),
            })
            .collect()
    }

    pub fn get(&self, id: &str) -> Option<Offer> {
        self.links.lock().iter().find(|(link, _)| link == id).map(|(_, offer)| offer.clone())
    }

    pub fn browser(&self, id: &str) -> Option<BrowserContext> {
        self.get(id).and_then(|offer| offer.browser)
    }

    pub fn release(&self, id: &str) {
        self.links.lock().retain(|(link, _)| link != id);
    }
}
