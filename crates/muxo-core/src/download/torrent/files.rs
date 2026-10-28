//! Which of a torrent's files are chosen, and which pieces they need.

use crate::types::TorrentFile;
use std::collections::BTreeSet;

/// The files to download, from what the window asked for: None for all of them. Indexes that
/// don't name a file, or none at all, are refused.
pub fn chosen_files(requested: Option<&[usize]>, count: usize) -> anyhow::Result<Option<BTreeSet<usize>>> {
    let Some(requested) = requested else { return Ok(None) };
    let chosen: BTreeSet<usize> = requested.iter().copied().collect();
    if chosen.iter().any(|index| *index >= count) {
        anyhow::bail!("A file chosen for this torrent isn’t in it");
    }
    if chosen.is_empty() {
        anyhow::bail!("Choose at least one file to download");
    }
    Ok(if chosen.len() == count { None } else { Some(chosen) })
}

/// The first and last piece each file has bytes in; None for an empty file.
fn file_spans(files: &[TorrentFile], piece_length: u64) -> Vec<Option<(usize, usize)>> {
    let mut offset = 0;
    files
        .iter()
        .map(|file| {
            let span = (file.length > 0)
                .then(|| ((offset / piece_length) as usize, ((offset + file.length - 1) / piece_length) as usize));
            offset += file.length;
            span
        })
        .collect()
}

/// Which pieces the chosen files need: a piece is needed when any byte of it belongs to one.
pub fn wanted_pieces(files: &[TorrentFile], piece_length: u64, chosen: &BTreeSet<usize>) -> Vec<bool> {
    let total: u64 = files.iter().map(|file| file.length).sum();
    let mut wanted = vec![false; total.div_ceil(piece_length) as usize];
    for (index, span) in file_spans(files, piece_length).into_iter().enumerate() {
        if let (Some((first, last)), true) = (span, chosen.contains(&index)) {
            wanted[first..=last].iter_mut().for_each(|piece| *piece = true);
        }
    }
    wanted
}

/// The files every piece of which is in. An empty file is.
pub fn finished_files(files: &[TorrentFile], piece_length: u64, completed: &[bool]) -> BTreeSet<usize> {
    file_spans(files, piece_length)
        .into_iter()
        .enumerate()
        .filter(|(_, span)| span.is_none_or(|(first, last)| completed[first..=last].iter().all(|done| *done)))
        .map(|(index, _)| index)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn needs_the_pieces_at_a_files_edges() {
        let files = vec![
            TorrentFile { path: "a".into(), length: 10 },
            TorrentFile { path: "b".into(), length: 10 },
            TorrentFile { path: "c".into(), length: 0 },
        ];
        let wanted = wanted_pieces(&files, 8, &BTreeSet::from([1]));
        assert_eq!(wanted, vec![false, true, true]);
        assert_eq!(finished_files(&files, 8, &[true, true, false]), BTreeSet::from([0, 2]));
        assert!(chosen_files(Some(&[]), 3).is_err());
        assert_eq!(chosen_files(Some(&[0, 1, 2]), 3).unwrap(), None);
    }
}
