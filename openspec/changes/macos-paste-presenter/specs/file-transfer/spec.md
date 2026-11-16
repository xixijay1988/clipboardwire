# file-transfer

## MODIFIED Requirements

### Requirement: Paste-time clipboard entries

On Windows, when `receive_mode` is `paste`, the receiver SHALL expose offerable
files through `CFSTR_FILEDESCRIPTORW` + `CFSTR_FILECONTENTS`, backed by an
`IStream` that pulls bytes on demand while the message pump keeps running.

`FILEDESCRIPTOR` entries SHALL set `FD_FILESIZE` and `FD_PROGRESSUI`.

`Read` SHALL return `S_OK` with zero bytes read at EOF, SHALL implement `Stat`
and `Seek`, and SHALL use an inactivity timeout that treats digest waiting as
progress.

On shutdown the receiver SHALL clear the clipboard only if it still owns it,
and SHALL NOT flush a promise-based object.

On macOS, when `receive_mode` is `paste`, the receiver SHALL expose each
offerable file as an empty placeholder file in a private spool directory,
registered through `NSFileCoordinator`/`NSFilePresenter`, and place the
placeholder's `file://` URL on the general pasteboard.

When a coordinated reader asks to read a placeholder, the receiver SHALL run
the normal lazy pull into the spool, verify the digest, copy the verified
bytes into the placeholder, and only then answer the relinquish request. If
the pull or the verification fails, the receiver SHALL delete the placeholder
before answering, so the paste reports a file-not-found error rather than
copying empty bytes.

macOS paste SHALL NOT claim byte-level progress: the shell shows its
indeterminate "preparing to copy" state for the whole pull.

#### Scenario: Paste into Explorer

- **WHEN** the user pastes on Windows with `receive_mode = "paste"`
- **THEN** Explorer shows a copy-progress dialog and the file appears with
  bytes pulled from the source

#### Scenario: Paste into Finder

- **WHEN** the user pastes in Finder with `receive_mode = "paste"`
- **THEN** Finder shows "preparing to copy" while the pull runs and the file
  appears with the verified bytes

#### Scenario: Source vanished before paste

- **WHEN** the source is offline when the paste starts
- **THEN** the read fails and the shell reports a copy failure

#### Scenario: Digest failure during paste

- **WHEN** the digest does not match at the end of a paste
- **THEN** the final bytes and EOF are withheld, so the shell reports failure
  instead of saving a corrupt file

#### Scenario: macOS paste after offer expired

- **WHEN** the offer was aborted or expired while armed
- **THEN** the placeholder is deleted before the read is released and Finder
  reports a copy error
