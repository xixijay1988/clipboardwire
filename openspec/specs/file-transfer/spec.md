# file-transfer

Shipped behaviour as of v0.5.4 (protocol v0.3.0). Covers how a file leaves one
client and becomes a real file on another.

## Requirements

### Requirement: Chunked file transmission

The sender SHALL split a file into `file_chunk` frames of
`FILE_CHUNK_BYTES` (4 MiB) raw bytes each, base64-encode each chunk into the
frame's `payload_b64` field, and send all chunks over the existing `/sync`
WebSocket connection.

Every chunk of one file SHALL carry the same `file_id` (RFC 4122 v4 UUID),
`name` (filename only, no directory component), `content_type`,
`total_chunks`, `file_size`, and `file_sha256`.

#### Scenario: Single-chunk file

- **WHEN** a 100 KiB file is sent
- **THEN** exactly one `file_chunk` frame is produced with `chunk_index = 0`
  and `total_chunks = 1`

#### Scenario: Multi-chunk file

- **WHEN** a 9 MiB file is sent
- **THEN** three `file_chunk` frames are produced with
  `chunk_index` 0, 1, 2 and `total_chunks = 3`

#### Scenario: Chunk size stays inside the frame limit

- **WHEN** a chunk is base64-encoded
- **THEN** the resulting JSON frame is smaller than `MAX_FRAME_BYTES`
  (50 MiB) so the hub does not reject it with `frame_too_large`

### Requirement: Whole-file hash computed before the first chunk

The sender SHALL compute the SHA-256 of the entire file before enqueueing the
first `file_chunk`, and SHALL put it in every chunk as a colon-separated
uppercase hex string.

#### Scenario: Hash precedes transfer

- **WHEN** `send_file_through` is called
- **THEN** the file is read end-to-end for hashing before any chunk reaches
  the outbound channel

### Requirement: Zero-byte files are refused

The sender SHALL reject a zero-byte file with an error rather than emitting a
transfer with no payload.

#### Scenario: Empty file

- **WHEN** the user copies a 0-byte file
- **THEN** the send is refused and an error is logged; nothing goes on the wire

### Requirement: Hub relays chunks without caching

The hub SHALL fan every valid `file_chunk` out to all other currently
registered clients and SHALL NOT store it for late joiners. Before relaying,
the hub SHALL stamp `from` with the origin connection's client id and SHALL
validate name, NUL, `total_chunks`, `chunk_index`, `payload_b64` and
`file_sha256` via `FileChunkFrame::validate`.

#### Scenario: Two peers connected

- **WHEN** client A sends a chunk while B is connected
- **THEN** B receives the chunk with `from` set to A's client id
- **AND** A does not receive its own chunk

#### Scenario: Peer connects after the transfer

- **WHEN** a chunk was relayed while no peer was connected
- **THEN** that chunk is lost; a later-connecting peer cannot recover it

### Requirement: Receiver assembles into a partial file

The receiver SHALL lazily create `<save_dir>/<file_id>.partial` on the first
chunk of a transfer, pre-allocate it to `file_size`, and seek-write every
chunk at `chunk_index * FILE_CHUNK_BYTES`.

Chunks MAY arrive out of order. A duplicate `chunk_index` SHALL be ignored
without error. Inconsistent `total_chunks`, `file_sha256` or `file_size`
across chunks of the same `file_id` SHALL abort that transfer with an error.

#### Scenario: Out-of-order arrival

- **WHEN** chunks arrive in reverse order
- **THEN** the assembled file matches the original bytes

#### Scenario: Duplicate chunk

- **WHEN** the same `chunk_index` arrives twice
- **THEN** the second copy is ignored and the transfer still completes

### Requirement: SHA-256 verified before the file is published

On the final chunk the receiver SHALL `sync_all` the partial file, recompute
its SHA-256, compare against `file_sha256`, and only then rename it into
`<save_dir>/<name>`.

On mismatch the partial file SHALL be removed and the transfer reported as
failed.

#### Scenario: Tampered payload

- **WHEN** any chunk's payload differs from what the sender hashed
- **THEN** verification fails, the `.partial` file is deleted, and no file
  appears in the save dir

### Requirement: Received filename cannot escape the save dir

The receiver SHALL strip path separators and control characters from the
sender-supplied `name`, and SHALL fall back to `received-file` for empty,
`.` and `..` names. Name collisions SHALL be resolved with a ` (n)` suffix
before falling back to a UUID suffix.

#### Scenario: Traversal attempt

- **WHEN** the sender sets `name` to `../etc/passwd`
- **THEN** the file is written inside the save dir as `.._etc_passwd`

#### Scenario: Same name twice

- **WHEN** two files named `doc.txt` are received
- **THEN** the second lands as `doc (1).txt`

### Requirement: Default save location

Received files SHALL be saved under the platform downloads directory in a
`clipboardwire/` subdirectory, created on first use. If no downloads directory
can be resolved the receiver SHALL fall back to `<home>/Downloads/clipboardwire`.

#### Scenario: Fresh install

- **WHEN** the first file arrives
- **THEN** `~/Downloads/clipboardwire/` is created and the file lands there

### Requirement: Multi-file clipboard batching on receive

When several files complete within `RECEIVE_BATCH_WINDOW` (500 ms) of each
other, the receiver SHALL apply them to the OS clipboard as one list rather
than one clipboard set per file.

#### Scenario: Five files copied together

- **WHEN** five files finish transferring within the batch window
- **THEN** the OS clipboard is set once, containing all five paths

### Requirement: One-shot `send` subcommand

`clipboardwire send <FILE>` SHALL connect with the existing client config,
publish the file as a normal eager chunk stream, flush all queued chunks, and
exit. It SHALL NOT reconnect and SHALL NOT accept inbound transfers.

#### Scenario: Scripted transfer

- **WHEN** `clipboardwire send ./report.pdf` runs while a tray client is
  connected
- **THEN** the tray client saves the file and the command exits 0
