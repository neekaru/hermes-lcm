# LCM SQLite Redesign — Single-Writer Architecture

Status: **IMPLEMENTED (Stage 1 + 2)** — 2026-10-06. Verified against the plugin
test suite. Rollback = restore from `backups/lcm-plugin-*` / `git stash`.

## 1. Diagnosis (fakta dari kode + runtime)

### 1.1 Satu file DB, banyak koneksi penulis
Semua store menunjuk file fisik yang SAMA (`HERMES_HOME/lcm.db`), tapi
masing-masing membuka koneksi sendiri:

| Store | Lock (sebelum) | Isolation |
|---|---|---|
| `MessageStore` (store.py) | `_write_lock` RLock | default (deferred) |
| `SummaryDAG` (dag.py) | `_db_lock` RLock | default |
| `LifecycleStateStore` | `_lock` RLock | `None` (autocommit) |
| `AssertionStore` | `_write_lock` RLock | `None` |
| `QueryViewStore` | `_write_lock` RLock | default |
| `RollupStore` | `_write_lock` RLock | default |
| `VectorStore` | `_write_lock` RLock | `None` |
| `TrajectoryStore` | `_lock` RLock | `None` |

`engine._bind_storage()` membuat **7 koneksi** ke DB yang sama dalam SATU proses,
tiap koneksi `check_same_thread=False`.

**Masalah:** lock-nya **per-store**, bukan per-file. Store A dan Store B bisa
masuk `BEGIN ... COMMIT` bersamaan di file yang sama → dua penulis intra-proses
tanpa serialisasi. Itu interleaving yang menghasilkan korupsi on-disk historis.

### 1.2 Bukti runtime (gateway)
13 fd ke `lcm.db`, 7 di antaranya `(deleted)` — koneksi basi dari swap DB.

### 1.3 Faktor eksternal (penyebab dominan di lapangan)
Foreign holder: sesi `hermes` CLI tertinggal, memegang fd ke `lcm.db (deleted)`
+ copy lama → **penulis kedua lintas-proses**.

## 2. Yang diimplementasikan

### Stage 1 — Lock global per-file (SELESAI)
`sqlite_util.py`:
- `write_lock_for(db_path)` → SATU `RLock` per realpath (registry global).
  `:memory:` dapat lock unik (DB privat).
- `reset_write_locks()` untuk test hygiene.
- `write_transaction(conn, db_path, immediate=True)` → contextmanager: acquire
  lock global → `BEGIN IMMEDIATE` (kalau belum dalam transaksi) → commit/rollback.

Semua store (8 buah) sekarang mengambil `self._write_lock`/`_db_lock`/`_lock`
dari `write_lock_for(self.db_path)` — jadi store manapun pada file yang sama
berbagi lock yang sama. Reader copy (`vector_store` line ~1984,
`retrieval_core` line ~473) TIDAK lagi membuat RLock baru (itu dulu memecah
serialisasi) — mewarisi lock global.

### Stage 2 — BEGIN IMMEDIATE menyeluruh (SELESAI untuk MessageStore)
Semua jalur tulis `MessageStore` dirutekan lewat `write_transaction(...)`:
`append`, `_append_protected_batch`, `reassign_session_messages`,
`delete_session_messages`, `gc_externalized_tool_result`, `pin`, `unpin`,
`normalize_legacy_blank_sources`, `write_metadata_json`. Ini membuat kontensi
lintas-proses langsung terlihat di awal transaksi (dihormati `busy_timeout`),
bukan gagal upgrade di tengah.

Store lain sudah memakai `BEGIN IMMEDIATE` eksplisit (assertion_store,
query_view_store, vector_store, trajectory_store, lifecycle_state, rollup_store)
dan kini berbagi lock global juga.

### Stage 3 — Verifikasi
- `tests/test_single_writer.py` (7 test): lock dibagi lintas store; 8 thread
  menulis lewat 2 store berbeda ke 1 file → `integrity_check=ok`, 0 error.
- Full suite: **3035+ passed**; 2 kegagalan `test_ingest_protection` adalah
  **pre-existing** (terbukti dengan `git stash` — gagal juga di baseline).
- `test_crash_safe_wal.py` diperbarui: `mmap_size=0` + `locking_mode=normal`.

### Stage 4 — Guard
`lcm_guard.py` sudah punya `foreign_holders()` (deteksi penulis non-gateway via
cgroup) + `holders()` + WAL size + FD count + disk. Berjalan senyap = sehat.

## 3. Prinsip desain (tercapai)

1. **Satu penulis per file per proses** — lock global per realpath. ✅
2. **Serialisasi lintas-proses** — WAL + `busy_timeout` + `BEGIN IMMEDIATE`. ✅
3. **Checkpoint hanya penulis tunggal** saat close (RESTART→TRUNCATE). ✅
4. **Gateway = satu-satunya penulis sah**; guard mendeteksi foreign holder. ✅
5. **Fail-fast** saat lock tak didapat / penulis asing (guard alert). ✅

## 4. Risiko & rollback
- Refactor menyentuh 9 file inti + 1 test. Rollback: `git checkout -- <file>` atau
  restore `backups/lcm-plugin-*`.
- Backups: `backups/lcm-plugin-20261005_080904/`, `backups/lcm/guard/`.
