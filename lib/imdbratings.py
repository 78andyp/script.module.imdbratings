"""IMDb ratings from IMDb's ratings dataset, in one local store every add-on using it shares."""

import gzip
import sqlite3
import threading
import time
from email.utils import parsedate_to_datetime
from typing import Callable, Iterable, Iterator, Optional, Tuple
from urllib.request import Request, urlopen

import xbmc
import xbmcvfs

_ADDON_ID = 'script.module.imdbratings'
_DATASET_URL = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
_STORE = 'special://temp/imdbratings.db'
_HEADERS = {'User-Agent': _ADDON_ID, 'Accept-Encoding': 'identity'}
_CHECK_GAP = 15 * 60
_RETRY_GAP = 6 * 3600
_STOP_CHECK_LINES = 10000
_SCHEMA = 1

_local = threading.local()
_next_check = 0.0
_stopped_until = 0.0
# made at import, a Monitor created after Kodi's abort notice never reports it
_monitor = xbmc.Monitor()


class StoreError(Exception):
    """Store failure from lookup: the store can't be read or holds no dataset yet."""


class _Stopped(Exception):
    """Stop request from Kodi or the caller, raised inside a load."""


class _Holder:
    """One thread's store connection, closed when the thread ends."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def __del__(self):
        self.conn.close()


def get_rating(imdb_id: Optional[str]) -> Optional[Tuple[float, int]]:
    """Get the rating and vote count of an IMDb title, updating the store first when it is due."""
    title = _title_number(imdb_id)
    if title is None:
        return None
    conn = _connect()
    if conn is None:
        return None
    if time.time() >= _next_check:
        _update(conn)
    try:
        return _stored(conn, title)
    except sqlite3.Error as exc:
        _log(f'lookup failed: {exc}', xbmc.LOGERROR)
        return None


def lookup(imdb_id: Optional[str]) -> Optional[Tuple[float, int]]:
    """Look up a stored rating without updating; raises StoreError when the store can't answer."""
    title = _title_number(imdb_id)
    if title is None:
        return None
    conn = _open()
    try:
        rating = _stored(conn, title)
        loaded = rating is not None or _meta(conn, 'modified') is not None
    except sqlite3.Error as exc:
        raise StoreError(f'lookup failed: {exc}') from exc
    if not loaded:
        raise StoreError('no dataset loaded yet')
    return rating


def refresh(abort: Optional[Callable[[], bool]] = None,
            on_download: Optional[Callable[[], None]] = None) -> bool:
    """Refresh store when shared check due and no stop lately; True when new data loaded."""
    if time.time() < _stopped_until:
        return False
    conn = _connect()
    return conn is not None and _update(conn, abort, on_download)


def dataset_date() -> Optional[str]:
    """Last-Modified date of the loaded IMDb file; None before a first load or on a store error."""
    conn = _connect()
    if conn is None:
        return None
    try:
        return _meta(conn, 'modified')
    except sqlite3.Error as exc:
        _log(f'date lookup failed: {exc}', xbmc.LOGERROR)
        return None


def _title_number(imdb_id: Optional[str]) -> Optional[int]:
    """Number part of an IMDb title id, or None when it isn't one."""
    if not imdb_id:
        return None
    text = imdb_id.strip()
    if not text.startswith('tt'):
        return None
    digits = text[2:]
    # 18 digits always fit SQLite's 64-bit INTEGER
    if not (digits.isascii() and digits.isdigit()) or len(digits) > 18:
        return None
    return int(digits)


def _stored(conn: sqlite3.Connection, title: int) -> Optional[Tuple[float, int]]:
    """Rating and vote count stored for a title number, or None."""
    row = conn.execute('SELECT rating, votes FROM ratings WHERE id = ?', (title,)).fetchone()
    return (row[0] / 10, row[1]) if row else None


def _connect() -> Optional[sqlite3.Connection]:
    """Connect to the store, logging a failure and returning None instead of raising."""
    try:
        return _open()
    except StoreError as exc:
        _log(str(exc), xbmc.LOGERROR)
        return None


def _open() -> sqlite3.Connection:
    """Open the store once per thread; creates tables, seeds next check time, raises StoreError."""
    global _next_check
    holder = getattr(_local, 'holder', None)
    if holder is not None:
        return holder.conn
    try:
        conn = sqlite3.connect(xbmcvfs.translatePath(_STORE), timeout=5, isolation_level=None)
        conn.execute('PRAGMA journal_mode = WAL')
        _create(conn)
        if not _next_check:
            # once per interpreter, or each new thread waits out a busy store again
            _next_check = _due(conn)
    except sqlite3.Error as exc:
        raise StoreError(f'cannot open the store: {exc}') from exc
    _local.holder = _Holder(conn)
    return conn


def _create(conn: sqlite3.Connection) -> None:
    """Create the tables empty when the store is new or older; raises StoreError on a newer one."""
    if _schema(conn) < _SCHEMA:
        try:
            conn.execute('BEGIN IMMEDIATE')
        except sqlite3.OperationalError:
            # lock held by another add-on's first load, which may have made the tables
            if _schema(conn) < _SCHEMA:
                raise
        else:
            _build(conn)
    if _schema(conn) > _SCHEMA:
        raise StoreError('the store was made by a newer version of the module')


def _build(conn: sqlite3.Connection) -> None:
    """Build empty tables unless another add-on just did, then commit the open transaction."""
    try:
        # another add-on may have created them while this one waited for the lock
        if _schema(conn) < _SCHEMA:
            conn.execute('DROP TABLE IF EXISTS ratings')
            conn.execute('DROP TABLE IF EXISTS meta')
            conn.execute('CREATE TABLE ratings '
                         '(id INTEGER PRIMARY KEY, rating INTEGER, votes INTEGER)')
            conn.execute('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)')
            conn.execute(f'PRAGMA user_version = {_SCHEMA}')
        conn.execute('COMMIT')
    except sqlite3.Error:
        if conn.in_transaction:
            conn.execute('ROLLBACK')
        raise


def _schema(conn: sqlite3.Connection) -> int:
    """Schema version stored in the file."""
    return conn.execute('PRAGMA user_version').fetchone()[0]


def _due(conn: sqlite3.Connection) -> float:
    """Time the next check falls due, past the check gap and any retry wait after a failed load."""
    return max(float(_meta(conn, 'checked') or 0) + _CHECK_GAP, float(_meta(conn, 'retry') or 0))


def _update(conn: sqlite3.Connection, abort: Optional[Callable[[], bool]] = None,
            on_download: Optional[Callable[[], None]] = None) -> bool:
    """Update the ratings when a check is due and the lock is free; True when new data loaded."""
    global _next_check, _stopped_until
    _next_check = time.time() + _CHECK_GAP
    try:
        conn.execute('BEGIN IMMEDIATE')
    except sqlite3.OperationalError:
        _log('store busy, update skipped')
        return False
    loaded = False
    try:
        if _due(conn) <= time.time():
            conn.execute("INSERT OR REPLACE INTO meta VALUES ('checked', ?)", (str(time.time()),))
            loaded = _reload(conn, abort, on_download)
        conn.execute('COMMIT')
        _next_check = _due(conn)
    except _Stopped:
        # rollback drops check stamp too, letting other add-ons check now
        _stopped_until = time.time() + _CHECK_GAP
        _log('load stopped, old ratings kept', xbmc.LOGINFO)
        conn.execute('ROLLBACK')
    except Exception as exc:
        _log(f'update failed: {exc}', xbmc.LOGERROR)
        loaded = False
        if conn.in_transaction:
            conn.execute('ROLLBACK')
    # else the WAL keeps a second copy of the ratings on disk
    conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    return loaded


def _reload(conn: sqlite3.Connection, abort: Optional[Callable[[], bool]],
            on_download: Optional[Callable[[], None]]) -> bool:
    """Reload ratings from a newer IMDb file; True on load, failure sets retry wait, stop raises."""
    modified = _remote_modified()
    if not modified or not _newer(modified, _meta(conn, 'modified')):
        return False

    def stopped() -> bool:
        return _monitor.abortRequested() or (abort is not None and abort())

    if stopped():
        raise _Stopped()
    _log('downloading the ratings dataset', xbmc.LOGINFO)
    if on_download is not None:
        try:
            on_download()
        except Exception as exc:
            _log(f'download callback failed: {exc}', xbmc.LOGERROR)
    loaded = False
    conn.execute('SAVEPOINT reload')
    try:
        # rebuilt in place, another add-on may have the file open
        conn.execute('DELETE FROM ratings')
        request = Request(_DATASET_URL, headers=_HEADERS)
        with urlopen(request, timeout=30) as response, gzip.GzipFile(fileobj=response) as lines:
            rows = _rows(lines, stopped)
            count = conn.executemany('INSERT INTO ratings VALUES (?, ?, ?)', rows).rowcount
        if count <= 0:
            raise ValueError('the dataset held no ratings')
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('modified', ?)", (modified,))
        _log(f'stored {count} ratings', xbmc.LOGINFO)
        loaded = True
    except _Stopped:
        raise
    except Exception as exc:
        _log(f'download failed: {exc}', xbmc.LOGERROR)
        conn.execute('ROLLBACK TO reload')
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('retry', ?)",
                     (str(time.time() + _RETRY_GAP),))
    conn.execute('RELEASE reload')
    return loaded


def _rows(lines: Iterable[bytes], stopped: Callable[[], bool]) -> Iterator[Tuple[int, int, int]]:
    """Title number, rating times ten and votes per line, skipping bad ones; raises on a stop."""
    for number, line in enumerate(lines):
        if number % _STOP_CHECK_LINES == 0 and stopped():
            raise _Stopped()
        fields = line.split(b'\t')
        try:
            yield int(fields[0][2:]), round(float(fields[1]) * 10), int(fields[2])
        except (ValueError, IndexError):
            continue


def _remote_modified() -> Optional[str]:
    """Last-Modified of IMDb's file, or None when the check fails."""
    request = Request(_DATASET_URL, method='HEAD', headers=_HEADERS)
    try:
        with urlopen(request, timeout=10) as response:
            return response.headers.get('Last-Modified')
    except OSError as exc:
        _log(f'dataset check failed: {exc}', xbmc.LOGERROR)
        return None


def _newer(remote: str, stored: Optional[str]) -> bool:
    """Whether IMDb's date is later than the stored one, or differs when either won't parse."""
    if stored is None:
        return True
    try:
        return parsedate_to_datetime(remote) > parsedate_to_datetime(stored)
    except (TypeError, ValueError):
        return remote != stored


def _meta(conn: sqlite3.Connection, key: str) -> Optional[str]:
    """Value stored under a meta key, or None."""
    row = conn.execute('SELECT value FROM meta WHERE key = ?', (key,)).fetchone()
    return row[0] if row else None


def _log(message: str, level: int = xbmc.LOGDEBUG) -> None:
    """Log under the module's id."""
    xbmc.log(f'[{_ADDON_ID}] {message}', level)
