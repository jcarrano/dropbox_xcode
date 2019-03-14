"""
Dropbox music downloader
========================

This script simultaneously downloads and transcodes to OPUS entire directories
from Dropbox. Only losslessly encoded files in FLAC are re-encoded.

A short side note
-----------------

I spent more time writing this that it would have taken me to download my
entire FLAC collection to an external HDD and then converting it offline. It
is _clearly_ over-engineered and, at the same time, probably lacking in
several areas, but...
...it was fun, and I would not be programming if it was not fun.
"""

__author__ = "Juan I Carrano"
__copyright__ = "Copyright 2019 Juan I Carrano <juan@carrano.com.ar>"
__license__ = "MIT"

import argparse
import subprocess
import datetime
import os
import pathlib
import hashlib
import tempfile
import asyncio
import contextlib
import itertools
import concurrent.futures
import logging
from typing import *

import dropbox
import tqdm
import mutagen

DEFAULT_TOKEN_FILE = pathlib.Path.home().joinpath(".dropbox_xcode_token")

DropboxMeta = Union[dropbox.files.FileMetadata, dropbox.files.FolderMetadata]


class DropboxHash:
    """Implementation of Dropbox's file hash, according to the definition at
    https://www.dropbox.com/developers/reference/content-hash
    This is implemented from scratch meaning Dropbox's own reference
    implementation (that is under Apache 2.0 license) was not used.
    """
    block_size = 4 * 1024 * 1024  # 4 MiB in bytes

    def __init__(self, _data_hash=None, _hash_hash=None):
        self._data = _data_hash or hashlib.sha256()
        self._hash = _hash_hash or hashlib.sha256()
        # Invariant: this can never be zero
        self._pending_bytes = self.block_size

    @property
    def digest_size(self) -> int:
        return self._hash.digest_size

    def update(self, data):
        assert(self._pending_bytes)

        data_mv = memoryview(data)

        while data_mv:
            pending = self._pending_bytes

            this_dataslice = data_mv[:pending]
            bytes_inserted = len(this_dataslice)
            self._data.update(this_dataslice)
            self._pending_bytes = pending - bytes_inserted

            if bytes_inserted and self._pending_bytes == 0:
                self._hash.update(self._data.digest())
                self._pending_bytes = self.block_size
                self._data = hashlib.sha256()

            data_mv = data_mv[pending:]

    def digest(self) -> bytes:
        assert(self._pending_bytes)

        if self._pending_bytes != self.block_size:
            # the copy is so that one can query the digest at any time without
            # affecting the result.
            _hash = self._hash.copy()
            _hash.update(self._data.digest())
        else:
            _hash = self._hash

        return _hash.digest()

    def hexdigest(self) -> str:
        return self.digest().hex()

    def copy(self) -> 'DropboxHash':
        this_copy = DropboxHash(self._data.copy(), self._hash.copy())
        this_copy._pending_bytes = self._pending_bytes

        return this_copy


class SimpleMetadata:
    """Unifies Folder and File metadata, and drops useless properties."""

    MUSIC_EXTENSIONS = [".flac"]

    def __init__(self, dropbox_meta: DropboxMeta, parent_dir=None):
        """Initialize the "reduced" metadata. The parent_dir should be a
        SimpleMetadata for the dropbox folder where this is located. It is used
        to correct dropbox's path case weirdness."""
        self.size = getattr(dropbox_meta, "size", None)
        self.audio_length = getattr(dropbox_meta, "audio_length", None)

        # This works becaus the "path_display" property always has the last
        # component with the correct case, and because dropbox does not care
        # about the case when we ask for a file. Provided the parent has the
        # correct case, the children will, and an inductive process follows.
        _path = pathlib.PurePosixPath(dropbox_meta.path_display)

        if parent_dir:
            if not parent_dir.folder:
                raise ValueError("parent_dir must be a folder")

            if str(parent_dir).lower() != str(_path.parent).lower():
                raise ValueError("parent_dir should be a the direct parent of "
                                 "this path.", parent_dir.path, _path)

            basename = _path.name
            self.path = parent_dir.path.joinpath(basename)
        else:
            self.path = _path

        if not self.folder:
            self.content_hash = dropbox_meta.content_hash
            self.date = dropbox_meta.server_modified

    def __str__(self):
        return str(self.path)

    @property
    def folder(self) -> bool:
        """Return true if the path refers to a directory, false if it is a
        file."""
        return self.size is None

    @property
    def music(self) -> bool:
        """Return True if the file is an audio file of a type that has to be
        transcoded."""
        return self.path.suffix.lower() in self.MUSIC_EXTENSIONS


def mdate(path: pathlib.Path) -> datetime.datetime:
    """Get the modification date of a file as a datetime object."""
    return datetime.datetime.fromtimestamp(path.stat().st_mtime)


def _psize(path: pathlib.Path) -> int:
    """Get the size of a file"""
    return path.stat().st_size


def audio_meta(path: pathlib.Path) -> Tuple[float, int]:
    """Get the duration in seconds and the bitrate of audio file."""
    audioinfo = mutagen.File(path).info
    return audioinfo.length, getattr(audioinfo, "bitrate", None)


SyncSpec = Tuple[SimpleMetadata, pathlib.Path]
MusicSyncSpec = NewType("MusicSyncSpec", SyncSpec)
FileSyncSpec = NewType("FileSyncSpec", SyncSpec)
DirSyncSpec = NewType("DirSyncSpec", SyncSpec)


class SyncLocation:
    """Local directory where files are downloaded."""

    OPUS_EXTENSION = ".opus"

    def __init__(self, dbx: dropbox.dropbox.Dropbox,
                 local_root: pathlib.Path,
                 remote_root: pathlib.PurePosixPath,
                 ignore_timestamps=False):
        self.dbx = dbx
        self.local_root = local_root
        self.remote_root = remote_root
        self.ignore_timestamps = ignore_timestamps

    def destination_of(self, meta: SimpleMetadata) -> pathlib.Path:
        """Return the location where the file identified by the metadata "meta"
        would be saved."""
        raw_dest = self.local_root.joinpath(
                    meta.path.relative_to(self.remote_root))

        return (raw_dest.with_suffix(self.OPUS_EXTENSION) if meta.music
                else raw_dest)

    @staticmethod
    def _hash_local(path: pathlib.Path) -> str:
        """Return the dropbox hash of a local file."""
        hasher = DropboxHash()
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(hasher.block_size)
                if not chunk:
                    break
                hasher.update(chunk)

        return hasher.hexdigest()

    @staticmethod
    def _equality_test():
        """Equality test for files that are not audio files. Test file size,
        followed by Dropbox hash."""
        pass

    @staticmethod
    def _audio_equality_test(meta: SimpleMetadata, dest: pathlib.Path):
        """Equality test for audio files in different formats. This only checks
        that the files have the same length (it tolerates up to 0.5 seconds
        difference."""
        remote_duration = meta.audio_length
        return (False if not remote_duration
                else abs(remote_duration - audio_meta(dest)[0]) <= 0.5)

    def is_synced(self, meta: SimpleMetadata) -> Tuple[bool, pathlib.Path]:
        """Chek if the file is synced at the given root directory.
        Folders are always sinced if they exist.
        For files, the timestamp is checked first (except if ignore_timestamps
        is set). If the local file is newer than the remote, no further tests
        are done and the file is considered synced. If it is older, the size is
        compared and if they match, the dropbox hash is compared next.

        Returns
        -------
        synced: True if synchronized
        destination: the destination path in the local machine.
        """

        dest = self.destination_of(meta)
        if meta.folder:
            synced = dest.is_dir()
        elif dest.is_file():
            synced = ((not self.ignore_timestamps and (mdate(dest) > meta.date))
                      or (_psize(dest) == meta.size
                          and self._hash_local(dest) == meta.content_hash)
                      )
        else:

            synced = False

        return synced, dest

    def _list_folder(self, dir_meta: SimpleMetadata) -> Iterable[SimpleMetadata]:
        result = self.dbx.files_list_folder(str(dir_meta))

        while True:
            yield from (SimpleMetadata(meta, parent_dir=dir_meta)
                        for meta in result.entries)

            if result.has_more:
                result = self.dbx.files_list_folder_continue(result.cursor)
            else:
                break

    def findfiles(self) -> Iterable[SimpleMetadata]:
        """Recursively list all files in a dropbox folder."""

        # We cannot use recursive=True because we must keep track of parent
        # directories
        root_dir = SimpleMetadata(
                    self.dbx.files_get_metadata(str(self.remote_root)))

        yield root_dir

        pending_folders = [root_dir]

        while pending_folders:
            contents = list(self._list_folder(pending_folders.pop()))

            pending_folders.extend(d for d in contents if d.folder)

            yield from contents

    def fileclassify(self, paths: Iterable[SimpleMetadata]) -> Tuple[
                     List[DirSyncSpec], List[FileSyncSpec],
                     List[MusicSyncSpec]]:
        """Given a list of paths, classify it into directories, normal files and
        convertible audio files.

        Returns three sets of dirs, normal_files, convertible_files.
        """

        dirs = []  # type: List[DirSyncSpec]
        normal = []  # type: List[FileSyncSpec]
        convertible = []  # type: List[MusicSyncSpec]

        for meta in paths:
            is_synced, dest = self.is_synced(meta)

            if not is_synced:
                (dirs if meta.folder
                 else (convertible if meta.music
                       else normal)).append((meta, dest))

        return dirs, normal, convertible


# The synchronizer uses asyncio. It would be possible to use concurrent.futures
# with a thread pool executor to explicitly control concurrency. This, however,
# would make it harder to clean up temporary files in case of failure. With
# asyncio a context manager can be used just like in sequential code.
# The downside is that concurrency control must be explicit (using a semaphore.)
# Also, it would not be a good idea to submit the whole set of task at once,
# since they would be started immediately by the loop- in the case of FLAC
# files it would even get to create temporary files before being blocked by the
# semaphore. Thus tasks must be submitted one by one.
# On the bright side, though, this can (and will) be used to implement a sort
# of scheduler, which chooses between downloading normal or music files
# depending on the transcoding load.

class NopSynchronizer:
    """No-operation synchronizer, used for dry-runs.

    Note about directories: for simplicity, directories are NOT synced. Instead,
    for each file F something like "mkdir -p $(dirname F) is done. This means
    that empty directories won't get synced."""
    def __init__(self, dbx: dropbox.dropbox.Dropbox,
                 max_downloads=2, max_transcode=4, loop=None):
        """
        Parameters
        ----------
        dbx: the dropbox connection object
        max_downloads: maximum number of concurrent download operations.
        max_transcode: maximum number of concurrent audio conversions. Set this
            to the number of CPUs for maximum speed.
        loop: an asyncio loop.
        """
        self.dbx = dbx

        self._dl_limiter = asyncio.Semaphore(max_downloads)
        # There is no point in having a semaphore for the transcode limit
        # because we will never wait on that semaphore: we can only schedule
        # new tasks if there is a download slot open. We use a simple counter.
        self._xcode_limiter = max_transcode
        self._loop = None

    @staticmethod
    @contextlib.contextmanager
    def _temp_file(*args, **kwargs):
        yield None

    async def _download(self, meta: SimpleMetadata, dest: pathlib.Path):
        """Download a file from dropbox. Override this method in the "wet run"
        synchronizer."""
        await asyncio.sleep(0.1)

    async def _transcode(self, source: pathlib.Path, dest: pathlib.Path):
        """Transcode an audio file. Override this method in the "wet run"
        synchronizer. """
        await asyncio.sleep(0.2)

    def _ensure_dir(self, dest: pathlib.Path) -> None:
        """Ensure that directory component of the given path exists. Override
        this method in the "wet run" synchronizer. """
        pass

    async def sync_all(self, files: Iterable[FileSyncSpec],
                       music: Iterable[MusicSyncSpec]):
        """Download and convert, and try to schedule tasks so that there is
        always network and CPU utilization."""

        # chaining with repeat allows us to call next() without having to
        # worry about catching the StopIteration exception
        _files = itertools.chain(files, itertools.repeat((None, None)))
        _music = itertools.chain(music, itertools.repeat((None, None)))
        running_tasks = []

        while True:
            await self._dl_limiter.acquire()

            this_music, music_dst = (next(_music) if self._xcode_limiter > 0
                                     else (None, None))

            this_file, file_dst = (next(_files) if not this_music
                                   else (None, None))

            sync_meta = this_music or this_file

            if not sync_meta:
                break

            sync_dest = music_dst or file_dst

            # promise mypy that sync_dest is not Nont
            assert(sync_dest is not None)

            # maybe the next two lines should be a separate function
            self._ensure_dir(sync_dest)
            sync_fn = self.sync_file if this_file else self.sync_music

            running_tasks = [t for t in running_tasks if not t.done()]
            running_tasks.append(asyncio.create_task(
                                 sync_fn(sync_meta, sync_dest)))

        if running_tasks:
            await asyncio.wait(running_tasks)

    async def sync_file(self, meta: SimpleMetadata, dest: pathlib.Path):
        """Download a file (no transcoding). This is just a wrapper for
        _download() that posts to the download semaphore after it is done."""
        try:
            await self._download(meta, dest)
        except dropbox.exceptions.ApiError:
            logging.exception("Failed to download file %s", str(meta))
            raise
        finally:
            self._dl_limiter.release()

    async def sync_music(self, meta: SimpleMetadata, dest: pathlib.Path):
        with self._temp_file(suffix=meta.path.suffix) as fn:
            await self.sync_file(meta, fn)

            assert(self._xcode_limiter)

            # Note that because of cooperative scheduling of asyncio tasks,
            # after the _dl_limiter is relesed, the scheduler will be run
            # immediately, so for a music sync task, it will never see a
            # condition where the download is finished but the limiter has not
            # yet been decreased.
            self._xcode_limiter -= 1
            try:
                await self._transcode(fn, dest)
            finally:
                self._xcode_limiter += 1


class Synchronizer(NopSynchronizer):
    """Real (wet run) synchronizer."""
    def __init__(self, *args, **kwargs):
        self._dl_executor = concurrent.futures.ThreadPoolExecutor(
                                            thread_name_prefix="downloader")
        super().__init__(*args, **kwargs)

    @staticmethod
    @contextlib.contextmanager
    def _temp_file(*args, **kwargs):
        """Context manager wrapper around mkstemp to ensure the file is deleted.
        """
        handle, fn = tempfile.mkstemp(*args, **kwargs)
        try:
            yield fn
        finally:
            os.remove(fn)

    def _ensure_dir(self, dest: pathlib.Path) -> None:
        os.makedirs(dest.parent, exist_ok=True)

    def _download(self, meta: SimpleMetadata, dest: pathlib.Path):
        loop = self._loop or asyncio.get_running_loop()
        return loop.run_in_executor(self._dl_executor,
                                    self.dbx.files_download_to_file,
                                    str(dest),
                                    str(meta.path))

    async def _transcode(self, input: pathlib.Path, output: pathlib.Path,
                         bitrate: int = 160):

        cmdline = ["opusenc", "--quiet", "--music", "--bitrate", str(bitrate),
                   str(input), str(output)]

        process = await asyncio.create_subprocess_exec(
                    *cmdline, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT)

        out, err = await process.communicate()

        if process.returncode:
            logging.error("Error transcoding file: %s.", out)

    @staticmethod
    def check_opusenc() -> bool:
        """Check that opusenc is installed. Avoid wasting the user's time in a
        sync that will fail."""
        try:
            subprocess.run(["opusenc", "-V"], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
            return True
        except FileNotFoundError:
            return False


def set_tempdir(path: Union[pathlib.Path, None]) -> None:
    """Set the tempfile module's tempdir to the user choice, or attempt to
    use a tmpfs."""
    if path:
        tempfile.tempdir = str(path.resolve())
    elif os.name == "posix" and not os.environ.keys() & ("TMPDIR", "TEMP",
                                                         "TMP"):
        tmpfs = pathlib.Path("/run/user/{}".format(os.getuid()))
        if tmpfs.is_dir():
            tempfile.tempdir = str(tmpfs)


class TQDMHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        tqdm.tqdm.write(msg+"\n")


def main():
    parser = argparse.ArgumentParser(
        description="Dropbox music downloader",
        epilog="Note that because a million things can go wrong when syncing "
               "and transcoding, we do not attempt to catch many errors. "
               "Instead, the goal is to be able to resume the operation later."
               "By default, this script tries to use tmpfs at /run/user/$UID "
               "temporary directory.")

    token_source = parser.add_mutually_exclusive_group()
    token_source.add_argument('-t', '--token', help="Dropbox access token.")
    token_source.add_argument('-f', '--token-file',
                              help="Read dropbox token from file. If neither "
                              "-t nor -f are specified, try reading it from "
                              "{}.".format(DEFAULT_TOKEN_FILE),
                              type=argparse.FileType())

    parser.add_argument('-n', '--dry-run', help="List the remote directory and "
                        "what would be done", default=False,
                        action="store_true")

    parser.add_argument('-o', '--output', help="Store files to this output "
                        "directory (defaults to current dir)",
                        default=".", type=pathlib.Path)

    parser.add_argument('--tmpdir', help="Override temporary directory",
                        type=pathlib.Path)

    parser.add_argument('--logfile', help="Save logs to file",
                        type=pathlib.Path)

    parser.add_argument('remote', help="Remote (dropbox) directory to download",
                        type=pathlib.PurePosixPath)

    ns = parser.parse_args()

    if ns.dry_run:
        synch_class = NopSynchronizer
    else:
        synch_class = Synchronizer

    if not ns.dry_run and not synch_class.check_opusenc():
        print("'opusenc' executable not found. Please install opus-tools")
        return 1

    cmdline_token = ns.token or (ns.token_file.read().strip()
                                 if ns.token_file else None)

    token = cmdline_token or (DEFAULT_TOKEN_FILE.read_text().strip()
                              if DEFAULT_TOKEN_FILE.is_file() else None)

    if not token:
        print("ERROR: You must provide a Dropbox API token via the -t/-f "
              "option or by placing it at {}".format(DEFAULT_TOKEN_FILE))
        return 1

    set_tempdir(ns.tmpdir)
    logging.getLogger().addHandler(TQDMHandler())

    if ns.logfile:
        logging.getLogger().addHandler(logging.FileHandler(ns.logfile))

    dbx = dropbox.Dropbox(token)

    destination_dir = (ns.output.joinpath(ns.remote.name)
                       if ns.output.is_dir() else ns.output)

    location = SyncLocation(dbx, destination_dir, ns.remote)

    all_items = tqdm.tqdm(iterable=location.findfiles(), unit="files",
                          desc="Scanning files", total=0)

    _, file_syncs, music_syncs = location.fileclassify(all_items)

    print(len(file_syncs), "regular files out of sync")
    print(len(music_syncs), "audio tracks out of sync")

    file_w_progress = tqdm.tqdm(iterable=file_syncs, unit="files",
                                desc="Regular files")

    music_w_progress = tqdm.tqdm(iterable=music_syncs, unit="tracks",
                                 desc="Music tracks")

    synch = synch_class(dbx)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(synch.sync_all(file_w_progress, music_w_progress))

    print()  # ensure the progress bar does not get deleted
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
