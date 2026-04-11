#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "pydantic>=2.12.5",
# ]
# ///
# Copyright (C) 2026 Julian Valentin
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""Rename photo and video files according to the timestamp they were taken.

The format for the stems of the new filenames is ``YYYY-MM-DD_HH-MM-SS_HASH``, where ``YYYY-MM-DD_HH-MM-SS`` is the time
in local time when the file was created, and ``HASH`` is the first 8 bytes of the SHA256 hash of the contents of the
file. When run without any arguments, all files in the current directory are processed (non-recursively), and the user
is asked for confirmation before any files are renamed.
"""

import os
import re
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, BooleanOptionalAction, Namespace
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from glob import glob
from hashlib import file_digest
from logging import DEBUG, INFO, basicConfig, getLogger
from pathlib import Path
from shutil import unpack_archive, which
from subprocess import PIPE, run
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Annotated, Any
from urllib.request import urlretrieve

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, RootModel

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping, Sequence

_logger = getLogger(__name__)


_DATETIME_PATTERN = re.compile(
    r"(?P<year>\d+):(?P<month>\d+):(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)"
    r"((?P<timezone_sign>[+-])(?P<timezone_hour>\d+):(?P<timezone_minute>\d+))?",
    flags=re.ASCII,
)
"""Pattern for ExifTool datetimes."""


def validate_exiftool_datetime(value: Any) -> datetime:  # noqa: ANN401
    """Validate a datetime string that has been output by ExifTool.

    :param value: Datetime string.
    :return: Parsed datetime.
    """
    if not isinstance(value, str):
        msg = "datetime must be str"
        raise ValueError(msg)  # noqa: TRY004
    match = _DATETIME_PATTERN.fullmatch(value)
    if match is None:
        msg = "invalid datetime"
        raise ValueError(msg)
    if match["timezone_sign"]:
        offset = timedelta(hours=int(match["timezone_hour"]), minutes=int(match["timezone_minute"]))
        if match["timezone_sign"] == "-":
            offset *= -1
        tzinfo = timezone(offset)
    else:
        tzinfo = None
    return datetime(
        int(match["year"]),
        int(match["month"]),
        int(match["day"]),
        int(match["hour"]),
        int(match["minute"]),
        int(match["second"]),
        tzinfo=tzinfo,
    )


class Metadata(BaseModel):
    """Metadata of a single media file."""

    model_config = ConfigDict(extra="allow")

    source_file: Annotated[Path, Field(validation_alias="SourceFile")]
    """Path of the media file."""
    exif_date_time_original: Annotated[
        datetime | None, Field(validation_alias="EXIF:DateTimeOriginal"), BeforeValidator(validate_exiftool_datetime)
    ] = None
    """``EXIF:DateTimeOriginal`` field."""
    quicktime_creation_date_time: Annotated[
        datetime | None, Field(validation_alias="QuickTime:CreationDate"), BeforeValidator(validate_exiftool_datetime)
    ] = None
    """``QuickTime:CreationDate`` field."""


MetadataList = RootModel[list[Metadata]]
"""List of media metadata."""


def main(string_arguments: Sequence[str] | None = None) -> None:
    """Run main entry point.

    :param string_arguments: String arguments to parse. If ``None``, ``sys.argv[1:]`` is used.
    """
    arguments = parse_arguments(string_arguments)
    basicConfig(format="%(message)s", level=DEBUG if arguments.verbose else INFO)
    old_media_paths = collect_media_paths(arguments.glob_patterns)
    if len(old_media_paths) == 0:
        msg = "no files match the given glob patterns"
        raise RuntimeError(msg)
    if arguments.collect_only:
        for path in old_media_paths:
            _logger.info("%s", path)
        return
    rename_dict = get_rename_dict(old_media_paths, arguments.exiftool_path)
    if len(rename_dict) == 0:
        _logger.info("no renames to perform")
        return
    if arguments.dry_run:
        return
    if (not arguments.force) and (input("Perform renames [y/n]? ").strip().lower() != "y"):
        return
    rename(rename_dict)


def parse_arguments(string_arguments: Sequence[str] | None = None) -> Namespace:
    """Parse program arguments.

    :param string_arguments: String arguments to parse. If ``None``, ``sys.argv[1:]`` is used.
    """
    parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-c",
        "--collect-only",
        default=False,
        action=BooleanOptionalAction,
        help="only log the names of the files that would be processed",
    )
    parser.add_argument(
        "-e",
        "--exiftool",
        dest="exiftool_path",
        metavar="PATH",
        type=Path,
        help="path to ExifTool installation (will be downloaded if not provided and not found on system)",
    )
    parser.add_argument(
        "-f", "--force", default=False, action=BooleanOptionalAction, help="do not ask for confirmation before renaming"
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        default=False,
        action=BooleanOptionalAction,
        help="do not rename anything, just show what would be done",
    )
    parser.add_argument(
        "-v", "--verbose", default=False, action=BooleanOptionalAction, help="log additional information"
    )
    parser.add_argument(
        "glob_patterns",
        metavar="GLOB_PATTERN",
        nargs="*",
        default=["*"],
        help="Glob patterns of the files to rename. ** is supported. The default * renames all files in the current "
        "working directory (non-recursively).",
    )
    return parser.parse_args(string_arguments)


def collect_media_paths(glob_patterns: Sequence[str]) -> list[Path]:
    """Collect paths of all media files matching the given glob patterns.

    :glob_patterns: Sequence of glob patterns to match ``**`` is supported.
    :return: List of paths of media files (``.heic`, ``.jpg``, ``.mov``, ``.png``) matching the glob patterns. The paths
        are sorted for each glob pattern, but the paths of an earlier glob pattern appear earlier in the list than the
        paths of a later glob pattern.
    """
    supported_suffixes = {".heic", ".jpg", ".mov", ".png"}
    media_paths = []
    for glob_pattern in glob_patterns:
        media_paths += sorted(
            make_path_relative(path)
            for p in glob(glob_pattern, recursive=True)  # noqa: PTH207
            if (path := Path(p)).suffix.lower() in supported_suffixes
        )
    return media_paths


def get_rename_dict(old_media_paths: Sequence[Path], exiftool_path: Path | None = None) -> dict[Path, Path]:
    """Process media paths and return dict with mapping to new names.

    :param old_media_paths: Sequence of media paths before renaming.
    :param exiftool_path: Path to ExifTool. Will be downloaded if not provided and not found on system.
    :return: Dict from old media paths to renamed media paths.
    """
    with ExitStack() as exit_stack:
        if exiftool_path is None:
            if exiftool_path_str := which("exiftool"):
                exiftool_path = Path(exiftool_path_str)
            else:
                exiftool_path = exit_stack.enter_context(download_exiftool())
        _logger.info(
            "retrieving metadata of %d file%s with ExifTool",
            len(old_media_paths),
            "" if len(old_media_paths) == 1 else "s",
        )
        metadata_list = get_metadata_list_parallel(old_media_paths, exiftool_path)
    rename_dict = {}
    for old_media_path, metadata in zip(old_media_paths, metadata_list.root, strict=True):
        creation_datetime = get_creation_datetime(metadata)
        if creation_datetime is None:
            continue
        new_media_path = format_media_path(old_media_path, creation_datetime)
        if old_media_path == new_media_path:
            continue
        _logger.info("%s -> %s", old_media_path, make_path_relative(new_media_path, to=old_media_path.parent))
        rename_dict[old_media_path] = new_media_path
    return rename_dict


@contextmanager
def download_exiftool(version: str = "13.55") -> Generator[Path]:
    """Download ExifTool to a temporary directory.

    :return: Generator yielding the path of the ExifTool executable once. The executable is placed in a temporary
        directory, which is cleaned up when the generator resumes.
    """
    if os.name == "nt":
        archive_stem = f"exiftool-{version}_64"
        archive_filename = f"{archive_stem}.zip"
    else:
        archive_stem = f"Image-ExifTool-{version}"
        archive_filename = f"{archive_stem}.tar.gz"
    url = f"https://sourceforge.net/projects/exiftool/files/{archive_filename}/download"
    _logger.info("downloading ExifTool from %s", url)
    with TemporaryDirectory() as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        archive_path = tmp_dir / archive_filename
        urlretrieve(url, archive_path)
        unpack_archive(archive_path, tmp_dir)
        archive_path.unlink()
        if os.name == "nt":
            executable_path = tmp_dir / archive_stem / "exiftool.exe"
            # necessary to avoid having to press the enter key when processing
            executable_path.with_stem("exiftool(-k)").rename(executable_path)
        else:
            executable_path = tmp_dir / archive_stem / "exiftool"
        yield executable_path


def get_metadata_list_parallel(
    media_paths: Sequence[Path], exiftool_path: Path, *, number_of_workers: int = 8
) -> MetadataList:
    """Like :func:`get_metadata_list`, but in parallel using multi-threading.

    :param number_of_workers: Number of parallel workers.
    """
    if not media_paths:
        return MetadataList([])
    number_of_workers = min(number_of_workers, len(media_paths))
    with ThreadPoolExecutor(max_workers=number_of_workers) as executor:
        futures = [
            executor.submit(get_metadata_list, media_paths[worker_idx::number_of_workers], exiftool_path)
            for worker_idx in range(number_of_workers)
        ]
        results = [future.result().root for future in futures]
        return MetadataList(
            [result[path_idx] for path_idx in range(len(results[0])) for result in results if path_idx < len(result)]
        )


def get_metadata_list(media_paths: Sequence[Path], exiftool_path: Path) -> MetadataList:
    """Get metadata of media by running ExifTool.

    :param media_paths: Sequence of media paths.
    :param exiftool_path: Path to ExifTool executable.
    :return: List of media metadata (one entry per file).
    """
    with TemporaryDirectory() as tmp_dir_str:
        path_list_path = Path(tmp_dir_str) / "media_paths.txt"
        path_list_path.write_text("\n".join(str(p) for p in media_paths), encoding="utf-8", newline="\n")
        process = run(
            [exiftool_path, "-groupNames", "-json", "-quiet", "-@", path_list_path],
            check=True,
            encoding="utf-8",
            stdout=PIPE,
            text=True,
        )
    return MetadataList.model_validate_json(process.stdout)


def get_creation_datetime(metadata: Metadata) -> datetime | None:
    """Get creation datetime of media path by parsing ExifTool's output.

    :param metadata: Media metadata of the file as returned by ExifTool.
    :return: Creation datetime or ``None``, if it has no such datetime or if the datetime has an incorrect format.
    """
    if metadata.exif_date_time_original is not None:
        return metadata.exif_date_time_original
    if metadata.quicktime_creation_date_time is not None:
        return metadata.quicktime_creation_date_time
    _logger.info("%s: could not find creation datetime in metadata, skipping", metadata.source_file)
    _logger.debug("%s: corresponding metadata: %s", metadata.source_file, metadata.model_dump_json())
    return None


def format_media_path(old_media_path: Path, creation_datetime: datetime) -> Path:
    """Format renamed media path.

    :param old_media_path: Media path before renaming.
    :param creation_datetime: Creation datetime of media.
    :return: Renamed media path. It has the same directory and suffix as ``old_media_path``, but
        ``YYYY-MM-DD_HH-MM-SS_HASH`` as stem, where the first part is the datetime in local time when the media file was
        created, and ``HASH`` is the first 8 bytes of the SHA-256 hash of the contents of the media file.
    """
    with old_media_path.open("rb") as file:
        hash_ = file_digest(file, "sha256").hexdigest()
    return old_media_path.with_name(
        f"{creation_datetime.strftime('%Y-%m-%d_%H-%M-%S')}_{hash_[:8]}{old_media_path.suffix.lower()}"
    )


def rename(rename_mapping: Mapping[Path, Path]) -> None:
    """Rename files.

    If a file already exists at a renamed path, the source file is removed. This is because the renamed paths include
    the hash of the file, and therefore two files with the same renamed paths can be assumed to be equal.

    :param rename_mapping: Mapping from old media paths to renamed media paths.
    """
    _logger.info("renaming %d file%s", len(rename_mapping), "" if len(rename_mapping) == 1 else "s")
    for old_path, new_path in rename_mapping.items():
        if new_path.is_file():
            old_path.unlink()
        else:
            old_path.rename(new_path)


def make_path_relative(path: Path, *, to: Path | None = None) -> Path:
    """Make the given path relative to another path, if possible.

    :param path: Path to make relative.
    :path to: Path to which ``path`` should be relative to. If ``None``, the current working directory is used.
    :return: ``path`` relative to ``to``. If ``path`` is not related to ``to``, ``path`` is returned unchanged.
    """
    if not to:
        to = Path.cwd()
    path = path.resolve()
    to = to.resolve()
    try:
        return path.relative_to(to)
    except ValueError:
        return path


if __name__ == "__main__":
    main()
