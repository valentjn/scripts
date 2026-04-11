#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#     "pydantic>=2.12.5",
#     "pyyaml>=6.0.3",
# ]
# requires-python = ">=3.14"
# ///
# Copyright (C) 2026 Julian Valentin
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""Stupid backup tool."""

import asyncio
import os
import re
import subprocess
from argparse import ArgumentParser, Namespace
from asyncio import StreamReader, create_subprocess_exec, gather, to_thread, wait_for
from datetime import UTC, datetime
from enum import StrEnum, auto
from itertools import groupby
from logging import DEBUG, basicConfig, getLogger
from pathlib import Path, PurePosixPath
from shlex import join, quote
from subprocess import PIPE, STDOUT, CalledProcessError
from typing import TYPE_CHECKING, Annotated, ClassVar, Self, cast

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    Field,
    NonNegativeInt,
    PositiveInt,
    field_validator,
    model_validator,
)
from yaml import safe_load

if TYPE_CHECKING:
    from asyncio.subprocess import Process
    from collections.abc import Sequence

_logger = getLogger(__name__)


def check_path_not_empty(value: str) -> str:
    """Check that the supplied path is not empty."""
    if not str(value):
        msg = f"empty path: {value}"
        raise ValueError(msg)
    return value


def check_path_is_dir(value: Path) -> Path:
    """Check that the provided path exists and is a directory."""
    if not value.exists():
        msg = f"directory not found: {value}"
        raise ValueError(msg)
    if not value.is_dir():
        msg = f"path not a directory: {value}"
        raise ValueError(msg)
    return value


type NonEmptyDirectory = Annotated[Path, BeforeValidator(check_path_not_empty), AfterValidator(check_path_is_dir)]
type NonEmptyPurePosixPath = Annotated[PurePosixPath, BeforeValidator(check_path_not_empty)]
type NonEmptyStr = Annotated[str, Field(min_length=1)]


class SSHDirectory(BaseModel):
    """A directory on a remote SSH host."""

    host: NonEmptyStr
    """Hostname."""
    path: NonEmptyPurePosixPath
    """Directory path on the remote host."""
    user: NonEmptyStr | None = None
    """Optional username to supply to SSH (default: configured via ``.ssh/config``)."""
    ssh_options: list[NonEmptyStr] = []
    """Additional options to supply to SSH."""

    VALID_USER_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"[0-9A-Za-z._][0-9A-Za-z._-]*")

    @field_validator("user")
    @classmethod
    def check_user(cls, value: str | None) -> str | None:
        """Check that the username is valid."""
        if value is not None and not cls.VALID_USER_PATTERN.fullmatch(value):
            msg = f"invalid username: {value}"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def check_host(self) -> Self:
        """Check that the host can be reached and the path exists."""
        _logger.info("checking connection to %s without password and existence of %s", self.host, self.path)
        command = [
            *self.ssh_command,
            f"if [ -d {quote(str(self.path))} ]; then echo dir_exists; else echo dir_not_found; fi",
        ]
        try:
            process = subprocess.run(command, capture_output=True, check=True, text=True)
        except CalledProcessError as e:
            msg = (
                f"could not connect to SSH host {self.host} without password, "
                f"stdout: {e.stdout.strip()!r}, stderr: {e.stderr.strip()!r}"
            )
            raise ValueError(msg) from e
        match process.stdout.strip():
            case "dir_exists":
                pass
            case "dir_not_found":
                msg = f"source not found on {self.host}: {self.path}"
                raise ValueError(msg)
            case stdout:
                msg = (
                    f"could not check existence of {self.path} on {self.host}, "
                    f"stdout: {stdout!r}, stderr: {process.stderr.strip()!r}"
                )
                raise ValueError(msg)
        return self

    @property
    def ssh_command(self) -> list[str]:
        """SSH command to connect to the host (including the host, but excluding the path)."""
        host = self.host
        if self.user:
            host = f"{self.user}@{host}"
        # disable password authentication to make sure the whole backup process runs without user interaction
        return ["ssh", "-o", "PasswordAuthentication=no", *self.ssh_options, host]


class Compression(StrEnum):
    """Compression type."""

    NONE = auto()
    """No compression."""
    BZIP2 = auto()
    """bzip2 compression."""
    GZIP = auto()
    """gzip compression."""
    LZMA = auto()
    """LZMA compression."""
    ZSTD = auto()
    """ZStandard compression."""

    @property
    def tar_option(self) -> str | None:
        """``tar`` option the compression type corresponds to."""
        return None if self is Compression.NONE else f"--{self.value}"

    @property
    def suffix(self) -> str:
        """Filename suffix (without ``.tar``) the compression type corresponds to."""
        return {
            Compression.NONE: "",
            Compression.BZIP2: ".bz2",
            Compression.GZIP: ".gzip",
            Compression.LZMA: ".xz",
            Compression.ZSTD: ".zstd",
        }[self]


DEFAULT_COMPRESSION = Compression.ZSTD
"""Default compression if none is specified."""
DEFAULT_BACKUP_COUNT = 2
"""Default number of backups to keep if none is specified."""


class SharedConfiguration(BaseModel):
    """Configuration that can be set on global level as well as for each backup (all items will be merged)."""

    exclude: list[NonEmptyStr] = []
    """List of exclusion patterns (POSIX shell wildcards).

    Anything that matches any of the exclusion patterns will be omitted from the backup archive. Patterns can match in
    subdirectories as well (e.g., ``dir/file`` will exclude all files or directories named ``file`` in all directories
    named ``dir``). To make a pattern match only on top-level, precede it with ``./``. Do not add a trailing slash to
    patterns to match directories; those patterns won't match at all.
    """
    compression: Compression | None = None
    """Compression type to use."""
    tar_options: list[NonEmptyStr] = []
    """Additional options to supply to ``tar``."""
    backup_count: PositiveInt | None = None
    """Number of backups to keep (oldest will be removed)."""

    def merge(self, global_config: Configuration) -> Self:
        """Merge with the specified global configuration.

        The local configuration has precedence.
        """
        return self.model_copy(
            update={
                "exclude": global_config.exclude + self.exclude,
                "compression": global_config.compression or self.compression,
                "tar_options": global_config.tar_options + self.tar_options,
                "backup_count": global_config.backup_count or self.backup_count,
            },
            deep=True,
        )


class BackupConfiguration(SharedConfiguration):
    """A single source/target backup pair."""

    name: NonEmptyStr
    """Name used as target filename prefix and for display purposes."""
    source: NonEmptyDirectory | SSHDirectory
    """Local or remote source directory."""
    target: NonEmptyDirectory
    """Local target directory in which the backup archives will be stored."""
    order: NonNegativeInt = 0
    """Number determining the order in which the backups are performed.

    Backups with a lower index are performed first. Backups with the same index are performed in parallel. By default,
    all backups are performed in parallel.
    """

    INVALID_NAME_WINDOWS_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"[\x00-\x1f<>:\"/\\|?*]")
    """Pattern that Windows filenames must not contain."""
    INVALID_NAME_LINUX_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"[\x00/]")
    """Pattern that POSIX filenames must not contain."""

    @field_validator("name")
    @classmethod
    def check_name(cls, value: str) -> str:
        """Check that the name doesn't contain characters that are illegal for filenames."""
        pattern = cls.INVALID_NAME_WINDOWS_PATTERN if os.name == "nt" else cls.INVALID_NAME_LINUX_PATTERN
        if pattern.search(value):
            msg = f"name contains illegal characters: {value}"
            raise ValueError(msg)
        return value


class Configuration(SharedConfiguration):
    """Configuration for stupid-backup."""

    backups: list[BackupConfiguration]
    """List of backup pairs."""

    @staticmethod
    def from_file(path: Path) -> Configuration:
        """Read and create configuration object from a file."""
        _logger.info("reading configuration: %s", path)
        with path.open(encoding="utf-8") as file:
            return Configuration.model_validate(safe_load(file))


def main(string_arguments: Sequence[str] | None = None) -> None:
    """Run ``main_async()``."""
    asyncio.run(main_async(string_arguments))


async def main_async(string_arguments: Sequence[str] | None = None) -> None:
    """Run main entry point."""
    basicConfig(format="%(message)s", level=DEBUG)
    arguments = parse_arguments(string_arguments)
    config = Configuration.from_file(arguments.config_path)
    await multi_back_up(config)


def parse_arguments(string_arguments: Sequence[str] | None = None) -> Namespace:
    """Parse command line arguments."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("config_path", metavar="CONFIG_PATH", type=Path, help="path to YAML configuration")
    return parser.parse_args(string_arguments)


async def multi_back_up(global_config: Configuration) -> None:
    """Process multiple backup configuration and perform the backups."""
    configs = sorted(global_config.backups, key=lambda c: c.order)
    for order, order_configs in groupby(configs, lambda c: c.order):
        order_configs_list = [c.merge(global_config) for c in order_configs]
        _logger.info(
            "processing all configurations with order %d: %s", order, ", ".join(c.name for c in order_configs_list)
        )
        inputs = await gather(*(prepare_back_up(c) for c in order_configs_list))
        await gather(*(back_up(c, *i) for c, i in zip(order_configs_list, inputs, strict=True)))


async def prepare_back_up(config: BackupConfiguration) -> tuple[list[str], Path, Path]:
    """Process a backup configuration and perform the backup."""
    _logger.info("%s: starting backup", config.name)
    time = datetime.now(tz=UTC)
    backup_command, temporary_archive_path = get_backup_command(config, time)
    await check_path_does_not_exist(temporary_archive_path)
    final_archive_path = get_archive_path(config.name, config.target, config.compression or DEFAULT_COMPRESSION, time)
    await check_path_does_not_exist(final_archive_path)
    return backup_command, temporary_archive_path, final_archive_path


async def back_up(
    config: BackupConfiguration, backup_command: Sequence[str], temporary_archive_path: Path, final_archive_path: Path
) -> None:
    """Process a backup configuration and perform the backup."""
    await run_backup_command(config.name, backup_command, temporary_archive_path)
    await rename(temporary_archive_path, final_archive_path)
    await remove_old_backups(config.name, config.target, config.backup_count or DEFAULT_BACKUP_COUNT)
    _logger.info("%s: backup finished", config.name)


def get_archive_path(
    name: str, target: Path, compression: Compression, time: datetime, *, temporary: bool = False
) -> Path:
    """Get the path to a backup archive to be created."""
    temporary_suffix = "_temporary" if temporary else ""
    return target / f"{name}_{time.strftime('%Y-%m-%d_%H-%M-%S')}{temporary_suffix}.tar{compression.suffix}"


def get_backup_command(config: BackupConfiguration, time: datetime) -> tuple[list[str], Path]:
    """Get the backup command from the specified configuration, and the temporary archive path."""
    temporary_archive_path = get_archive_path(
        config.name, config.target, config.compression or DEFAULT_COMPRESSION, time, temporary=True
    )
    tar_command = [
        "tar",
        "--create",
        f"--directory={config.source if isinstance(config.source, Path) else config.source.path}",
        *(f"--exclude={pattern}" for pattern in config.exclude),
        f"--file={temporary_archive_path if isinstance(config.source, Path) else '-'}",
        # necessary as Windows paths can contain colons, which tar interprets as host separator
        "--force-local",
        "--verbose",
    ]
    if not (isinstance(config.source, Path) and os.name == "nt"):
        # Windows's bsdtar doesn't know --sort
        tar_command.append("--sort=name")
    if option := (config.compression or DEFAULT_COMPRESSION).tar_option:
        tar_command.append(option)
    tar_command += config.tar_options
    tar_command.append(".")
    tar_command_str = join(tar_command)
    if isinstance(config.source, SSHDirectory):
        tar_command_str = (
            f"{join([*config.source.ssh_command, tar_command_str])} > {quote(str(temporary_archive_path))}"
        )
    command = ["bash", "-c", tar_command_str]
    return command, temporary_archive_path


async def check_path_does_not_exist(path: Path) -> None:
    """Check that a path does not already exist."""
    if await to_thread(path.exists):
        msg = f"path already exists: {path}"
        raise FileExistsError(msg)


async def run_backup_command(name: str, command: Sequence[str], temporary_archive_path: Path) -> None:
    """Run the specified backup command."""
    _logger.info("%s: running backup command: %s", name, join(command))
    process = await create_subprocess_exec(command[0], *command[1:], stdout=PIPE, stderr=STDOUT)
    stdout = cast("StreamReader", process.stdout)
    while True:
        try:
            line = await stdout.readline()
        except:
            await terminate_process(process)
            if await to_thread(temporary_archive_path.exists):
                _logger.info("removing unfinished temporary archive: %s", temporary_archive_path)
                await to_thread(temporary_archive_path.unlink)
            raise
        else:
            if not line:
                break
            _logger.debug("%s: %s", name, line.rstrip(b"\r\n").decode())
    exit_code = await process.wait()
    if exit_code != 0:
        msg = f"backup command for {name} returned non-zero exit code {exit_code}"
        raise RuntimeError(msg)
    _logger.info("%s: backup command suceeded", name)


async def terminate_process(process: Process) -> None:
    """Terminate (and kill if necessary) an async subprocess."""
    try:
        await wait_for(process.wait(), 0.2)
    except TimeoutError:
        process.terminate()
        try:
            await wait_for(process.wait(), 0.2)
        except TimeoutError:
            process.kill()


async def rename(source: Path, target: Path) -> None:
    """Rename a file, aborting if the target file already exists."""
    if await to_thread(target.exists):
        msg = f"target already exists: {target}"
        raise FileExistsError(msg)
    await to_thread(source.rename, target)


async def remove_old_backups(name: str, target: Path, backup_count: int) -> None:
    """Remove old backups such that at most ``backup_count`` backups remain."""
    compression_suffixes = "|".join(re.escape(compression.suffix) for compression in Compression)
    pattern = re.compile(
        rf"{re.escape(name)}_\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}-\d{{2}}\.tar(?:{compression_suffixes})",
        flags=re.ASCII,
    )
    paths = list(await to_thread(target.iterdir))
    paths = sorted(p for p in paths if pattern.fullmatch(p.name))
    if len(paths) <= backup_count:
        _logger.info("%s: no old backups to remove", name)
        return
    for path in paths[: len(paths) - backup_count]:
        _logger.info("%s: removing old backup: %s", name, path)
        await to_thread(path.unlink)


if __name__ == "__main__":
    main()
