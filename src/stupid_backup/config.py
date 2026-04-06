# Copyright (C) 2026 Julian Valentin
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""Pydantic models for configuration."""

import os
import re
from enum import StrEnum, auto
from logging import getLogger
from pathlib import Path, PurePosixPath
from shlex import quote
from subprocess import CalledProcessError, run
from typing import Annotated, ClassVar, Self

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
            process = run(command, capture_output=True, check=True, text=True)
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
DEFAULT_BACKUP_COUNT = 3
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
