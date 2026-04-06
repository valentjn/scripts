# Copyright (C) 2026 Julian Valentin
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""Main backup logic."""

import os
import re
from asyncio import StreamReader, create_subprocess_exec, gather, to_thread, wait_for
from datetime import UTC, datetime
from itertools import groupby
from logging import getLogger
from pathlib import Path
from shlex import join, quote
from subprocess import PIPE, STDOUT
from typing import TYPE_CHECKING, cast

from .config import DEFAULT_BACKUP_COUNT, DEFAULT_COMPRESSION, Compression, Configuration, SSHDirectory

if TYPE_CHECKING:
    from asyncio.subprocess import Process
    from collections.abc import Sequence

    from .config import BackupConfiguration

_logger = getLogger(__name__)


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
