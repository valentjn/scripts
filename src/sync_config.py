#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.15"
# ///
# Copyright (C) 2026 Julian Valentin
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""Synchronize common parts of configuration files.

The script clones the given Git repository into a temporary directory and synchronizes the parts of the given files that
are enclosed by the ``BEGIN_SYNC_CONFIG`` and ``END_SYNC_CONFIG`` markers with the corresponding upstream files in the
repository. Only the parts between the markers are stored in the repository; everything outside the markers is left
untouched. If no markers are present in a file, the whole file is synchronized.

Local files are mapped to upstream files (and vice versa) via their path relative to the home directory (the ``HOME``
environment variable, or ``Path.home()`` if that variable is not set).

Each computer maintains its own branch in the repository, named after the computer. The ``main`` branch contains the
merged state of all computers.

There are two directions:

- ``push`` copies the configuration parts of the local files to the upstream files on the computer branch, commits the
  changes, and merges the computer branch into ``main``.
- ``pull`` merges ``main`` into the computer branch, merges the computer branch into ``main`` (which should be a
  fast-forward merge), and copies the configuration parts of the upstream files to the local files.

If no files are given on the command line, all files currently checked into ``main`` are used. If a local file is
missing, an error is raised.

If a merge results in conflicts and ``--force`` is not given, the user is asked to resolve the conflicts in the
temporary checkout. With ``--force``, conflicts are resolved in favor of the local files when pushing and in favor of
``main`` when pulling. After a successful operation, both ``main`` and the computer branch are pushed to the remote
repository.
"""

import re
import shlex
from argparse import ArgumentParser, BooleanOptionalAction, Namespace, RawDescriptionHelpFormatter
from difflib import unified_diff
from enum import StrEnum, auto
from logging import DEBUG, INFO, basicConfig, getLogger
from os import environ
from pathlib import Path
from socket import gethostname
from subprocess import CompletedProcess, run
from tempfile import TemporaryDirectory
from typing import Final, cast
lazy from collections.abc import Sequence

MAIN_BRANCH: Final[str] = "main"
"""Name of the main branch in the Git repository."""
REMOTE: Final[str] = "origin"
"""Name of the remote in the Git repository."""

WHOLE_FILE_REGEX: Final[re.Pattern[str]] = re.compile(r"\A(?P<config>.*)\z", re.DOTALL)
"""Regular expression for matching the whole file if there are no markers."""
PART_REGEX: Final[re.Pattern[str]] = re.compile(
    r"(?:^[^\n]*BEGIN_SYNC_CONFIG[^\n]*\n)"
    r"(?P<config>(?:.*?\n)?)"
    r"(?:[^\n]*END_SYNC_CONFIG[^\n]*(?:\n|$))",
    re.DOTALL | re.MULTILINE,
)
"""Regular expression for matching configuration parts to be synchronized."""

_logger = getLogger(__name__)


class Direction(StrEnum):
    """Direction in which the configuration parts are synchronized."""

    PUSH = auto()
    """Copy the configuration parts of the local files to the upstream files."""
    PULL = auto()
    """Copy the configuration parts of the upstream files to the local files."""


class GitError(Exception):
    """Exception that occurs when a Git call errors."""

    def __init__(self, message: str, subprocess: CompletedProcess[str]) -> None:
        """Initialize the error.

        :param message: Error message.
        :param subprocess: Completed subprocess.
        """
        super().__init__(f"Git failed: {message}")
        self.add_note(f"exit code:\n{subprocess.returncode}")
        self.add_note(f"stdout:\n{subprocess.stdout}")
        self.add_note(f"stderr:\n{subprocess.stderr}")


def main(string_arguments: Sequence[str] | None = None) -> None:
    """Run main function.

    :param string_arguments: Command line arguments as a list of strings. If ``None``, arguments will be taken from the
        command line.
    """
    arguments = parse_arguments(string_arguments)
    basicConfig(level=DEBUG if arguments.verbose else INFO, format="[%(levelname)s] %(message)s")
    computer_branch = get_computer_branch()
    with TemporaryDirectory() as tmp_dir_str:
        repository_dir = Path(tmp_dir_str) / "repo"
        clone_repository(arguments.remote_url, repository_dir)
        if arguments.direction == Direction.PUSH:
            push(
                repository_dir,
                arguments.paths,
                computer_branch,
                arguments.message,
                force=arguments.force,
                dry_run=arguments.dry_run,
            )
        else:
            pull(repository_dir, arguments.paths, computer_branch, force=arguments.force, dry_run=arguments.dry_run)


def parse_arguments(string_arguments: Sequence[str] | None = None) -> Namespace:
    """Parse program arguments.

    :param string_arguments: String arguments to parse. If ``None``, ``sys.argv[1:]`` is used.
    :return: Parsed arguments.
    """
    parser = ArgumentParser(description=__doc__, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument(
        "-f",
        "--force",
        default=False,
        action=BooleanOptionalAction,
        help=(
            "resolve merge conflicts in favor of the local files when pushing and in favor of the main branch when "
            "pulling, without asking for confirmation (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "-m",
        "--message",
        default="Synchronize configuration files",
        help="commit message to use when pushing changed files (default: %(default)s)",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        default=False,
        action=BooleanOptionalAction,
        help="perform a dry run without making any changes (default: %(default)s)",
    )
    parser.add_argument(
        "-r",
        "--remote",
        dest="remote_url",
        default="git@github.com:valentjn/config.git",
        help="remote URL of Git repository to synchronize with (default: %(default)s)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        default=False,
        action=BooleanOptionalAction,
        help="enable verbose output (default: %(default)s)",
    )
    parser.add_argument(
        "direction",
        type=Direction,
        choices=Direction,
        help="direction in which the configuration parts are synchronized",
    )
    parser.add_argument(
        "paths",
        metavar="PATH",
        nargs="*",
        type=Path,
        help="files and directories to synchronize (default: all files in the main branch)",
    )
    return parser.parse_args(string_arguments)


def get_computer_branch() -> str:
    """Get the name of the computer branch (hostname sanitized for use as a Git branch name).

    :return: Name of the computer branch.
    """
    hostname = gethostname()
    branch_name = re.sub(r"[^a-z0-9._]+", "-", hostname.lower()).strip("-.")
    if not branch_name:
        msg = f"could not determine computer branch from hostname {hostname}"
        raise RuntimeError(msg)
    return branch_name


def clone_repository(remote_url: str, directory: Path) -> None:
    """Clone the given Git repository into the specified directory, and check out the main branch.

    :param remote_url: Remote URL of the Git repository.
    :param directory: Directory to clone the repository into. Must not exist yet.
    """
    _logger.info("cloning %s", remote_url)
    run(["git", "clone", "--quiet", remote_url, directory], check=True)
    if branch_exists(directory, f"{REMOTE}/{MAIN_BRANCH}"):
        run_git(directory, "switch", MAIN_BRANCH)
    else:
        _logger.info("%s branch does not exist, creating it", MAIN_BRANCH)
        run_git(directory, "switch", "--orphan", MAIN_BRANCH)
        run_git(directory, "commit", "--allow-empty", "--message", "Initial commit")


def push(
    repository_dir: Path,
    paths: Sequence[Path],
    computer_branch: str,
    message: str,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> None:
    """Push the configuration parts of the local files to the remote repository.

    :param repository_dir: Path of the cloned Git repository.
    :param paths: Paths of the local files to synchronize. If empty, all files in the main branch are used.
    :param computer_branch: Name of the computer branch.
    :param force: Whether to resolve conflicts in favor of the local files without asking for confirmation.
    :param dry_run: Whether to perform a dry run without making any changes.
    :param message: Commit message to use.
    """
    local_paths = resolve_local_paths(repository_dir, paths)
    if not local_paths:
        _logger.warning("no files to synchronize (the main branch is empty, specify files explicitly)")
        return
    configs = {p: read_local_config(p) for p in local_paths}
    checkout_computer_branch(repository_dir, computer_branch, create_from_root=True)
    changed = False
    for local_path, config in configs.items():
        upstream_path = repository_dir / get_relative_path(local_path)
        changed = (
            update_file(upstream_path, config, local=False, relative_to=repository_dir, dry_run=dry_run) or changed
        )
    if changed:
        run_git(repository_dir, "add", "--all")
        run_git(repository_dir, "commit", "--message", message)
    else:
        _logger.info("no changes to commit")
    run_git(repository_dir, "switch", MAIN_BRANCH)
    merge_branch(repository_dir, computer_branch, force=force)
    push_branches(repository_dir, computer_branch, dry_run=dry_run)


def pull(
    repository_dir: Path, paths: Sequence[Path], computer_branch: str, *, force: bool = False, dry_run: bool = False
) -> None:
    """Pull the configuration parts of the upstream files to the local files.

    :param repository_dir: Path of the cloned Git repository.
    :param paths: Paths of the local files to synchronize. If empty, all files in the main branch are used.
    :param computer_branch: Name of the computer branch.
    :param force: Whether to resolve conflicts in favor of the main branch without asking for confirmation.
    :param dry_run: Whether to perform a dry run without making any changes.
    """
    local_paths = resolve_local_paths(repository_dir, paths)
    if not local_paths:
        _logger.warning("no files to synchronize (the main branch is empty, specify files explicitly)")
        return
    local_files = {p: read_local_file(p) for p in local_paths}
    checkout_computer_branch(repository_dir, computer_branch, create_from_root=False)
    merge_branch(repository_dir, MAIN_BRANCH, force=force)
    run_git(repository_dir, "switch", MAIN_BRANCH)
    subprocess = run_git(repository_dir, "merge", "--ff-only", computer_branch, check=False)
    if subprocess.returncode != 0:
        msg = f"merging {computer_branch} into {MAIN_BRANCH} is not a fast-forward merge"
        raise GitError(msg, subprocess)
    new_local_texts = {
        p: f"{t[: m.start('config')]}{read_upstream_config(repository_dir, p)}{t[m.end('config') :]}"
        for p, (t, m) in local_files.items()
    }
    push_branches(repository_dir, computer_branch, dry_run=dry_run)
    home_directory = get_home_directory()
    for local_path, new_local_text in new_local_texts.items():
        update_file(local_path, new_local_text, local=True, relative_to=home_directory, dry_run=dry_run)


def resolve_local_paths(repository_dir: Path, paths: Sequence[Path]) -> list[Path]:
    """Resolve the local files to synchronize.

    :param repository_dir: Path of the cloned Git repository.
    :param paths: Paths given on the command line. If empty, all files in the main branch are used.
    :return: Paths of the local files to synchronize.
    """
    if paths:
        return [q for p in paths for q in (p.rglob("*") if p.is_dir() else [p]) if q.is_file()]
    output = run_git(repository_dir, "ls-tree", "--name-only", "-r", "main").stdout
    home_directory = get_home_directory()
    return [home_directory / p for p in output.splitlines() if p]


def read_local_config(local_path: Path) -> str:
    """Read the configuration part of a local file.

    :param local_path: Path of the local file.
    :return: Configuration part of the local file (without the begin and end lines).
    """
    return read_local_file(local_path)[1]["config"]


def read_local_file(local_path: Path) -> tuple[str, re.Match[str]]:
    """Read a local file and extract its configuration part.

    :param local_path: Path of the local file.
    :return: Text of the local file and match object of the configuration part.
    """
    if not local_path.is_file():
        msg = f"local file not found: {local_path}"
        raise FileNotFoundError(msg)
    text = local_path.read_text()
    match = PART_REGEX.search(text) or cast("re.Match[str]", WHOLE_FILE_REGEX.search(text))
    return text, match


def read_upstream_config(repository_dir: Path, local_path: Path) -> str:
    """Read the configuration part of the upstream file corresponding to the given local file.

    :param repository_dir: Path of the cloned Git repository.
    :param local_path: Path of the local file.
    :return: Configuration part of the upstream file.
    """
    upstream_path = repository_dir / get_relative_path(local_path)
    if not upstream_path.is_file():
        msg = f"upstream file not found: {upstream_path.relative_to(repository_dir)}"
        raise FileNotFoundError(msg)
    return upstream_path.read_text()


def get_relative_path(local_path: Path) -> Path:
    """Get the path of the upstream file relative to the root of the Git repository.

    :param local_path: Path of the local file.
    :return: Path of the upstream file relative to the root of the Git repository.
    """
    local_path = local_path.resolve()
    home_directory = get_home_directory()
    try:
        relative_path = local_path.relative_to(home_directory)
    except ValueError as e:
        msg = f"{local_path} is not contained in the home directory {home_directory}"
        raise RuntimeError(msg) from e
    return relative_path


def get_home_directory() -> Path:
    """Get the home directory.

    :return: The directory denoted by the ``HOME`` environment variable, or ``Path.home()`` if that variable is not set.
    """
    return Path(environ.get("HOME") or Path.home()).resolve()


def update_file(path: Path, text: str, *, local: bool, relative_to: Path | None = None, dry_run: bool = False) -> bool:
    """Update the given file with the provided text.

    :param path: Path of the file to update.
    :param text: Text to write to the file.
    :param local: Whether the file is a local file.
    :param relative_to: If provided, display the path relative to this directory in the log messages.
    :param dry_run: If ``True``, do not actually write to the file.
    :return: Whether the text differs from the old contents.
    """
    display_local = "local" if local else "remote"
    display_path = (path.relative_to(relative_to) if relative_to else path).as_posix()
    old_content = path.read_text() if path.is_file() else ""
    if old_content == text:
        _logger.info("no changes for %s %s", display_local, display_path)
        return False
    _logger.info("%s %s %s", "would update" if dry_run else "updating", display_local, display_path)
    if _logger.isEnabledFor(DEBUG):
        diff_lines = unified_diff(
            old_content.splitlines(),
            text.splitlines(),
            fromfile=f"a/{display_local}/{display_path}",
            tofile=f"b/{display_local}/{display_path}",
            lineterm="",
            color=True,
        )
        for line in diff_lines:
            _logger.debug(line)
    if not dry_run or not local:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, newline="\n")
    return True


def checkout_computer_branch(repository_dir: Path, computer: str, *, create_from_root: bool) -> None:
    """Check out the computer branch, creating it if it does not exist.

    :param repository_dir: Path of the cloned Git repository.
    :param computer: Name of the computer branch.
    :param create_from_root: Where to create the computer branch from if it does not exist. If ``True``, create it at
        the root commit of the main branch. If ``False``, create it at the main branch.
    """
    if branch_exists(repository_dir, f"{REMOTE}/{computer}"):
        run_git(repository_dir, "switch", computer)
    else:
        base = (
            run_git(repository_dir, "rev-list", "--max-parents=0", MAIN_BRANCH).stdout.split()[-1]
            if create_from_root
            else MAIN_BRANCH
        )
        run_git(repository_dir, "switch", "--create", computer, base)


def branch_exists(repository_dir: Path, ref: str) -> bool:
    """Check whether the given Git reference exists.

    :param repository_dir: Path of the cloned Git repository.
    :param ref: Git reference to check.
    :return: Whether the reference exists.
    """
    return run_git(repository_dir, "rev-parse", "--verify", "--quiet", ref, check=False).returncode == 0


def merge_branch(repository_dir: Path, branch: str, *, force: bool) -> None:
    """Merge the given branch into the currently checked-out branch.

    :param repository_dir: Path of the cloned Git repository.
    :param branch: Branch to merge into the currently checked-out branch.
    :param force: Whether to resolve conflicts in favor of the given branch without asking for confirmation.
    """
    _logger.info("merging %s", branch)
    command = ["merge", "--no-edit"]
    if force:
        command += ["--strategy-option", "theirs"]
    command.append(branch)
    subprocess = run_git(repository_dir, *command, check=False)
    if subprocess.returncode == 0:
        return
    unmerged_paths = get_unmerged_paths(repository_dir)
    if not unmerged_paths:
        msg = f"merging {branch} failed"
        raise GitError(msg, subprocess)
    if force:
        _logger.info("resolving conflicts in favor of the merged branch")
        for path in unmerged_paths:
            if run_git(repository_dir, "restore", "--theirs", "--", path, check=False).returncode == 0:
                run_git(repository_dir, "add", "--", path)
            else:
                run_git(repository_dir, "rm", "--force", "--quiet", "--", path)
        run_git(repository_dir, "commit", "--no-edit")
    else:
        while unmerged_paths := get_unmerged_paths(repository_dir):
            _logger.warning("conflicts in:\n%s", "\n".join(unmerged_paths))
            answer = (
                input(f"Resolve and stage the conflicts in {repository_dir}, then type 'y' to continue: ")
                .strip()
                .lower()
            )
            if answer != "y":
                msg = "merge aborted by user"
                raise RuntimeError(msg)
        run_git(repository_dir, "add", "--all")
        run_git(repository_dir, "commit", "--no-edit")


def get_unmerged_paths(repository_dir: Path) -> list[str]:
    """Get the paths of the files with unresolved merge conflicts.

    :param repository_dir: Path of the cloned Git repository.
    :return: Paths of the files with unresolved merge conflicts.
    """
    output = run_git(repository_dir, "diff", "--name-only", "--diff-filter=U").stdout
    return [L for L in output.splitlines() if L]


def push_branches(repository_dir: Path, computer_branch: str, *, dry_run: bool = False) -> None:
    """Push the main branch and the computer branch to the remote repository.

    :param repository_dir: Path of the cloned Git repository.
    :param computer_branch: Name of the computer branch.
    :param dry_run: Whether to perform a dry run without actually pushing.
    """
    if dry_run:
        _logger.info("would push %s and %s", MAIN_BRANCH, computer_branch)
        return
    _logger.info("pushing %s and %s", MAIN_BRANCH, computer_branch)
    run_git(repository_dir, "push", REMOTE, MAIN_BRANCH, computer_branch)


def run_git(repository_dir: Path, *arguments: str, check: bool = True) -> CompletedProcess[str]:
    """Run a Git command in the given repository.

    :param repository_dir: Path of the Git repository.
    :param arguments: Arguments to pass to Git.
    :param check: Whether to raise an exception if the command fails.
    :return: Result of the command.
    """
    command = ["git", *arguments]
    subprocess = run(command, check=False, cwd=repository_dir, capture_output=True, text=True)
    if check and subprocess.returncode != 0:
        raise GitError(shlex.join(shlex.quote(a) for a in command), subprocess)
    return subprocess


if __name__ == "__main__":
    main()
