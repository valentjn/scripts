# Copyright (C) 2026 Julian Valentin
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""Module for main entry point."""

from argparse import ArgumentParser, Namespace
from asyncio import run
from logging import DEBUG, basicConfig
from pathlib import Path
from typing import TYPE_CHECKING

import stupid_backup

from .backup import multi_back_up
from .config import Configuration

if TYPE_CHECKING:
    from collections.abc import Sequence


def main(string_arguments: Sequence[str] | None = None) -> None:
    """Run ``main_async()``."""
    run(main_async(string_arguments))


async def main_async(string_arguments: Sequence[str] | None = None) -> None:
    """Run main entry point."""
    basicConfig(format="%(message)s", level=DEBUG)
    arguments = parse_arguments(string_arguments)
    config = Configuration.from_file(arguments.config_path)
    await multi_back_up(config)


def parse_arguments(string_arguments: Sequence[str] | None = None) -> Namespace:
    """Parse command line arguments."""
    parser = ArgumentParser(description=stupid_backup.__doc__)
    parser.add_argument("config_path", metavar="CONFIG_PATH", type=Path, help="path to YAML configuration")
    return parser.parse_args(string_arguments)


if __name__ == "__main__":
    main()
