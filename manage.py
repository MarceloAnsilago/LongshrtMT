#!/usr/bin/env python
from __future__ import annotations

import os
import sys


def main():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Django is required to run the web dashboard. "
            "Install it with `pip install django`."
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
