from __future__ import annotations

import re


def generate_unique_display_name(base_name: str, existing_names: set[str]) -> str:
    """Generate a unique display name using macOS-style duplicate handling.

    If 'file.csv' exists, returns 'file (1).csv', then 'file (2).csv', etc.
    """
    if base_name not in existing_names:
        return base_name

    # Split name and extension
    # Handle cases like "file.csv", "file.tar.gz", "file"
    match = re.match(r"^(.+?)(\.[^.]+)?$", base_name)
    if match:
        name_part = match.group(1)
        ext_part = match.group(2) or ""
    else:
        name_part = base_name
        ext_part = ""

    # Find the next available number
    counter = 1
    while True:
        candidate = f"{name_part} ({counter}){ext_part}"
        if candidate not in existing_names:
            return candidate
        counter += 1
