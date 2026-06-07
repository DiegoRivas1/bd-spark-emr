#!/usr/bin/env python3

import re
import sys
import os

doc = os.environ.get("map_input_file", "unknown")

for line in sys.stdin:
    for word in re.findall(r"[a-z]+", line.lower()):
        if len(word) > 2:
            print(f"{word}\t{doc}")
