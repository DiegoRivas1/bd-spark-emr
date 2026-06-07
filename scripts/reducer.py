#!/usr/bin/env python3

import sys

current = None
count = 0

for line in sys.stdin:
    word, value = line.strip().split("\t")

    if current == word:
        count += int(value)
    else:
        if current:
            print(f"{current}\t{count}")
        current = word
        count = int(value)

if current:
    print(f"{current}\t{count}")
