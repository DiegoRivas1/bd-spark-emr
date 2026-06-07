#!/usr/bin/env python3

import sys

current = None
docs = set()

for line in sys.stdin:
    word, doc = line.strip().split("\t", 1)

    if word == current:
        docs.add(doc)
    else:
        if current:
            print(f"{current}\t{','.join(sorted(docs))}")

        current = word
        docs = {doc}

if current:
    print(f"{current}\t{','.join(sorted(docs))}")