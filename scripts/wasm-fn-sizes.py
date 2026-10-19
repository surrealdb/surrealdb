#!/usr/bin/env python3
"""Print each function of a wasm module with its body size, largest first.

    scripts/wasm-fn-sizes.py <module.wasm>

Output is tab-separated `<bytes>\t<name>`, one line per defined function, where
<bytes> is the function's entry in the code section (its size prefix and body)
and <name> comes from the module's name section, or `func[<index>]` when the
section has none for it. A summary of the section sizes goes to stderr.

`twiggy` cannot read the modules emcc writes: their imports use the compact
import encoding and declare exception tags. This reads only the import, code
and name sections, and skips the rest by length.
"""

import sys


def leb(b, i):
    result = shift = 0
    while True:
        x = b[i]
        i += 1
        result |= (x & 0x7F) << shift
        shift += 7
        if x < 0x80:
            return result, i


def string(b, i):
    n, i = leb(b, i)
    return b[i : i + n].decode("utf-8", "replace"), i + n


def limits(b, i):
    flags = b[i]
    i += 1
    _, i = leb(b, i)
    if flags & 1:
        _, i = leb(b, i)
    return i


def import_desc(b, i):
    """Skips one import description; answers (next offset, whether it is a function)."""
    kind = b[i]
    i += 1
    if kind == 0:  # function: type index
        _, i = leb(b, i)
        return i, True
    if kind == 1:  # table: reference type, limits
        return limits(b, i + 1), False
    if kind == 2:  # memory: limits
        return limits(b, i), False
    if kind == 3:  # global: value type, mutability
        return i + 2, False
    if kind == 4:  # tag: attribute, type index
        _, i = leb(b, i + 1)
        return i, False
    raise SystemExit(f"unknown import kind {kind:#x} at offset {i - 1:#x}")


def count_imported_functions(b, i):
    count, i = leb(b, i)
    functions = 0
    for _ in range(count):
        _, i = string(b, i)
        field, i = string(b, i)
        # Compact imports: an empty field name then 0x7f followed by a vector of
        # (name, description) items, or 0x7e followed by one description shared
        # by every item and then the vector of names.
        if field == "" and b[i] in (0x7F, 0x7E):
            if b[i] == 0x7E:
                i, is_fn = import_desc(b, i + 1)
                n, i = leb(b, i)
                for _ in range(n):
                    _, i = string(b, i)
                functions += n if is_fn else 0
            else:
                n, i = leb(b, i + 1)
                for _ in range(n):
                    _, i = string(b, i)
                    i, is_fn = import_desc(b, i)
                    functions += is_fn
            continue
        i, is_fn = import_desc(b, i)
        functions += is_fn
    return functions


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <module.wasm>", file=sys.stderr)
        sys.exit(2)
    b = open(sys.argv[1], "rb").read()
    if b[:4] != b"\0asm":
        raise SystemExit(f"{sys.argv[1]}: not a wasm module")
    i = 8
    imported = 0
    bodies = []
    names = {}
    sections = []
    while i < len(b):
        sid = b[i]
        size, i = leb(b, i + 1)
        end = i + size
        label = str(sid)
        if sid == 0:
            label, j = string(b, i)
            if label == "name":
                while j < end:
                    sub = b[j]
                    sub_size, j = leb(b, j + 1)
                    if sub == 1:  # function names
                        k = j
                        n, k = leb(b, k)
                        for _ in range(n):
                            index, k = leb(b, k)
                            names[index], k = string(b, k)
                    j += sub_size
        elif sid == 2:
            imported = count_imported_functions(b, i)
        elif sid == 10:
            n, j = leb(b, i)
            for k in range(n):
                body, after = leb(b, j)
                bodies.append((imported + k, body + after - j))
                j = after + body
        sections.append((label, size))
        i = end
    for index, size in sorted(bodies, key=lambda t: -t[1]):
        print(f"{size}\t{names.get(index, f'func[{index}]')}")
    for label, size in sections:
        print(f"section {label:>12} {size:>11,}", file=sys.stderr)
    print(f"{len(bodies):,} functions, {sum(s for _, s in bodies):,} bytes of code", file=sys.stderr)


if __name__ == "__main__":
    main()
