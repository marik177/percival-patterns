import hashlib
from pathlib import Path
import os
import shutil

BLOCKSIZE = 65536


def hash_file(path: Path):
    hasher = hashlib.sha1()
    with path.open('rb') as file:
        buf = file.read(BLOCKSIZE)
        while buf:
            hasher.update(buf)
            buf = file.read(BLOCKSIZE)
    return hasher.hexdigest()


def sync(source, dest):
    # Walk the source folder and build a dict of filenames and their hashes
    source_hashes = {}
    for folder, _, files in os.walk(source):
        for fn in files:
            source_hashes[hash_file(Path(folder).joinpath(fn))] = fn

    seen = set()
    for folder, _, files in os.walk(dest):
        for fn in files:
            dest_path = Path(dest).joinpath(fn)
            dest_hash = hash_file(dest_path)
            seen.add(dest_hash)

            # if there's a file in target that's not in source, delete it
            if dest_hash not in source_hashes:
                os.remove(dest_path)

            # if there's a file in target that has a different path in source,
            # move it to the correct path
            elif dest_hash in source_hashes and source_hashes[dest_hash] != fn:
                # shutil.move(dest_path, Path(folder) / source_hashes[dest_hash])
                os.rename(dest_path, Path(folder).joinpath(source_hashes[dest_hash]))

    # for every file that appears in source but not target, copy the file to
    # the target

    for source_hash, fn in source_hashes.items():
        if source_hash not in seen:
            shutil.copy(Path(source).joinpath(fn), Path(dest).joinpath(fn))



