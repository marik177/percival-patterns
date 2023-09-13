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
    # imperative shell step 1, gather inputs
    source_hashes = read_paths_and_hashes(source)
    dest_hashes = read_paths_and_hashes(dest)

    # step 2: call functional core
    actions = determine_actions(source_hashes, dest_hashes, source, dest)
    # imperative shell step 3, apply outputs
    for action, *paths in actions:
        if action == 'copy':
            shutil.copy(*paths)
        if action == 'move':
            shutil.move(*paths)
        if action == 'delete':
            os.remove(paths[0])


def read_paths_and_hashes(root):
    hashes = {}
    for folder, _, files in os.walk(root):
        for fn in files:
            hashes[hash_file(Path(folder) / fn)] = fn
    return hashes


def determine_actions(source_hashes, dest_hashes, source_folder, dest_folder):
    for sha, filename in source_hashes.items():
        if sha not in dest_hashes:
            source_path = Path(source_folder) / filename
            dest_path = Path(dest_folder) / filename
            yield 'copy', source_path, dest_path
        elif dest_hashes[sha] != filename:
            olddestpath = Path(dest_folder)/dest_hashes[sha]
            newdesppath = Path(dest_folder)/filename
            yield 'move', olddestpath, newdesppath

    for sha, filename in dest_hashes.items():
        if sha not in source_hashes:
            yield 'delete', Path(dest_folder) / filename




