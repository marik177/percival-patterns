import tempfile
import shutil
from pathlib import Path
from sync import sync, FileSystem


class TestE2E:
    @staticmethod
    def test_when_a_file_exists_in_the_source_but_not_the_destination():
        try:
            source = tempfile.mkdtemp()
            dest = tempfile.mkdtemp()

            content = "I am a very useful file"
            (Path(source) / 'my-file').write_text(content)

            sync(source, dest)
            expected_path = Path(source) / 'my-file'
            assert expected_path.exists()
            assert expected_path.read_text() == content
        finally:
            shutil.rmtree(source)
            shutil.rmtree(dest)

    @staticmethod
    def test_when_a_file_has_been_renamed_in_the_source():
        try:
            source = tempfile.mkdtemp()
            dest = tempfile.mkdtemp()

            content = "I am a very useful file"

            source_path = Path(source) / 'source-filename'
            old_dest_path = Path(dest) / 'dest-filename'
            expected_dest_path = Path(dest) / 'source-filename'

            source_path.write_text(content)
            old_dest_path.write_text(content)

            sync(source, dest)
            assert old_dest_path.exists() is False
            assert expected_dest_path.read_text() == content
        finally:
            shutil.rmtree(source)
            shutil.rmtree(dest)


class FakeFileSystem:
    def __init__(self, paths_hashes):
        self.paths_hashes = paths_hashes
        self.actions = []

    def read(self, path):
        return self.paths_hashes[path]

    def copy(self, src, dest):
        self.actions.append(('copy', src, dest))

    def move(self, src, dest):
        self.actions.append(('move', src, dest))

    def delete(self, dest):
        self.actions.append(('delete', dest))


def test_when_a_file_exists_in_the_source_but_not_the_destination():
    source_hash = {'hash1': 'fn1'}
    dest_hash = {}
    fakers = FakeFileSystem(
        {'/src': source_hash,
         '/dest': dest_hash
         })
    sync('/src', '/dest', filesystem=fakers)

    assert fakers.actions == [('copy', Path('/src/fn1'), Path('/dest/fn1'))]


def test_when_a_file_has_been_renamed_in_the_source():
    source_hash = {'hash1': 'fn1'}
    dest_hash = {'hash1': 'fn2'}
    fakers = FakeFileSystem(
        {
            '/src': source_hash,
            '/dest': dest_hash,
        }
    )
    sync('/src', '/dest', filesystem=fakers)

    assert fakers.actions == [('move', Path('/dest/fn2'), Path('/dest/fn1'))]
