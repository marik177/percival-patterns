import tempfile
import shutil
from pathlib import Path
from sync import sync, determine_actions


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


def test_when_a_file_exists_in_the_source_but_not_the_destination():
    source_hash = {'hash1': 'fn1'}
    dest_hash = {}
    actions = determine_actions(source_hash, dest_hash, Path('/src'), Path('/dest'))
    assert list(actions) == [('copy', Path('/src/fn1'), Path('/dest/fn1'))]


def test_when_a_file_has_been_renamed_in_the_source():
    source_hash = {'hash1': 'fn1'}
    dest_hash = {'hash1': 'fn2'}
    actions = determine_actions(source_hash, dest_hash, Path('/src'), Path('/dest'))
    assert list(actions) == [('move', Path('/dest/fn2'), Path('/dest/fn1'))]
