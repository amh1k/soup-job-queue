#!/usr/bin/env python3
"""Archive raw experiment evidence without changing original runs or old archives."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import tarfile
import tempfile


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def verify(archive, run):
    expected = {p.name: p for p in run.iterdir()
                if p.is_file() and p.name not in {'manifest.json', 'operations.json'}}
    with tarfile.open(archive, 'r:gz') as tar:
        members = tar.getmembers()
        if {m.name for m in members} != {f'{run.name}/{n}' for n in expected}:
            raise ValueError(f'{archive}: archive membership differs from local run')
        for member in members:
            if not member.isfile():
                raise ValueError(f'{archive}: non-file member')
            with tar.extractfile(member) as stream:
                actual = hashlib.file_digest(stream, 'sha256').hexdigest()
            if actual != digest(expected[Path(member.name).name]):
                raise ValueError(f'{archive}: content differs: {member.name}')


def package(root):
    out = root / 'archives'
    out.mkdir(exist_ok=True)
    index = out / 'sha256.json'
    hashes = json.loads(index.read_text()) if index.exists() else {}
    for run in sorted(root.iterdir()):
        if not run.is_dir() or not (run / 'events.jsonl').exists():
            continue
        target = out / (run.name + '.tar.gz')
        if target.exists():
            if target.name not in hashes or hashes[target.name] != digest(target):
                raise ValueError(f'{target}: missing or mismatched recorded checksum')
            verify(target, run)
        else:
            fd, temporary = tempfile.mkstemp(prefix='.archive-', dir=out)
            try:
                with os.fdopen(fd, 'wb') as raw:
                    with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as gz:
                        with tarfile.open(fileobj=gz, mode='w') as tar:
                            for path in sorted(run.iterdir()):
                                if path.is_symlink():
                                    raise ValueError(f'{path}: symlink not allowed')
                                if path.is_file() and path.name not in {'manifest.json', 'operations.json'}:
                                    tar.add(path, arcname=run.name + '/' + path.name, recursive=False)
                    raw.flush()
                    os.fsync(raw.fileno())
                verify(Path(temporary), run)
                os.link(temporary, target)  # Fail rather than overwrite another archive.
                hashes[target.name] = digest(target)
            finally:
                Path(temporary).unlink(missing_ok=True)
        # Save after each run so a later failure doesn't orphan earlier archives.
        fd, temporary = tempfile.mkstemp(prefix='.checksums-', dir=out)
        try:
            with os.fdopen(fd, 'w') as stream:
                json.dump(hashes, stream, indent=2)
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, index)
        finally:
            Path(temporary).unlink(missing_ok=True)
        print(f'Verified {target}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('results'))
    args = parser.parse_args()
    package(args.root)
