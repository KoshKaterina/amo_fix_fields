"""Read-only local evidence for analytics. Never a public endpoint."""
import argparse
import json
import os
import sqlite3
import time
import tempfile
from pathlib import Path

def records(path):
    db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro',uri=True)
    db.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in db.execute('SELECT entry,submission_hash,accepted_at,delivered_at FROM miniapp_form_events ORDER BY accepted_at,submission_hash')]
    finally:
        db.close()

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--db',required=True)
    parser.add_argument('--export',required=True)
    args=parser.parse_args()
    destination=Path(args.export)
    fd,name=tempfile.mkstemp(prefix='.form-evidence-',dir=destination.parent)
    tmp=Path(name)
    os.fchmod(fd,0o600)
    with os.fdopen(fd,'w') as stream:
        json.dump({'schema':1,'generated_at':int(time.time()),'records':records(args.db)},stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp,destination)
