#!/usr/bin/env python3
"""Transactional hardening of existing Academy containers; preserves current state.

Requires a verified state backup, tested shared-state ACLs and deployment approval.
Never starts an additional live worker concurrently with the original container.
"""
import argparse
import http.client
import json
import os
from pathlib import Path
import socket
import sqlite3
from contextlib import closing
import subprocess
import time
import urllib.request

ALLOWED = {
    'amo-academy-webhook-v11-enabled', 'amo-academy-webhook-v12-telegram-dedup',
    'amo-academy-webhook-v13-source-fields', 'amo-academy-webhook-v14-lines-merged',
}

class DockerConnection(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect('/var/run/docker.sock')

def api(method, path, body=None):
    conn = DockerConnection('localhost', timeout=30)
    conn.request(method, path, None if body is None else json.dumps(body),
                 {'Content-Type': 'application/json'})
    response = conn.getresponse()
    data = response.read()
    conn.close()
    if response.status >= 300:
        raise RuntimeError(f'Docker API failed: {method} {path.split("?")[0]} HTTP {response.status}')
    return json.loads(data) if data else None

def inspect(name):
    return api('GET', f'/containers/{name}/json')

def cli(*args):
    subprocess.run(['docker', *args], check=True, stdout=subprocess.DEVNULL)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('container', choices=sorted(ALLOWED))
    parser.add_argument('--backup-dir', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--image', help='Optional verified image with identical application bytes')
    parser.add_argument('--dependency-refresh', action='store_true',
                        help='Replace an already hardened container with a tested dependency image')
    args = parser.parse_args()
    name = args.container
    original = inspect(name)
    assert original['State']['Running']
    assert original['HostConfig']['NetworkMode'] == 'bridge'
    if args.dependency_refresh:
        assert args.image, 'Dependency refresh requires an explicit verified image'
        assert original['Config']['User'] == '65532:65532'
        assert original['HostConfig']['ReadonlyRootfs']
    else:
        assert original['Config']['User'] in ('', '0', 'root')
    assert original['HostConfig']['PortBindings']['8000/tcp'][0]['HostIp'] == '127.0.0.1'
    assert len(original['Mounts']) == 1 and original['Mounts'][0]['Destination'] == '/app/var'
    backup = args.backup_dir / name
    backup.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(backup, 0o700)
    saved = backup / 'container-before.json'
    if saved.exists():
        assert json.loads(saved.read_text())['Id'] == original['Id'], 'Backup belongs to a different container'
    saved.write_text(json.dumps(original))
    saved.chmod(0o600)
    config = dict(original['Config'])
    chosen_image = api('GET', '/images/' + args.image + '/json')['Id'] if args.image else original['Image']
    config['Image'] = chosen_image
    config['User'] = '65532:65532'
    host = dict(original['HostConfig'])
    host['ReadonlyRootfs'] = True
    host['CapDrop'] = ['ALL']
    host['CapAdd'] = []
    host['SecurityOpt'] = list(host.get('SecurityOpt') or [])
    if 'no-new-privileges' not in host['SecurityOpt']:
        host['SecurityOpt'].append('no-new-privileges')
    host['Tmpfs'] = {**(host.get('Tmpfs') or {}), '/tmp': 'rw,noexec,nosuid,size=16m'}
    host['LogConfig'] = {'Type': 'json-file', 'Config': {'max-size': '10m', 'max-file': '5'}}
    config['HostConfig'] = host
    if not args.apply:
        print(f'{name}: preflight passed; no runtime changes')
        return
    suffix = '-deps' if args.dependency_refresh else '-security'
    candidate = name + suffix + '-candidate'
    rollback_name = name + suffix + '-rollback'
    # A stopped worker can safely resume its current queue, but do not interrupt
    # an explicitly leased delivery when a short defer is enough.
    env = dict(value.split('=', 1) for value in original['Config']['Env'])
    queue_path = env.get('ACADEMY_INVITE_OUTBOX_PATH')
    if queue_path:
        relative = Path(queue_path).relative_to('/app/var')
        assert '..' not in relative.parts
        source = Path(original['Mounts'][0]['Source']) / relative
        with closing(sqlite3.connect('file:' + str(source) + '?mode=ro', uri=True)) as db:
            active = db.execute('SELECT count(*) FROM invite_job WHERE lease_until > ?',
                                (time.time(),)).fetchone()[0]
        assert active == 0, 'Active delivery lease; defer this replacement'
    created = api('POST', '/containers/create?name=' + candidate, config)['Id']
    new = inspect(created)
    assert new['Config']['Env'] == original['Config']['Env']
    assert new['Image'] == chosen_image
    assert new['HostConfig']['Binds'] == original['HostConfig']['Binds']
    assert new['HostConfig']['PortBindings'] == original['HostConfig']['PortBindings']
    assert inspect(name)['Id'] == original['Id']
    stopped = renamed = False
    try:
        cli('stop', '-t', '30', name)
        stopped = True
        cli('rename', name, rollback_name)
        renamed = True
        cli('rename', candidate, name)
        cli('start', name)
        port = original['HostConfig']['PortBindings']['8000/tcp'][0]['HostPort']
        for _ in range(30):
            try:
                with urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=2) as response:
                    assert response.status == 200 and json.load(response)['ok']
                break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError('Replacement failed health check')
        current = inspect(name)
        assert current['Config']['Env'] == original['Config']['Env']
        assert current['Image'] == chosen_image
        assert current['Config']['User'] == '65532:65532'
        assert current['HostConfig']['ReadonlyRootfs']
        record = {'id': current['Id'], 'original_id': original['Id'], 'rollback_container': rollback_name,
                  'image': current['Image'], 'health': 200, 'user': current['Config']['User']}
        (backup / 'verified.json').write_text(json.dumps(record))
        (backup / 'rollback.sh').write_text('#!/bin/sh\nset -eu\ndocker stop -t 30 ' + name + '\n'
            + 'docker rename ' + name + ' ' + candidate + '\n'
            + 'docker rename ' + rollback_name + ' ' + name + '\ndocker start ' + name + '\n')
        (backup / 'rollback.sh').chmod(0o700)
        print(f'{name}: health200 UID65532 readonly; verified image and exact env/state retained')
    except Exception:
        # Use the CURRENT mounted queue. Never copy state-before over live data.
        try:
            cli('rm', '-f', created)
        finally:
            if renamed:
                cli('rename', rollback_name, name)
            if stopped:
                cli('start', name)
        raise

if __name__ == '__main__':
    main()
