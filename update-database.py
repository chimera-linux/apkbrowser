import os
import sqlite3
import requests
import configparser
import tarfile
import io
from email.utils import parseaddr


def create_tables(db):
    cur = db.cursor()
    schema = [
        """
        CREATE TABLE IF NOT EXISTS 'packages' (
            'id' INTEGER PRIMARY KEY,
            'name' TEXT,
            'version' TEXT,
            'description' TEXT,
            'url' TEXT,
            'license' TEXT,
            'arch' TEXT,
            'repo' TEXT,
            'checksum' TEXT,
            'size' INTEGER,
            'installed_size' INTEGER,
            'origin' TEXT,
            'maintainer' INTEGER,
            'build_time' INTEGER,
            'commit' TEXT,
            'provider_priority' INTEGER,
            'fid' INTEGER
        )
        """,
        "CREATE INDEX IF NOT EXISTS 'packages_name' on 'packages' (name)",
        "CREATE INDEX IF NOT EXISTS 'packages_maintainer' on 'packages' (maintainer)",
        "CREATE INDEX IF NOT EXISTS 'packages_build_time' on 'packages' (build_time)",
        "CREATE INDEX IF NOT EXISTS 'packages_origin' on 'packages' (origin)",
        """
        CREATE TABLE IF NOT EXISTS 'files' (
            'id' INTEGER PRIMARY KEY,
            'file' TEXT,
            'path' TEXT,
            'pid' INTEGER REFERENCES packages(id) ON DELETE CASCADE
        )
        """,
        "CREATE INDEX IF NOT EXISTS 'files_file' on 'files' (file)",
        "CREATE INDEX IF NOT EXISTS 'files_path' on 'files' (path)",
        "CREATE INDEX IF NOT EXISTS 'files_pid' on 'files' (pid)",
        """
        CREATE TABLE IF NOT EXISTS maintainer (
            'id' INTEGER PRIMARY KEY,
            'name' TEXT,
            'email' TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS 'maintainer_name' on maintainer (name)",
        """
        CREATE TABLE IF NOT EXISTS 'repoversion' (
            'repo' TEXT,
            'arch' TEXT,
            'version' TEXT,
            PRIMARY KEY ('repo', 'arch')
        ) WITHOUT ROWID
        """,
        """
        CREATE TABLE IF NOT EXISTS 'flagged' (
            'origin' TEXT,
            'version' TEXT,
            'repo' TEXT,
            'created' INTEGER,
            'updated' INTEGER,
            'reporter' TEXT,
            'new_version' TEXT,
            'message' TEXT,
            PRIMARY KEY ('origin', 'version', 'repo')
        ) WITHOUT ROWID
        """
    ]

    fields = ["provides", "depends", "install_if"]
    for field in fields:
        schema.append("""
        CREATE TABLE IF NOT EXISTS '{}' (
            'name' TEXT,
            'version' TEXT,
            'operator' TEXT,
            'pid' INTEGER REFERENCES packages(id) ON DELETE CASCADE
        )
        """.format(field))
        schema.append(f"CREATE INDEX IF NOT EXISTS '{field}_name' on {field} (name)")
        schema.append(f"CREATE INDEX IF NOT EXISTS '{field}_pid' on {field} (pid)")

    for sql in schema:
        cur.execute(sql)


def get_local_repo_version(db, repo, arch):
    cur = db.cursor()
    sql = """
    SELECT version
    FROM repoversion
    WHERE repo = ?
        AND arch = ?
    """
    cur.execute(sql, [repo, arch])
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        return ''


def ensure_maintainer_exists(db, maintainer):
    name, email = parseaddr(maintainer)

    if not email:
        return

    sql = """
    INSERT OR REPLACE INTO maintainer ('id', 'name', 'email')
    VALUES (
        (SELECT id FROM maintainer WHERE name=? and email=?),
        ?, ?
    ) 
    """
    cursor = db.cursor()
    cursor.execute(sql, [name, email, name, email])
    return cursor.lastrowid


def parse_version_operator(package):
    operators = ['>=', '<=', '><', '=', '>', '<']
    for op in operators:
        if op in package:
            part = package.split(op)
            return part[0], op, part[1]
    return package, None, None


def add_packages(db, repo, arch, packages):
    cur = db.cursor()
    for pkg in packages:
        print("Adding {}".format(pkg))
        package = packages[pkg]
        if 'm' in package:
            maintainer_id = ensure_maintainer_exists(db, package['m'])
        else:
            maintainer_id = None
        package['k'] = package['k'] if 'k' in package else None

        sql = """
        INSERT INTO 'packages' (name, version, description, url, license, arch, repo, checksum, size, installed_size,
        origin, maintainer, build_time, "commit", provider_priority)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur.execute(sql, [package['P'], package['V'], package['T'], package['U'], package['L'], package['A'], repo,
                          package['C'], package['S'], package['I'], package['o'], maintainer_id, package['t'],
                          package['c'], package['k']])
        pid = cur.lastrowid

        if 'P' in package:
            for provide in package['P']:
                name, operator, ver = parse_version_operator(provide)
                sql = """
                INSERT INTO provides (name, version, operator, pid) VALUES (?, ?, ?, ?)
                """
                cur.execute(sql, [name, ver, operator, pid])

        if 'i' in package:
            for iif in package['i']:
                name, operator, ver = parse_version_operator(iif)
                sql = """
                INSERT INTO install_if (name, version, operator, pid) VALUES (?, ?, ?, ?)
                """
                cur.execute(sql, [name, ver, operator, pid])

        if 'D' in package:
            for dep in package['D']:
                name, operator, ver = parse_version_operator(dep)
                sql = """
                INSERT INTO depends (name, version, operator, pid) VALUES (?, ?, ?, ?)
                """
                cur.execute(sql, [name, ver, operator, pid])


def del_packages(db, repo, arch, remove):
    cur = db.cursor()
    for package in remove:
        print("Removing {}".format(package))
        part = package.split('-')
        name = '-'.join(part[:-1])
        ver = part[-1]
        sql = """
        DELETE FROM packages
        WHERE repo = ?
            AND arch = ?
            AND name = ?
            AND version = ?
        """
        cur.execute(sql, [repo, arch, name, ver])


def clean_maintainers(db):
    pass


def update_local_repo_version(db, repo, arch, version):
    sql = """
    INSERT OR REPLACE INTO repoversion (
        'version', 'repo', 'arch'
    )
    VALUES (?, ?, ?)
    """
    cur = db.cursor()
    cur.execute(sql, [version, repo, arch])


def process_apkindex(db, repo, arch, contents):
    tar_file = io.BytesIO(contents)
    tar = tarfile.open(fileobj=tar_file, mode='r:gz')
    version_file = tar.extractfile('DESCRIPTION')
    version = version_file.read().decode()
    print(version)
    if version == get_local_repo_version(db, repo, arch):
        return

    print("The APKINDEX on the remote server is newer, updating local repository")
    index_file = tar.extractfile('APKINDEX')
    index = io.StringIO(index_file.read().decode() + "\n")
    buffer = {}
    packages = {}
    while True:
        line = index.readline()
        line = line.strip()
        if line == '' and len(buffer) == 0:
            break
        if line == '':
            packages[buffer['P'] + '-' + buffer['V']] = buffer
            buffer = {}
        else:
            key, value = line.split(':', maxsplit=1)
            if key in "Dpi":
                # Depends, Provides and Install-if are multi-value fields
                value = value.split(' ')
            buffer[key] = value

    remote = set(packages.keys())

    sql = """
    SELECT packages.name || '-' || packages.version
    FROM packages
    WHERE repo = ?
        AND arch = ?
    """
    cur = db.cursor()
    cur.execute(sql, [repo, arch])
    local = set(map(lambda x: x[0], cur.fetchall()))

    add = remote - local
    remove = local - remote

    add_packages(db, repo, arch, dict(filter(lambda arg: arg[0] in add, packages.items())))
    del_packages(db, repo, arch, remove)
    clean_maintainers(db)
    update_local_repo_version(db, repo, arch, version)


def generate(config_file, branch):
    config = configparser.ConfigParser()
    config.read(config_file)

    url = config.get('repository', 'url')

    db_path = os.path.join(config.get('database', 'path'), f"aports-{branch}.db")

    db = sqlite3.connect(db_path)
    create_tables(db)
    for repo in config.get('repository', 'repos').split(','):
        for arch in config.get('repository', 'arches').split(','):
            apkindex_url = f'{url}/{branch}/{repo}/{arch}/APKINDEX.tar.gz'
            apkindex = requests.get(apkindex_url)
            if apkindex.status_code == 200:
                print(f"parsing {repo}/{arch} APKINDEX")
                process_apkindex(db, repo, arch, apkindex.content)
            else:
                print("skipping {}, {} returned {}".format(arch, apkindex_url, apkindex.status_code))
    db.commit()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="apkbrowser database generator")
    parser.add_argument('config', help='path to the config file')
    parser.add_argument('branch', help='branch to generate')
    args = parser.parse_args()
    generate(args.config, args.branch)
