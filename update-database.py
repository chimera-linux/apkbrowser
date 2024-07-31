import os
import io
import sys
import sqlite3
import pathlib
import configparser
import subprocess
import time
from email.utils import parseaddr

config = configparser.ConfigParser()
config.read("config.ini")


def get_file(url):
    if url.startswith("file://"):
        try:
            with open(url.removeprefix("file://"), "rb") as inf:
                return (200, inf.read())
        except FileNotFoundError:
            return (404, None)
        except Exception:
            return (500, None)
    # actual url
    import requests

    req = requests.get(url)
    if req.status_code == 200:
        return (200, req.content)
    else:
        return (req.status_code, None)


def dump_adb(adbc, rootn=None):
    apk_bin = config.get("settings", "apk", fallback="apk")
    sp = subprocess.run(
        [apk_bin, "adbdump", "/dev/stdin"], input=adbc, capture_output=True
    )
    if sp.returncode != 0:
        return None
    # root is a dict
    adb = {}
    adbstack = [(adb, None)]
    depth = 0
    # whether we're in the section we need
    insect = not rootn
    # read line by line
    for ln in io.BytesIO(sp.stdout):
        ol = ln
        if ln.startswith(b"#"):
            continue
        olen = len(ln)
        ln = ln.lstrip()
        # check current depth
        cdepth = (olen - len(ln)) / 2
        # bail out if it's irrelevant to us
        if rootn:
            if cdepth == 0:
                insect = ln.startswith(rootn)
            elif not insect:
                continue
        # we might not be inside the current structure anymore
        for i in range(int(depth - cdepth)):
            # decode long strings
            if isinstance(adbstack[-1][0], bytearray):
                adbstack[-2][0][adbstack[-1][1]] = adbstack[-1][0].decode(
                    errors="ignore"
                )
            adbstack.pop()
            depth -= 1
        # if we are in a string, append the original line to it, minus depth
        if isinstance(adbstack[-1][0], bytearray):
            adbstack[-1][0].extend(ol[depth * 2 :])
            continue
        # get the topmost structure
        st = adbstack[-1][0]
        # now parse
        ln = ln.rstrip()
        if ln.startswith(b"- "):
            # list item
            if not isinstance(st, list):
                return None
            ln = ln.removeprefix(b"- ")
            # there may be a dict or string as the list element
            if ln.endswith(b":") or ln.find(b": ") > 0:
                # this is possibly ambiguous
                nst = {}
                st.append(nst)
                adbstack.append((nst, len(st) - 1))
                st = nst
                depth += 1
                # from here we treat it like if it wasn't a list item
            elif ln == b"|":
                nst = bytearray()
                st.append(nst)
                adbstack.append((nst, len(st) - 1))
                st = nst
                depth += 1
                continue
            else:
                st.append(ln.decode(errors="replace"))
                continue
        # not a list item, so get key and value
        if not isinstance(st, dict):
            return None
        kend = ln.find(b":")
        if kend < 0:
            return None
        key = ln[0:kend].decode()
        val = ln[kend + 1 :].lstrip()
        # no value means we are starting a new dict
        if len(val) == 0:
            nst = {}
            st[key] = nst
            adbstack.append((nst, key))
            depth += 1
            continue
        # a list
        if val.startswith(b"#") and val.endswith(b"items"):
            nst = []
            st[key] = nst
            adbstack.append((nst, key))
            depth += 1
            continue
        # a multiline string
        if val == b"|":
            nst = bytearray()
            st[key] = nst
            adbstack.append((nst, key))
            depth += 1
            continue
        # plain value
        st[key] = val.decode(errors="replace")
    # done
    return adb


def set_options(db):
    cur = db.cursor()
    cur.execute("PRAGMA journal_mode = WAL")


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
                'unique_id' TEXT,
                'size' TEXT,
                'installed_size' TEXT,
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
        """,
    ]

    fields = ["provides", "depends", "install_if"]
    for field in fields:
        schema += [
            f"""
                CREATE TABLE IF NOT EXISTS '{field}' (
                    'name' TEXT,
                    'version' TEXT,
                    'operator' TEXT,
                    'pid' INTEGER REFERENCES packages(id) ON DELETE CASCADE
                )
            """,
            f"CREATE INDEX IF NOT EXISTS '{field}_name' on {field} (name)",
            f"CREATE INDEX IF NOT EXISTS '{field}_pid' on {field} (pid)",
        ]

    for sql in schema:
        cur.execute(sql)


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
    operators = [">=", "<=", "><", "=", ">", "<", "~=", "=~", "~"]
    for op in operators:
        if op in package:
            part = package.split(op)
            return part[0], op, part[1]
    return package, None, None


def get_file_list(url):
    print(f"getting file list for {url}")
    rescode, rescontent = get_file(url)
    if not rescontent:
        rescontent = b""
    adbc = dump_adb(rescontent, b"paths:")
    result = []
    if not adbc:
        return result
    if "paths" in adbc:
        for p in adbc["paths"]:
            if "files" in p:
                for f in p["files"]:
                    if "name" not in p:
                        result.append(f"/{f['name']}")
                    else:
                        result.append(f"/{p['name']}/{f['name']}")
    return result


def add_packages(db, branch, repo, arch, packages, changed):
    cur = db.cursor()
    for pkg in changed:
        print(f"adding {pkg}")
        package = packages[pkg]
        if "maintainer" in package:
            maintainer_id = ensure_maintainer_exists(db, package["maintainer"])
        else:
            maintainer_id = None

        sql = """
            INSERT INTO 'packages' (
                name, version, description, url, license, arch,
                repo, unique_id, size, installed_size, origin,
                maintainer, build_time, "commit", provider_priority
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur.execute(
            sql,
            [
                package["name"],
                package["version"],
                package["description"],
                package["url"],
                package["license"],
                package["arch"],
                repo,
                package["unique-id"],
                package["file-size"],
                package["installed-size"],
                package["origin"],
                maintainer_id,
                package["build-time"],
                package.get("repo-commit", "unknown"),
                package.get("provider-priority", None),
            ],
        )
        pid = cur.lastrowid

        for provide in package.get("provides", []):
            name, operator, ver = parse_version_operator(provide)
            sql = """
                INSERT INTO provides (name, version, operator, pid) VALUES (?, ?, ?, ?)
            """
            cur.execute(sql, [name, ver, operator, pid])

        for iif in package.get("install-if", []):
            name, operator, ver = parse_version_operator(iif)
            sql = """
                INSERT INTO install_if (name, version, operator, pid) VALUES (?, ?, ?, ?)
            """
            cur.execute(sql, [name, ver, operator, pid])

        for dep in package.get("depends", []):
            name, operator, ver = parse_version_operator(dep)
            sql = """
                INSERT INTO depends (name, version, operator, pid) VALUES (?, ?, ?, ?)
            """
            cur.execute(sql, [name, ver, operator, pid])

        url = config.get("repository", "url")
        apk_url = (
            f'{url}/{branch}/{repo}/{arch}/{package["name"]}-{package["version"]}.apk'
        )
        files = get_file_list(apk_url)
        filerows = []
        for file in files:
            fname = os.path.basename(file)
            fpath = os.path.dirname(file)
            filerows.append([fname, fpath, pid])
        sql = """
            INSERT INTO 'files' (
                "file", "path", "pid"
            )
            VALUES (?, ?, ?)
        """
        cur.executemany(sql, filerows)


def del_packages(db, repo, arch, remove):
    cur = db.cursor()
    for package in remove:
        print(f"removing {package}")
        part = package.split("-")
        name = "-".join(part[:-2])
        ver = "-".join(part[-2:])
        sql = """
            DELETE FROM packages
            WHERE repo = ?
                AND arch = ?
                AND name = ?
                AND version = ?
        """
        cur.execute(sql, [repo, arch, name, ver])
        if cur.rowcount != 1:
            print(f"could not remove {name}={ver} from {repo}/{arch}")


def process_apkindex(db, branch, repo, arch, contents):
    adbc = dump_adb(contents)
    packages = {}

    for p in adbc.get("packages", []):
        packages[f"{p['name']}-{p['version']}"] = p

    sql = """
        SELECT packages.name || '-' || packages.version
        FROM packages
        WHERE repo = ?
            AND arch = ?
    """
    cur = db.cursor()
    cur.execute(sql, [repo, arch])

    local = set(map(lambda x: x[0], cur.fetchall()))
    remote = set(packages.keys())

    add_packages(
        db,
        branch,
        repo,
        arch,
        packages,
        remote - local,
    )
    del_packages(db, repo, arch, local - remote)

    (
        pathlib.Path(
            config.get("settings", "apkindex-cache", fallback="apkindex_cache")
        )
        / f"apkindex_{repo.replace('/', '_')}_{arch}.txt"
    ).unlink(missing_ok=True)


def prune_maintainers(db):
    cur = db.cursor()

    sql = """
        SELECT DISTINCT maintainer
        FROM packages
    """
    cur.execute(sql, [])

    pmaint = set(map(lambda x: x[0], cur.fetchall()))

    sql = """
        SELECT id
        FROM maintainer
    """
    cur.execute(sql, [])

    mmaint = set(map(lambda x: x[0], cur.fetchall()))

    sql = """
        DELETE FROM maintainer
        WHERE id = ?
    """
    for idn in mmaint - pmaint:
        print("DEL", idn)
        cur.execute(sql, [idn])


def generate(branch, archs):
    url = config.get("repository", "url")
    dbp = config.get("database", "path")

    db = sqlite3.connect(
        os.path.join(dbp, f"cports-{branch}.db"),
        # when 3.12, use this instead of isolation_level
        # autocommit=True,
        isolation_level=None,
        timeout=5.0,
    )

    set_options(db)

    cur = db.cursor()
    retries = 0
    while retries < 5:
        try:
            cur.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError as e:
            print(f"it was locked or something: {e}")
            print("waiting 1s...")
            # cumulative with db timeout above when locked
            time.sleep(1)
            retries += 1

    create_tables(db)

    repos = config.get("repository", "repos").split(",")
    if not archs:
        archs = config.get("repository", "arches").split(",")

    for repo in repos:
        for arch in archs:
            apkindex_url = f"{url}/{branch}/{repo}/{arch}/APKINDEX.tar.gz"
            idxstatus, idxcontent = get_file(apkindex_url)
            if idxstatus == 200:
                print(f"parsing {repo}/{arch} APKINDEX")
                process_apkindex(db, branch, repo, arch, idxcontent)
            else:
                print(f"skipping {arch}, {apkindex_url} returned {idxstatus}")

    prune_maintainers(db)

    cur.execute("COMMIT")
    # not autoclosed
    db.close()


if __name__ == "__main__":
    for b in config.get("repository", "branches").split(","):
        generate(b, sys.argv[1:])
