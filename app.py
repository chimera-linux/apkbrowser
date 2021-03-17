import os
import sqlite3
import configparser
from math import ceil

from flask import Flask, render_template, redirect, url_for, g, request, abort

app = Flask(__name__)
application = app

config = configparser.ConfigParser()
config.read("config.ini")


def get_branches():
    return config.get('repository', 'branches').split(',')


def get_arches():
    return config.get('repository', 'arches').split(',')


def get_repos():
    return config.get('repository', 'repos').split(',')


def get_settings():
    return {
        "distro_name": config.get('branding', 'name'),
        "logo": config.get('branding', 'logo'),
        "favicon": config.get('branding', 'favicon'),
        "flagging": config.get('settings', 'flagging') == 'yes',
        "external_wiki": config.get('external', 'wiki'),
        "external_mirrors": config.get('external', 'mirrors'),
    }


def open_databases():
    db = {}
    db_dir = config.get('database', 'path')
    for branch in config.get('repository', 'branches').split(','):
        db_file = os.path.join(db_dir, f"aports-{branch}.db")
        db[branch] = sqlite3.connect(db_file)

    g._db = db


def get_maintainers(branch):
    db = getattr(g, '_db', None)
    if db is None:
        open_databases()
        db = getattr(g, '_db', None)
    cur = db[branch].cursor()
    cur.execute("SELECT name FROM maintainer")
    result = cur.fetchall()
    return map(lambda x: x[0], result)


def get_filter(name, arch, repo, maintainer, origin):
    filter_fields = {
        "packages.name": name,
        "packages.arch": arch,
        "packages.repo": repo,
        "maintainer.name": maintainer,
    }
    glob_fields = ["packages.name"]

    where = []
    args = []
    for key in filter_fields:
        if filter_fields[key] == "" or filter_fields[key] is None:
            continue
        if key == 'packages.name' and ':' in filter_fields[key]:
            where.append("provides.name = ?")
        elif key in glob_fields:
            where.append("{} GLOB ?".format(key))
        else:
            where.append("{} = ?".format(key))
        args.append(str(filter_fields[key]))
    if origin is not None and origin:
        where.append("packages.origin = packages.name")
    if len(where) > 0:
        where = "WHERE " + " AND ".join(where)
    else:
        where = ""
    return where, args


def get_num_packages(branch, name=None, arch=None, repo=None, maintainer=None, origin=None):
    db = getattr(g, '_db', None)
    if db is None:
        open_databases()
        db = getattr(g, '_db', None)

    where, args = get_filter(name, arch, repo, maintainer, origin)

    pjoin = ''
    if name is not None and ':' in name:
        pjoin = 'LEFT JOIN provides ON provides.pid = packages.id'

    sql = """
    SELECT count(*) as qty
    FROM packages
    LEFT JOIN maintainer ON packages.maintainer = maintainer.id
    {}
    {}
    """.format(pjoin, where)

    cur = db[branch].cursor()
    cur.execute(sql, args)
    result = cur.fetchone()
    return result[0]


def get_packages(branch, offset, name=None, arch=None, repo=None, maintainer=None, origin=None):
    db = getattr(g, '_db', None)
    if db is None:
        open_databases()
        db = getattr(g, '_db', None)

    where, args = get_filter(name, arch, repo, maintainer, origin)

    pjoin = ''
    if name is not None and ':' in name:
        pjoin = 'LEFT JOIN provides ON provides.pid = packages.id'

    sql = """
    SELECT packages.*, datetime(packages.build_time, 'unixepoch') as build_time,
        maintainer.name as mname, maintainer.email as memail,
        datetime(flagged.created, 'unixepoch') as flagged
    FROM packages
    LEFT JOIN maintainer ON packages.maintainer = maintainer.id
    LEFT JOIN flagged ON packages.origin = flagged.origin
        AND packages.version = flagged.version
        AND packages.repo = flagged.repo
    {}
    {}
    ORDER BY packages.build_time DESC
    LIMIT 50 OFFSET ?
    """.format(pjoin, where)

    cur = db[branch].cursor()
    args.append(offset)
    cur.execute(sql, args)

    fields = [i[0] for i in cur.description]
    result = [dict(zip(fields, row)) for row in cur.fetchall()]
    return result


def get_package(branch, repo, arch, name):
    db = getattr(g, '_db', None)
    if db is None:
        open_databases()
        db = getattr(g, '_db', None)

    sql = """
        SELECT packages.*, datetime(packages.build_time, 'unixepoch') as build_time,
            maintainer.name as mname, maintainer.email as memail,
            datetime(flagged.created, 'unixepoch') as flagged
        FROM packages
        LEFT JOIN maintainer ON packages.maintainer = maintainer.id
        LEFT JOIN flagged ON packages.origin = flagged.origin
            AND packages.version = flagged.version AND packages.repo = flagged.repo
        WHERE packages.repo = ?
            AND packages.arch = ?
            AND packages.name = ?
    """

    cur = db[branch].cursor()
    cur.execute(sql, [repo, arch, name])

    fields = [i[0] for i in cur.description]
    alldata = cur.fetchall()
    if len(alldata) == 0:
        return None
    result = [dict(zip(fields, row)) for row in alldata]
    return result[0]


def get_depends(branch, package_id, arch):
    db = getattr(g, '_db', None)
    if db is None:
        open_databases()
        db = getattr(g, '_db', None)

    sql = """
        SELECT DISTINCT pa.repo, pa.arch, pa.name, MAX(pa.provider_priority)
        FROM depends de
        LEFT JOIN provides pr ON de.name = pr.name
        LEFT JOIN packages pa ON pr.pid = pa.id
        WHERE pa.arch = ? AND de.pid = ?
        GROUP BY pr.name
        ORDER BY pa.name
    """

    cur = db[branch].cursor()
    cur.execute(sql, [arch, package_id])

    fields = [i[0] for i in cur.description]
    result = [dict(zip(fields, row)) for row in cur.fetchall()]
    return result


def get_required_by(branch, package_id, arch):
    db = getattr(g, '_db', None)
    if db is None:
        open_databases()
        db = getattr(g, '_db', None)

    sql = """
        SELECT DISTINCT packages.* FROM provides
        LEFT JOIN depends ON provides.name = depends.name
        LEFT JOIN packages ON depends.pid = packages.id
        WHERE packages.arch = ? AND provides.pid = ?
        ORDER BY packages.name
    """

    cur = db[branch].cursor()
    cur.execute(sql, [arch, package_id])

    fields = [i[0] for i in cur.description]
    result = [dict(zip(fields, row)) for row in cur.fetchall()]
    return result


def get_subpackages(branch, repo, package_id, arch):
    db = getattr(g, '_db', None)
    if db is None:
        open_databases()
        db = getattr(g, '_db', None)

    sql = """
        SELECT DISTINCT packages.* FROM packages
        WHERE repo = ? AND arch = ? AND origin = ?
        ORDER BY packages.name
    """

    cur = db[branch].cursor()
    cur.execute(sql, [repo, arch, package_id])

    fields = [i[0] for i in cur.description]
    result = [dict(zip(fields, row)) for row in cur.fetchall()]
    return result


def get_install_if(branch, package_id):
    db = getattr(g, '_db', None)
    if db is None:
        open_databases()
        db = getattr(g, '_db', None)

    sql = """
        SELECT name, operator, version
        FROM install_if
        WHERE pid = ?
    """

    cur = db[branch].cursor()
    cur.execute(sql, [package_id])

    fields = [i[0] for i in cur.description]
    result = [dict(zip(fields, row)) for row in cur.fetchall()]
    return result


def get_provides(branch, package_id, pkgname):
    db = getattr(g, '_db', None)
    if db is None:
        open_databases()
        db = getattr(g, '_db', None)

    sql = """
        SELECT name, operator, version
        FROM provides
        WHERE pid = ?
            AND name != ?
    """

    cur = db[branch].cursor()
    cur.execute(sql, [package_id, pkgname])
    fields = [i[0] for i in cur.description]
    result = [dict(zip(fields, row)) for row in cur.fetchall()]
    return result


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


@app.route('/')
def index():
    return redirect(url_for("packages"))


@app.route('/packages')
def packages():
    name = request.args.get('name')
    branch = request.args.get('branch')
    repo = request.args.get('repo')
    arch = request.args.get('arch')
    maintainer = request.args.get('maintainer')
    origin = request.args.get('origin')

    page = request.args.get('page')

    form = {
        "name": name if name is not None else "",
        "branch": branch if branch is not None else config.get('repository', 'default-branch'),
        "repo": repo if repo is not None else "",
        "arch": arch if arch is not None else "",
        "maintainer": maintainer if maintainer is not None else "",
        "origin": origin if origin is not None else "",
        "page": int(page) if page is not None else 1
    }

    branches = get_branches()
    arches = get_arches()
    repos = get_repos()
    maintainers = get_maintainers(branch=form['branch'])

    offset = (form['page'] - 1) * 50

    packages = get_packages(branch=form['branch'], offset=offset, name=name, arch=arch, repo=repo,
                            maintainer=maintainer,
                            origin=origin)

    num_packages = get_num_packages(branch=form['branch'], name=name, arch=arch, repo=repo, maintainer=maintainer,
                                    origin=origin)
    pages = ceil(num_packages / 50)

    pag_start = form['page'] - 4
    pag_stop = form['page'] + 3
    if pag_start < 0:
        pag_stop += abs(pag_start)
        pag_start = 0
    pag_stop = min(pag_stop, pages)

    return render_template("index.html",
                           **get_settings(),
                           title="Package index",
                           form=form,
                           branches=branches,
                           arches=arches,
                           repos=repos,
                           maintainers=maintainers,
                           packages=packages,
                           pag_start=pag_start,
                           pag_stop=pag_stop,
                           pages=pages)


@app.route('/package/<branch>/<repo>/<arch>/<name>')
def package(branch, repo, arch, name):
    package = get_package(branch, repo, arch, name)

    if package is None:
        return abort(404)

    package['size'] = sizeof_fmt(package['size'])
    package['installed_size'] = sizeof_fmt(package['installed_size'])

    git_commit = package['commit'].replace('-dirty', '')
    git_url = config.get('external', 'git-commit').format(commit=git_commit, branch=branch, repo=repo, arch=arch,
                                                          name=name, version=package['version'],
                                                          origin=package['origin'])

    repo_url = config.get('external', 'git-repo').format(commit=git_commit, branch=branch, repo=repo, arch=arch,
                                                         name=name, version=package['version'],
                                                         origin=package['origin'])

    build_url = config.get('external', 'build-log').format(commit=git_commit, branch=branch, repo=repo, arch=arch,
                                                           name=name, version=package['version'],
                                                           origin=package['origin'])

    depends = get_depends(branch, package['id'], arch)
    required_by = get_required_by(branch, package['id'], arch)
    subpackages = get_subpackages(branch, repo, package['origin'], arch)
    install_if = get_install_if(branch, package['id'])
    provides = get_provides(branch, package['id'], package['name'])

    return render_template("package.html",
                           **get_settings(),
                           title=name,
                           branch=branch,
                           git_url=git_url,
                           repo_url=repo_url,
                           build_log_url=build_url,
                           num_depends=len(depends),
                           depends=depends,
                           num_required_by=len(required_by),
                           required_by=required_by,
                           num_subpackages=len(subpackages),
                           subpackages=subpackages,
                           install_if=install_if,
                           provides=provides,
                           pkg=package)


if __name__ == '__main__':
    app.run()
