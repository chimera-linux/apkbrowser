import os
import sqlite3
import packaging
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
        "external_website": config.get('external', 'website'),
    }


def open_databases():
    db = {}
    db_dir = config.get('database', 'path')
    for branch in config.get('repository', 'branches').split(','):
        db_file = os.path.join(db_dir, f"cports-{branch}.db")
        db[branch] = sqlite3.connect(db_file)

    g._db = db


def get_db():
    db = getattr(g, '_db', None)
    if db is None:
        open_databases()
        db = getattr(g, '_db', None)
    return db


def get_maintainers(branch):
    db = get_db()
    cur = db[branch].cursor()
    cur.execute("SELECT name FROM maintainer")
    result = cur.fetchall()
    return map(lambda x: x[0], result)


def get_filter(name, arch, repo, maintainer=None, origin=None, file=None, path=None):
    filter_fields = {
        "packages.name": name,
        "packages.arch": arch,
        "packages.repo": repo,
        "maintainer.name": maintainer,
        "files.file": file,
        "files.path": path
    }
    glob_fields = ["packages.name", "files.file", "files.path"]

    where = []
    args = []
    for key in filter_fields:
        if filter_fields[key] == "" or filter_fields[key] is None:
            continue
        if key == 'packages.name':
            where.append("(provides.name GLOB ? OR packages.name GLOB ?)")
            args.append(str(filter_fields[key]))
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
    db = get_db()

    where, args = get_filter(name, arch, repo, maintainer, origin)

    sql = """
    SELECT DISTINCT count(*) as qty
    FROM packages
    LEFT JOIN maintainer ON packages.maintainer = maintainer.id
    LEFT JOIN provides ON provides.pid = packages.id
    {}
    """.format(where)

    cur = db[branch].cursor()
    cur.execute(sql, args)
    result = cur.fetchone()
    return result[0]


def get_packages(branch, offset, name=None, arch=None, repo=None, maintainer=None, origin=None):
    db = get_db()

    where, args = get_filter(name, arch, repo, maintainer, origin)

    sql = """
    SELECT DISTINCT packages.*, datetime(packages.build_time, 'unixepoch') as build_time,
        maintainer.name as mname, maintainer.email as memail,
        datetime(flagged.created, 'unixepoch') as flagged
    FROM packages
    LEFT JOIN maintainer ON packages.maintainer = maintainer.id
    LEFT JOIN flagged ON packages.origin = flagged.origin
        AND packages.version = flagged.version
        AND packages.repo = flagged.repo
    LEFT JOIN provides ON provides.pid = packages.id
    {}
    ORDER BY packages.build_time DESC, packages.name ASC
    LIMIT 50 OFFSET ?
    """.format(where)

    cur = db[branch].cursor()
    args.append(offset)
    cur.execute(sql, args)

    fields = [i[0] for i in cur.description]
    result = [dict(zip(fields, row)) for row in cur.fetchall()]
    return result


def get_package(branch, repo, arch, name):
    db = get_db()

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


def get_num_contents(branch, name=None, arch=None, repo=None, file=None, path=None):
    db = get_db()

    where, args = get_filter(name, arch, repo, file=file, path=path)

    sql = """
        SELECT count(packages.id)
        FROM packages
        JOIN files ON files.pid = packages.id
        {}
    """.format(where)

    cur = db[branch].cursor()
    cur.execute(sql, args)
    result = cur.fetchone()
    return result[0]


def get_contents(branch, offset, file=None, path=None, name=None, arch=None, repo=None):
    db = get_db()

    where, args = get_filter(name, arch, repo, maintainer=None, origin=None, file=file, path=path)

    sql = """
        SELECT packages.repo, packages.arch, packages.name, files.*
        FROM packages
        JOIN files ON files.pid = packages.id
        {}
        ORDER BY files.path, files.file
        LIMIT 50 OFFSET ?
    """.format(where)

    cur = db[branch].cursor()
    args.append(offset)
    cur.execute(sql, args)

    fields = [i[0] for i in cur.description]
    result = [dict(zip(fields, row)) for row in cur.fetchall()]
    return result


def get_depends(branch, package_id, arch):
    db = get_db()

    sql_provides = """
        SELECT de.name, pa.repo, pa.arch, pa.name, pa.provider_priority, de.name as depname, de.version as depver
        FROM depends de
        JOIN provides pr ON de.name = pr.name
        LEFT JOIN packages pa ON pr.pid = pa.id
        WHERE de.pid = ? AND pa.arch = ?
    """

    sql_direct = """
        SELECT de.name, dp.repo, dp.arch, dp.name, dp.provider_priority, de.name as depname
        FROM depends de
        JOIN packages dp ON dp.name = de.name
        WHERE de.pid = ? AND dp.arch = ?
    """

    sql_names = """
        SELECT de.name as depname
        FROM depends de
        WHERE de.pid = ?
    """

    cur = db[branch].cursor()

    cur.execute(sql_provides, [package_id, arch])
    fields = [i[0] for i in cur.description]
    through_provides = [dict(zip(fields, row)) for row in cur.fetchall()]
    provides = {}
    for p in through_provides:
        depn = p['depname']
        if depn in provides:
            pp = provides[depn]
            oldver = packaging.version.parse(pp['depver'])
            newver = packaging.version.parse(pp['newver'])
            if newver > oldver:
                provides[depn] = p
            elif newver == oldver:
                oprio = -1
                nprio = -1
                if pp['provider_priority'] is not None:
                    oprio = pp['provider_priority']
                if p['provider_priority'] is not None:
                    nprio = p['provider_priority']
                if int(nprio) > int(oprio):
                    provides[depn] = p
        else:
            provides[depn] = p

    cur.execute(sql_direct, [package_id, arch])
    fields = [i[0] for i in cur.description]
    direct_dependency = [dict(zip(fields, row)) for row in cur.fetchall()]
    direct = {}
    for p in direct_dependency:
        direct[p['depname']] = p


    cur.execute(sql_names, [package_id])
    fields = [i[0] for i in cur.description]
    all_deps = [dict(zip(fields, row)) for row in cur.fetchall()]

    result = []
    for dep in all_deps:
        name = dep['depname']
        dep = None
        if name in direct:
            dep = direct[name]

        if name in provides and not dep:
            dep = provides[name]

        if dep is None:
            result.append({'name': name})
        else:
            result.append({'name': name, 'target': dep['name'], 'repo': dep['repo'], 'arch': dep['arch']})

    return result


def get_required_by(branch, package_id, arch):
    db = get_db()

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
    db = get_db()

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
    db = get_db()

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
    db = get_db()

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


@app.route('/contents')
def contents():
    file = request.args.get('file')
    path = request.args.get('path')
    name = request.args.get('name')
    branch = request.args.get('branch')
    repo = request.args.get('repo')
    arch = request.args.get('arch')

    page = request.args.get('page')

    form = {
        "file": file if file is not None else "",
        "path": path if path is not None else "",
        "name": name if name is not None else "",
        "branch": branch if branch is not None else config.get('repository', 'default-branch'),
        "repo": repo if repo is not None else config.get('repository', 'default-repo'),
        "arch": arch if arch is not None else "",
        "page": int(page) if page is not None else 1
    }

    branches = get_branches()
    arches = get_arches()
    repos = get_repos()

    offset = (form['page'] - 1) * 50
    if form['name'] == '' and form['file'] == '' and form['path'] == '':
        contents = []
        num_contents = 0
    else:
        contents = get_contents(branch=form['branch'], offset=offset, file=file, path=path, name=name, arch=arch,
                                repo=form['repo'])

        num_contents = get_num_contents(branch=form['branch'], file=file, path=path, name=name, arch=arch, repo=repo)

    pages = ceil(num_contents / 50)

    pag_start = form['page'] - 4
    pag_stop = form['page'] + 3
    if pag_start < 0:
        pag_stop += abs(pag_start)
        pag_start = 0
    pag_stop = min(pag_stop, pages)

    return render_template("contents.html",
                           **get_settings(),
                           title="Package index",
                           form=form,
                           branches=branches,
                           arches=arches,
                           repos=repos,
                           contents=contents,
                           pag_start=pag_start,
                           pag_stop=pag_stop,
                           pages=pages)


@app.route('/package/<branch>/<repo>/<arch>/<name>')
def package(branch, repo, arch, name):
    package = get_package(branch, repo, arch, name)

    if package is None:
        return abort(404)

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
