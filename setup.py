#!/usr/bin/env python
# -*- coding: utf-8 -*-

# {# pkglts, pysetup.kwds
# format setup arguments

from setuptools import setup, find_packages


short_descr = "A package for distributed workflows on cluster, cloud and grid"
readme = open('README.rst').read()
history = open('HISTORY.rst').read()


# find version number in src/openalea/distributed/version.py
version = {}
with open("src/openalea/distributed/version.py") as fp:
    exec(fp.read(), version)

# find packages
pkg_root_dir = 'src'
packages = [pkg for pkg in find_packages(pkg_root_dir)]
top_pkgs = [pkg for pkg in packages if len(pkg.split('.')) <= 2]
package_dir = dict([('', pkg_root_dir)] +
                   [(pkg, pkg_root_dir + "/" + pkg.replace('.', '/'))
                    for pkg in top_pkgs])



setup_kwds = dict(
    name='openalea.distributed',
    version=version["__version__"],
    description=short_descr,
    long_description=readme + '\n\n' + history,
    author="Gaetan",
    author_email="gaetan.heidsieck@inria.fr",
    url='',
    license='cecill-c',
    
    namespace_packages=['openalea'],
    # package installation
    packages=packages,
    package_dir=package_dir,
    zip_safe=False,
    setup_requires=[
        "pytest-runner",
        ],
    install_requires=[
        ],
    tests_require=[
        "pytest",
        "pytest-mock",
        ],
    entry_points={
        "wralea": ["openalea.distributed = openalea.distributed_wralea", ],
    },
    keywords='',
    include_package_data=True,
    )
# #}
# change setup_kwds below before the next pkglts tag

# do not change things below
# {# pkglts, pysetup.call
setup(**setup_kwds)
# #}
