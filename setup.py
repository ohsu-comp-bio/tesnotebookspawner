#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function
from distutils.core import setup
import os
import sys

v = sys.version_info
if v[:2] < (3, 3):
    error = "ERROR: Jupyter Hub requires Python version 3.3 or above."
    print(error, file=sys.stderr)
    sys.exit(1)

if os.name in ('nt', 'dos'):
    error = "ERROR: Windows is not supported"
    print(error, file=sys.stderr)

pjoin = os.path.join
here = os.path.abspath(os.path.dirname(__file__))

# Get the current package version.
version_ns = {}
with open(pjoin(here, 'tesspawner', '_version.py')) as f:
    exec(f.read(), {}, version_ns)

setup_args = dict(
    name='tesspawner',
    packages=['tesspawner'],
    version=version_ns['__version__'],
    description="TESspawner: A spawner for Jupyterhub to spawn notebooks using the GA4GH Task Execution Server.",
    long_description="",
    author="Adam Struck",
    author_email="strucka@ohsu.edu",
    url="",
    platforms="Linux, Mac OS X",
    keywords=['Interactive', 'Interpreter', 'Shell', 'Web'],
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
)


# setuptools requirements
if 'setuptools' in sys.modules:
    setup_args['install_requires'] = install_requires = []
    with open('requirements.txt') as f:
        for line in f.readlines():
            req = line.strip()
            if not req or req.startswith(('-e', '#')):
                continue
            install_requires.append(req)


def main():
    setup(**setup_args)


if __name__ == '__main__':
    main()
