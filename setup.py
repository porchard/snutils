#!/usr/bin/env python

import re
from setuptools import setup, find_packages

with open("snutils/__init__.py") as f:
    __version__ = re.search(r'__version__ ?= ?[\'\"]([\w.]+)[\'\"]', f.read()).group(1)

# Setup information
setup(
    name = 'snutils',
    version = __version__,
    packages = find_packages(),
    package_data={
        "": ["data/barcodes.*.txt.gz"]
    },
    description = 'Utility functions for multiome processing.',
    author = 'Peter Orchard',
    author_email = 'porchard@umich.edu',
    scripts = ['bin/mm'],
    install_requires = [
        'pandas']
)