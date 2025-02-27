#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='sparkbasics',
    version='1.0.0',
    description='BDCC Pyspark Basics project',
    packages=find_packages(where="src/main/python"),
    package_dir={'': 'src/main/python'},
    install_requires=[  # If there are any dependencies for your project
        'requests', 
        'pygeohash',
        'python-dotenv',
        'pyspark',
    ],
    zip_safe=False,
)
