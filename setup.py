#!/usr/bin/env python
from setuptools import setup

install_requires = open("requirements.txt").read().strip().split("\n")
dev_requires = open("dev-requirements.txt").read().strip().split("\n")

setup(
    name="target-gcs",
    version="0.1.0",
    description="Singer.io target for extracting data",
    author="Bilal BALTAGI",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_gcs"],
    install_requires=install_requires,
    entry_points="""
    [console_scripts]
    target-gcs=target_gcs:main
    """,
    packages=["target_gcs"],
    package_data = {},
    include_package_data=True,
)
