#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-augment",
    version="0.1.0",
    description="Singer.io tap for extracting data from the Augment Analytics API",
    author="bilanc",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "singer-python==5.12.1",
        "requests==2.29.0",
        "urllib3==1.26.20",
        "backoff==1.8.0",
    ],
    extras_require={"dev": ["pylint==2.6.2", "ipdb", "nose", "requests-mock==1.9.3"]},
    entry_points="""
          [console_scripts]
          tap-augment=tap_augment:main
      """,
    packages=["tap_augment"],
    package_data={"tap_augment": ["schemas/*.json"]},
    include_package_data=True,
)
