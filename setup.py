#!/usr/bin/env python

from setuptools import find_packages, setup, Command

required = [
    'pillow',
    'numpy',
    'python-dateutil',
]

setup(
    name="pipex",
    version="0.0.2",
    description="Data Processing With Pipe Syntax",
    author="Alchan Algy Kim",
    author_email="a9413miky@gmail.com",
    url="https://github.com/Algy/pipex",
    packages=find_packages(exclude=["test", "tess.*"]),
    entry_points={},
    package_data={
        "": ["LICENSE"],
    },
    python_requires=">=3.5",
    install_requires=required,
    extras_require={},
    include_package_data=True,
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
)
