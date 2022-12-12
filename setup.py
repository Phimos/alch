from setuptools import find_packages, setup

setup(
    name="alch",
    packages=find_packages(include=["alch"]),
    author="Yunchong Gan",
    author_email="yunchong@pku.edu.cn",
    license="MIT",
    entry_points={"console_scripts": ["alch=alch.cli:main"]},
)
