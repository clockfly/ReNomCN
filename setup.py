from setuptools import find_packages, setup

with open("requirements.txt") as reader:
    requires = [line for line in reader.readlines()]

setup(
    install_requires=requires,
    name="renom_cn",
    version="0.0b1",
    packages=find_packages()
)
