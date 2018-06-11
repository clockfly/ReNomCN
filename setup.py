from setuptools import find_packages, setup

requires = [
    "numpy", "pandas", "osisoft.pidevclub.piwebapi"
]

setup(
    install_requires=requires,
    name="renom_cn",
    version="0.0.1",
    packages=find_packages()
)
