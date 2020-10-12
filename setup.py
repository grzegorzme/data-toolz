import setuptools
from pkg_resources import parse_requirements

with open("README.md", "r") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    install_requires = [str(requirement) for requirement in parse_requirements(f)]

setuptools.setup(
    name="data-toolz",
    version="0.1.0",
    author="Grzegorz Melniczak",
    author_email="gmelniczak@olx.pl",
    description="Data helper package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://git.naspersclassifieds.com/shared-services/data-science/data-toolz.git",
    packages=setuptools.find_packages(exclude=["tests"]),
    classifiers=["Programming Language :: Python :: 3"],
    python_requires=">=3.7",
    install_requires=install_requires,
)
