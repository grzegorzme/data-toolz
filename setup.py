import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

requirements = ["s3fs==0.4.2", "pandas", "pyarrow"]

setuptools.setup(
    name="data-toolz",
    version="0.1.7",
    author="Grzegorz Melniczak",
    author_email="mogadish@gmail.com",
    description="Data helper package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/grzegorzme/data-toolz",
    packages=setuptools.find_packages(exclude=["tests"]),
    classifiers=["Programming Language :: Python :: 3"],
    python_requires=">=3.7",
    install_requires=requirements,
)
