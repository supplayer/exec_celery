import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="exec-celery",
    author="Supplayer",
    author_email="x254724521@hotmail.com",
    description="Celery handler.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Supplayer/exec_celery.git",
    packages=setuptools.find_packages(include=('execcelery',)),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=['celery[zstd,msgpack,gevent]'],
    setup_requires=['setuptools_scm'],
    use_scm_version=True
)
