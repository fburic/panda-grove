import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

project_urls = {
    "Documentation": "https://panda-grove.readthedocs.io",
    "Source Code": "https://github.com/fburic/panda-grove",
}

setuptools.setup(
    name="panda-grove",
    version='0.1.1',
    author="Filip Buric",
    author_email="",
    description="A lightweight package for easier management of multiple Pandas DataFrames",
    long_description=long_description,
    project_urls = project_urls,
    long_description_content_type="text/markdown",

    packages=setuptools.find_packages(exclude=['test']),
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Topic :: Database",
        "Topic :: Scientific/Engineering ",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only"
    ],
    python_requires='>=3.7',
)