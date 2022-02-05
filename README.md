# Panda Grove

A lightweight package for easier management of multiple Pandas DataFrames.

Some inspiration is taken from database models to help track relationships between
DataFrames and facilitate operations between these, 
without the strong structural and data type enforcements.

The philosophy of Panda Grove is to assist without getting in the way or
creating yet another API on top of Pandas.

![AppVeyor tests](https://img.shields.io/appveyor/tests/fburic/panda-grove?color=brightgreen)

## Highlights

- `Collection` class to encapsulate and manage multiple DataFrames
- Multi-merges (e.g. merge across multiple DataFrames)
- TODO: schema and datatype management


## Installation

Install the package with `pip` in
[editable](https://pip.pypa.io/en/stable/cli/pip_install/#editable-installs)
mode from the repo:

```shell
pip install -e git+ssh://github.com/fburic/panda-grove.git#egg=panda-grove
```

**TODO** Install the PyPI package:

```shell
pip install panda-grove
```
