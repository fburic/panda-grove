# Panda Grove :bamboo:

A lightweight package for easier management of multiple Pandas DataFrames.

The philosophy of Panda Grove is to assist without getting in the way or
creating yet another API on top of Pandas.

It also depends exclusively on the Pandas package for minimal overhead.

![AppVeyor tests](https://img.shields.io/appveyor/tests/fburic/panda-grove?color=brightgreen)

## Feature Highlights

* :ballot_box_with_check: Multi-merges (merge across multiple DataFrames at once)
* :ballot_box_with_check: `Collection` class to encapsulate and manage multiple DataFrames
* :ballot_box_with_check: DataFrame sanity checks
* :black_square_button: Datatype management


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
