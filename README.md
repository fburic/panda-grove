<div align="center">
  <img src="img/grove_logo.svg"><br>
</div>

<h1 align="center">Panda Grove</h1>

-----------------

A lightweight package for easier management of multiple Pandas DataFrames
during data analysis and exploration.

The philosophy of Panda Grove is to assist without getting in the way or
creating yet another API on top of Pandas.

It also depends exclusively on the Pandas package for minimal overhead.

## Feature Highlights

* :ballot_box_with_check: Multi-merges (merge across multiple DataFrames at once)
* :ballot_box_with_check: `Collection` class to encapsulate and manage multiple DataFrames
* :ballot_box_with_check: DataFrame sanity checks
* :black_square_button: Support all types of joins (currently only inner join)
* :black_square_button: Datatype management (work in progress)


## Installation

Install the PyPI package:

```shell
pip install panda-grove
```

## Requirements

- python >= 3.7
- pandas >= 1.1.0
- IPython >= 7 is not required but will add pretty printing in Jupyter notebooks
