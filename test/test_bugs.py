# Tests for bugs
from contextlib import redirect_stdout
import io

import grove
import pandas as pd


def test_issue_1_info_pandas_2():
    data = grove.Collection({
        'items': 'test/data/items.csv',
        'categories': 'test/data/categories.csv',
        'measurements': 'test/data/measurements.csv'
    })

    with redirect_stdout(io.StringIO()) as grove_print:
        data.info(verbose=True)
    grove_print = grove_print.getvalue()

    assert len(grove_print) > 0

    # The size calc will vary so just make sure each DataFrame is listed.
    # After all, the test is about getting a table, not the mem calc.
    assert 'items' in grove_print
    assert 'categories' in grove_print
    assert 'measurements' in grove_print
    assert 'TOTAL' in grove_print


def test_issue_2_numerical_col_names():
    df1 = pd.DataFrame(
        [('a', 1),
         ['b', 2]]
    )
    df2 = pd.DataFrame(
        [('x', 1),
         ['b', 3]]
    )
    data = grove.Collection({
        'A': df1,
        'B': df2
    })
    result = data.merge(['A', 'B'], on=0)
    expected_result = pd.merge(df1, df2, on=0)
    assert result.shape[0] > 0
    assert result.compare(expected_result).empty
