# Tests for bugs
from contextlib import redirect_stdout
import io
import grove


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
