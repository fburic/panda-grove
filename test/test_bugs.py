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

    # The size calc will vary so just make sure each DataFrame has a value.
    # After all, the test is about getting a table, not the mem calc.
    assert 'items 0.0' in grove_print
    assert 'categories 0.0' in grove_print
    assert 'measurements 0.0' in grove_print
    assert 'TOTAL 0.0' in grove_print
