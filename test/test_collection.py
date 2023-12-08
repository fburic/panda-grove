"""
Test general Collection management
"""
from contextlib import redirect_stdout
import io

import grove


def test_create_collection_from_spec():
    data = grove.Collection(
        [('items', 'data/items.csv'),
         ('categories', 'data/categories.csv'),
         ('measurements', 'data/measurements.csv')
         ]
    )
    data['categories2'] = 'data/categories.csv'

    assert data['items'].shape == (6, 2)
    assert data.categories.shape == (12, 2)
    assert data.categories2.shape == (12, 2)
    assert data['measurements'].shape == (15, 3)


def test_collection_inspection():
    data = grove.Collection(
        [('items', 'data/items.csv'),
         ('categories', 'data/categories.csv'),
         ('measurements', 'data/measurements.csv')
         ]
    )
    assert data.dataframe_list == ['categories', 'items', 'measurements']


def test_collection_info():
    """
    Test some edge cases.
    Notes:
    - The output is cumulative with verbose=True, so have it set.
    - Empty DataFrames still have metainfo and will yield output (no need to test)
    """

    # Edge case: empty Collection

    data = grove.Collection()
    with redirect_stdout(io.StringIO()) as grove_print:
        data.info(verbose=True)
    grove_print = grove_print.getvalue()
    assert grove_print.startswith('Contents: 0 DataFrames')
