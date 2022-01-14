"""
Test Panda Grove operations on DataFrames
"""
import pandas as pd
import pytest

import grove


def test_merge_same_id():
    data = grove.Collection(
        [('items', 'data/items.csv'),
         ('categories', 'data/categories.csv'),
         ('measurements', 'data/measurements.csv')
         ]
    )
    merged_df = data.merge(['items', 'categories', 'measurements'], on='id')
    merged_df_implicit = data.merge(['items', 'categories', 'measurements'])
    merged_single = data.merge(['items'])

    merged_df_pandas = pd.merge(
        pd.merge(
            data['items'], data['categories'], on='id'
        ),
        data['measurements'], on='id'
    )

    assert merged_df.compare(merged_df_pandas).empty
    assert data['items'].compare(merged_single).empty
    assert merged_df.compare(merged_df_implicit).empty


def test_merge_diff_id():
    data = grove.Collection(
        [('items', 'data/items.csv'),
         ('categories', 'data/categories.csv'),
         ('measurements', 'data/measurements.csv')
         ]
    )
    data['categories'] = data['categories'].rename(columns={'id': 'id1'})
    data['measurements'] = data['measurements'].rename(columns={'id': 'id2'})

    merged_df = data.merge(['items', 'categories', 'measurements'],
                           on=['id', 'id1', 'id2'])
    merged_df_pandas = pd.merge(
        pd.merge(
            data['items'], data['categories'], left_on='id', right_on='id1'
        ),
        data['measurements'], left_on='id1', right_on='id2'
    )

    assert merged_df.compare(merged_df_pandas).empty
    with pytest.raises(grove.GroveError,
                       match="Column 'id_x' not present in 'categories'"):
        data.merge(['items', 'categories'], on=['id', 'id_x'])
