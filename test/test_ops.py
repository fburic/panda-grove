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

    id_list = [['id', 'id1'], ['id1', 'id2']]
    merged_df = data.merge(['items', 'categories', 'measurements'], on=id_list)
    merged_df_pandas = pd.merge(
        pd.merge(
            data['items'], data['categories'],
            left_on=id_list[0][0], right_on=id_list[0][1]
        ),
        data['measurements'], left_on=id_list[1][0], right_on=id_list[1][1]
    )

    assert merged_df.compare(merged_df_pandas).empty
    with pytest.raises(grove.GroveError,
                       match="'id_x' not present in 'categories'"):
        data.merge(['items', 'categories'], on=[['id', 'id_x']])


def test_merge_mixed_id():
    data = grove.Collection(
        [('items', 'data/items.csv'),
         ('categories', 'data/categories.csv'),
         ('cat_descr', 'data/category_descriptions.csv')
         ]
    )
    id_list = ['id', ['category', 'category_code']]
    merged_df = data.merge(['items', 'categories', 'cat_descr'], on=id_list)
    merged_df_pandas = pd.merge(
        pd.merge(
            data['items'], data['categories'], on=id_list[0]
        ),
        data['cat_descr'], left_on=id_list[1][0], right_on=id_list[1][1]
    )
    assert merged_df.compare(merged_df_pandas).empty


def test_merge_multicolumn():
    data = grove.Collection(
        [('items', 'data/items.csv'),
         ('categories', 'data/categories.csv'),
         ('measurements', 'data/measurements.csv')
         ]
    )

    data['items'] = data['items'].assign(id2=data['items']['id'])
    data['categories'] = data['categories'].assign(id2=data['categories']['id'])
    data['measurements'] = data['measurements'].rename(columns={'id': 'id_x'})

    id_list = [[['id', 'id2'], ['id', 'id2']], ['id2', 'id_x']]
    merged_df = data.merge(['items', 'categories', 'measurements'], on=id_list)
    merged_df_pandas = pd.merge(
        pd.merge(
            data['items'], data['categories'],
            left_on=id_list[0][0], right_on=id_list[0][1]
        ),
        data['measurements'], left_on=id_list[1][0], right_on=id_list[1][1]
    )
    assert merged_df.compare(merged_df_pandas).empty
