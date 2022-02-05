"""
Test Panda Grove operations on DataFrames
"""
import numpy as np
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
                       match="'id_x' not present in DataFrame 1"):
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


def test_merge_dataframes():
    data = grove.Collection(
        [('items', 'data/items.csv'),
         ('categories', 'data/categories.csv'),
         ('cat_descr', 'data/category_descriptions.csv')
         ]
    )
    df_list = [data['items'][['id']],
               data['categories'].head(4),
               data['cat_descr'].drop_duplicates('category_code')]
    id_list = ['id', ['category', 'category_code']]
    merged_df = grove.merge(df_list, on=id_list)
    merged_df_pandas = pd.merge(
        pd.merge(
            df_list[0], df_list[1], on=id_list[0]
        ),
        df_list[2], left_on=id_list[1][0], right_on=id_list[1][1]
    )
    assert merged_df.compare(merged_df_pandas).empty


def test_reduce_mem_series():
    rng = np.random.default_rng(42)
    df_len = 10
    df = pd.DataFrame.from_records(
        zip(rng.integers(0, int(1e6), size=df_len),
            rng.random(size=df_len) * 1e6,
            rng.integers(0, 1, size=df_len)
            ),
        columns=['integers', 'floats', 'binaries']
    )

    assert grove.reduce_mem_series(df['integers']).dtype <= 'uint64'
    assert grove.reduce_mem_series(df['integers']).dtype == 'uint32'
    assert grove.reduce_mem_series(df['integers'] * -1).dtype == 'int32'
    assert grove.reduce_mem_series(df['binaries'] * -1).dtype == 'uint8'
    assert grove.reduce_mem_series(df['floats'],
                                   target_float='float32').dtype == 'float32'


def test_reduce_mem_df():
    rng = np.random.default_rng(42)
    df_len = 10
    df = pd.DataFrame.from_records(
        zip(rng.integers(0, int(1e6), size=df_len),
            rng.random(size=df_len) * 1e6,
            rng.integers(0, 1, size=df_len)
            ),
        columns=['integers', 'floats', 'binaries']
    )

    opt_df = grove.reduce_mem_df(df)
    assert opt_df['integers'].dtype == 'uint32'
    assert opt_df['floats'].dtype == 'float32'
    assert opt_df['binaries'].dtype == 'uint8'

    df = grove.reduce_mem_df(df, inplace=True)
    assert df['integers'].dtype == 'uint32'
    assert df['floats'].dtype == 'float32'
    assert df['binaries'].dtype == 'uint8'
