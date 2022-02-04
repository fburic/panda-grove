"""
Test general Collection management
"""
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
