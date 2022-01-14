Getting Started
===============

Installation
------------

Install the package with ``pip`` in
`editable <https://pip.pypa.io/en/stable/cli/pip_install/#editable-installs>`_
mode from the repo:

.. code-block:: shell

    pip install -e git+ssh://github.com/fburic/panda-grove.git#egg=panda-grove

**TODO** Install the PyPI package:

.. code-block:: shell

    pip install panda-grove


Start managing DataFrames with a Collection
-------------------------------------------

.. code-block:: python

    import grove

    data = grove.Collection({
        'items': 'data/items.csv',
        'categories': 'data/categories.csv',
        'measurements': 'data/measurements.csv'
    })

Retrieving DataFrames can be done through indexing or attribute access syntax.
Note that for the latter, :ref:`Collection <ref_collection>` class attribute names
take precedence.

    >>> data['items']
    >>> data.items

The indexing syntax allows retrieving multiple DataFrames at once, as a list::

    >>> data[['items', 'categories']]

If any name in the list is missing, a ``GroveError`` is raised.
Generally, Panda Grove prefers to fail quickly, loudly, and explicitly,
to avoid errors creeping in.

Adding new DataFrames
"""""""""""""""""""""

New DataFrames may be added through both indexing and attribute access syntax
(one at a time in both cases).
It's sufficient to just give the path of the file::

    >>> data['tests'] = 'data/tests.tsv'
    >>> data.planning = df

Inspecting the Collection
"""""""""""""""""""""""""

The contents of a Collection can be quickly listed by just printing it::

    >>> data

.. code-block::

    items
    ========
    * cols: 2
    * rows: 6

    categories
    ==========
    * cols: 2
    * rows: 12

    measurements
    ============
    * cols: 3
    * rows: 15

More technical details can be viewed with the ``Collection.info()`` method::

    >>> data.info()

.. code-block::

    Contents: 3 DataFrames
    ['categories', 'items', 'measurements']

    Memory usage
    ============
        DataFrame       MiB
            items  0.000818
       categories  0.001476
     measurements  0.001195
            TOTAL  0.003489


Merge multiple DataFrames
-------------------------

Data may spread across several tables.
Grove will iteratively merge a list of DataFrames.

    >>> data.merge(['items', 'categories', 'measurements'], on='id')

.. code-block::

        id description category  value  measurement_num
    0   A1       A one        A      1                1
    1   A1       A one        A      2                2
    2   A1       A one        A      1                3
    3   A1       A one      one      1                1
    4   A1       A one      one      2                2
    5   A1       A one      one      1                3
    ...

If the column to be merge on has the same name in all given DataFrames,
the ``on`` argument can be omitted, as with Pandas ``merge()``.

If column names differ between DataFrames, they are provided as a list:

    >>> data.merge(['items', 'categories', 'measurements'],
    ...            on=['id', 'id', 'id_2'])

.. code-block::

        id description category id_2  value  measurement_num
    0   A1       A one        A   A1      1                1
    1   A1       A one        A   A1      2                2
    2   A1       A one        A   A1      1                3
    3   A1       A one      one   A1      1                1
    4   A1       A one      one   A1      2                2
    5   A1       A one      one   A1      1                3
    ...

This is just shorthand for the normal Pandas way to merge multiple DataFrames,
but less writing and easier scaling:

.. code-block:: python

    pd.merge(
        pd.merge(
            data['items'], data['categories'], on='id'
        ),
        data['measurements'], left_on='id', right_on='id_2'
    )
