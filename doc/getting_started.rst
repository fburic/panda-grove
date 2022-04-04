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

.. tip::

    Creating a Collection using a dictionary of ``df_names -> filenames``
    (like in the first example above) has the advantage that the dictionary can be saved
    as a YAML / JSON specification, to be reused across pipelines or projects.
    E.g.

    .. code-block:: python

        import yaml
        with open('pipeline/input_data.yaml') as spec_file:
            data_spec = yaml.load(config_file, Loader=yaml.FullLoader)
            data = grove.Collection(data_spec)

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

A quick preview of included DataFrames can be shown with ``Collection.head()``::

    >>> data.head()

.. code-block::

    items
    ========
       id description
    0  A1       A one
    1  A2       A two

    categories
    ==========
       id category
    0  A1        A
    1  A1      one

    measurements
    ============
       id  value  measurement_num
    0  A1      1                1
    1  A1      2                2


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

There is also a module-level version of the ``merge`` function
that works independently of a Collection and may be passed
a list of DataFrame objects.

    >>> grove.merge([df1, df2, df3], on='id'])

If column names differ between DataFrames, they are provided as a list.

    >>> data.merge(['items', 'categories', 'measurements'],
    ...            on=['id', ['id', 'id_2']])

.. code-block::

        id description category id_2  value  measurement_num
    0   A1       A one        A   A1      1                1
    1   A1       A one        A   A1      2                2
    2   A1       A one        A   A1      1                3
    3   A1       A one      one   A1      1                1
    4   A1       A one      one   A1      2                2
    5   A1       A one      one   A1      1                3
    ...

This operation is just shorthand for the normal Pandas way to merge multiple DataFrames,
(as shown below) but less writing and easier scaling.

.. code-block:: python

    pd.merge(
        pd.merge(
            data['items'], data['categories'], on='id'
        ),
        data['measurements'], left_on='id', right_on='id_2'
    )

The general structure for a list of DataFrames ``[X1, X2, ...,  Xn]`` is
``[X1X2_on, X2X3_on, ..., Xn-1Xn_on]``,
where ``XiXj_on`` can be a string (common column),
a pair of strings (*left_on*, *right_on* arguments),
or a pair of list of multiple columns to join on.

Since the merge operation is performed iteratively left-to-right,
each ``XiXj_on`` specification can use any column in preceding DataFrames,
not just the columns in the adjacent ``Xi`` and ``Xj`` DataFrames.