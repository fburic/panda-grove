Getting Started
===============

Installation
------------

Install the package with ``pip`` in
`editable <https://pip.pypa.io/en/stable/cli/pip_install/#editable-installs>`_
mode from the repo:

.. code-block:: shell

    pip install -e git+https://github.com/fburic/panda-grove

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

