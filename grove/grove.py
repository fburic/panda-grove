import logging
from pathlib import Path
from typing import Iterable, Union

import numpy as np
import pandas as pd

try:
    from IPython.display import display
except ModuleNotFoundError:
    display = print

__all__ = ['Collection', 'merge', 'reduce_mem_series', 'reduce_mem_df',
           'sanity_check_df',
           'GroveError']


LOG_FORMAT = "%(name)s %(levelname)s:\t%(message)s"
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
logger = logging.getLogger('grove')


class Collection:
    """
    A container class to manage DataFrames.
    Each DataFrame is stored with a string name
    and is accessible via indexing or attribute name.

    Example
    -------
    >>> import pandas as pd
    >>> import grove
    >>> df = pd.read_csv('c.csv')
    >>>
    >>> data = grove.Collection({'A':  'a.csv',
    ...                          'B': 'b.tsv',
    ...                          'C': df})
    >>> data['D'] = 'd.tsv'
    >>> data['A']
    >>> data.B
    >>> data.C

    Note:
        For attribute access, the Collection class attribute names
        (e.g. ``show_schema``) take precedence and will be returned instead.
    """

    def __init__(self, data_sources: Union[dict, Iterable] = None):
        """
        One can create an empty collection and add DataFrames iteratively,
        or initialize from a list / dictionary specifying DataFrames and names.

        :param data_sources: Pairs of names and file paths / DataFrames,
                             as a list of tuples or dictionary

        Example
        -------
        >>> data = grove.Collection()
        >>> data['A'] = 'a.csv'  # Will load data in a pandas DataFrame
        >>> data['B'] = df
        >>>
        >>> data = grove.Collection({'A':  'a.csv',
        ...                          'B': pathlib.Path('b.tsv'),
        ...                          'C': df})
        """
        if data_sources is None:
            self._data_frames = {}
            return
        if isinstance(data_sources, dict):
            data_sources = list(data_sources.items())
        self._data_frames = {}
        if isinstance(data_sources, Iterable):
            for df_name, df_source in data_sources:
                self._add_dataframes(df_name, df_source)
        else:
            raise GroveError('data_sources specification not supported')

    def __getitem__(self, df_names: Union[str, list]) -> Union[pd.DataFrame, list]:
        """Retrieve DataFrame using indexing."""
        if isinstance(df_names, str):
            df_names = [df_names]

        missing_names = [
            name for name in df_names if name not in self._data_frames
        ]
        if missing_names:
            raise GroveError(f'DataFrame(s) not in Collection: {missing_names}')

        df_list = [self._data_frames[name] for name in df_names]
        if len(df_list) == 1:
            return df_list[0]
        else:
            return df_list

    def __setitem__(self, df_name: str, df_source: Union[pd.DataFrame, str, Path]):
        self._add_dataframes(df_name, df_source)

    def __getattr__(self, df_name: str) -> pd.DataFrame:
        """
        Retrieve DataFrame as attribute, unless an existing Collection attribute
        exists called `df_name`, in which case that is returned.
        """
        try:
            return self._data_frames[df_name]
        except KeyError as _:
            raise AttributeError(f"'Collection' object has no attribute '{df_name}'")

    def __repr__(self):
        if len(self._data_frames.items()) == 0:
            return 'Collection is empty'
        descr = ''
        for df_name, df in self._data_frames.items():
            descr += df_name + '\n' + '=' * max(8, len(df_name)) + '\n'
            descr += f'* cols: {len(df.columns)}' + '\n'
            descr += f'* rows: {df.shape[0]}' + '\n'
            descr += '\n'
        return descr

    def _add_dataframes(self, df_name, df_source: Union[pd.DataFrame, str, Path]):
        """
        Generic method to add a DataFrame to a grove Collection

        :param df_name: String name for the new Collection DataFrame
        :param df_source: A file path (as str or pathlib.Path) or DataFrame object
        """
        if isinstance(df_source, str) or isinstance(df_source, Path):
            self._data_frames[df_name] = _read_dataframe(df_source)
        elif isinstance(df_source, pd.DataFrame):
            self._data_frames[df_name] = df_source
        else:
            raise TypeError(f'{df_source} is an unsupported data type')

    def merge(self, df_name_list: list, on: Union[str, list] = None) -> pd.DataFrame:
        """
        Merge multiple DataFrames in the Collection.

        The merging is performed pairwise, in the specified order.
        I.e. for a given list ``['A', 'B', 'C']``,
        the result is ``merge(merge(A, B), C)``

        Handling of the ``on`` argument is only slightly wrapped around Pandas behavior.
        The ``on`` argument can be omitted to join on the columns with the same name in all DataFrames.
        A single string may be provided if it's the common column to join on in all DataFrames.
        The general structure for a list of DataFrames ``[X1, X2, ...,  Xn]`` is
        ``[X1X2_on, X2X3_on, ..., Xn-1Xn_on]``,
        where ``XiXj_on`` can be a string (common column),
        a pair of strings (*left_on*, *right_on* arguments),
        or a pair of list of multiple columns to join on.
        See *Examples* below.

        Note:
            Since the merge operation is performed iteratively left-to-right,
            each ``XiXj_on`` ``on`` specification can use any column in preceding DataFrames,
            not just the columns in the adjacent ``Xi`` and ``Xj`` DataFrames.

        Examples
        --------

        All DataFrames are to be merged on their ``'id'`` column:

        >>> data.merge(['items', 'categories', 'measurements'], on='id')


        DataFrames ``items`` and ``categories`` are merged on ``'id'``, while
        ``cat_descr`` is merged on ``'category'``, which matches ``'category'`` in ``categories``:

        >>> data.merge(['items', 'categories', 'cat_descr'],
        ...            on=['id', ['category', 'category_code']])

        Multi-column join between ``items`` and ``categories``
        (on columns ``'id'`` and ``'id2'``), followed by joining in ``measurements``
        on column ``'id_x'`` matching column ``'id2'`` in the ``items - categories`` merge.

        >>> data.merge(
        ...     ['items', 'categories', 'measurements'],
        ...     on=[[['id', 'id2'], ['id', 'id2']],
        ...         ['id2', 'id_x']]
        ... )

        For multi-column joins with common columns,
        make sure to put these in a nested list:

        >>> data.merge(
        ...     ['items', 'categories', 'measurements'],
        ...     on=[[['A', 'B']], ['id2', 'id_x']]
        ... )

        :param df_name_list: Names of collection DataFrame to merge
        :param on: Column names to join on.
                   Either a single string or omitted if it's the same across all
                   DataFrames. If column names to join on differ, these are given
                   as a list.
        :return: Merged DataFrame
        """
        return merge(
            [self._data_frames[df_name] for df_name in df_name_list], on=on
        )

    def info(self, memory_usage=True, verbose=False):
        """
        Print information about the collection.

        :param memory_usage: Whether to include memory estimates from DataFrames
        :param verbose: Whether to also call ``.info()`` for DataFrames

        Example
        -------
        >>> data = grove.Collection({
        ...     'items': 'data/items.csv',
        ...     'categories': 'data/categories.csv',
        ...     'measurements': 'data/measurements.csv'})
        >>> data.info()
        Contents: 3 DataFrames
        ['categories', 'items', 'measurements']
        Memory usage
        ============
            DataFrame       MiB
                items  0.000818
           categories  0.001476
         measurements  0.001195
                TOTAL  0.003489
        """
        n_df = len(self._data_frames.items())
        df_names = sorted([df_name for df_name in self._data_frames.keys()])
        if n_df == 0:
            return 'Collection is empty'
        info = ''
        info += f'Contents: {n_df} DataFrames\n' + str(df_names) + '\n'

        if memory_usage:
            info += '\nMemory usage\n============\n'
            mem_list = pd.DataFrame.from_records(
                [(df_name, df.memory_usage(deep=True).sum() / 1024 ** 2)
                  for df_name, df in self._data_frames.items()],
                columns=['DataFrame', 'MiB']
            )
            mem_list = mem_list.append(
                {'DataFrame': 'TOTAL', 'MiB': mem_list['MiB'].sum()},
                ignore_index=True
            )
            info += mem_list.to_string(index=False) + '\n'

        print(info)
        if verbose:
            for df_name in df_names:
                display(TextHeader(df_name))
                display(self._data_frames[df_name].dtypes.rename('Dtype').rename_axis('Column').reset_index())
                print()

    def reduce_mem(self, target_float: str = 'float32'):
        """
        Minimize the Collection memory usage,
        by using the smallest applicable Numpy datatypes for all integer and float columns,
        in all included DataFrames.
        This is done **in-place** (i.e. will modify the collection DataFrames),
        since the use case is optimizing large Collections for downstream analyses.

        For floats, the target float precision (default ``float32``)
        must be decided beforehand, since this is application-specific.

        Subsequent operations on the DataFrames will likely promote the dtypes to the
        platform default (e.g. ``int64``).

        Example
        -------

        >>> rng = np.random.default_rng(42)
        >>> df_len = int(1e6)
        >>> df = pd.DataFrame.from_records(
        ...     zip(rng.integers(0, int(1e6), size=df_len),
        ...         rng.random(size=df_len) * 1e6,
        ...         rng.integers(0, 1, size=df_len)
        ...         ),
        ...     columns=['integers', 'floats', 'binaries'])
        >>> data = grove.Collection({'df1': df})
        >>> data.info(verbose=True)
        Contents: 1 DataFrames
        ['df1']
        Memory usage
        ============
        DataFrame        MiB
              df1  22.888306
            TOTAL  22.888306
        df1
        ========
        <class 'pandas.core.frame.DataFrame'>
        RangeIndex: 1000000 entries, 0 to 999999
        Data columns (total 3 columns):
         #   Column    Non-Null Count    Dtype
        ---  ------    --------------    -----
         0   integers  1000000 non-null  int64
         1   floats    1000000 non-null  float64
         2   binaries  1000000 non-null  int64
        dtypes: float64(1), int64(2)None

        The default sizes on the system in this example are ``int64`` and ``float64``,
        which are too large for our data.
        Memory usage can be reduced by a factor of *2.6*:

        >>> data.reduce_mem()
        >>> data.info(verbose=True)
        Contents: 1 DataFrames
        ['df1']
        Memory usage
        ============
        DataFrame       MiB
              df1  8.583191
            TOTAL  8.583191
        df1
        ========
        <class 'pandas.core.frame.DataFrame'>
        RangeIndex: 1000000 entries, 0 to 999999
        Data columns (total 3 columns):
         #   Column    Non-Null Count    Dtype
        ---  ------    --------------    -----
         0   integers  1000000 non-null  uint32
         1   floats    1000000 non-null  float32
         2   binaries  1000000 non-null  uint8
        dtypes: float32(1), uint32(1), uint8(1)None

        :param target_float: Float columns will be converted to this precision.
        """
        for df_name in self._data_frames.keys():
            _ = reduce_mem_df(self._data_frames[df_name],
                              target_float=target_float,
                              inplace=True)

    def head(self,  n: int = 5) -> None:
        """
        Iteratively print head() for all DataFrames in the Collection.

        :param n: How many rows to show
        """
        for df_name, df in self._data_frames.items():
            display(TextHeader(df_name), df.head(n))
            print()

    def sanity_checks(self) -> None:
        """
        Iteratively check for typical desirable data properties for all
        Collection DataFrames.
        See documentation for :py:func:`grove.sanity_check_df` for more details
        """
        for df_name, df in self._data_frames.items():
            display(TextHeader(df_name))
            sanity_check_df(df)
            print()

    @property
    def dataframe_list(self):
        """
        Get the sorted list of DataFrame names in the Collection.
        """
        return sorted(self._data_frames.keys())


def merge(df_list: list, on: Union[str, list] = None) -> pd.DataFrame:
    """
    Merge multiple DataFrames.
    This module-level function allows more flexibility in passing DataFrame
    while doing any on-the-fly operations.

    Example
    -------

    >>> grove.merge(
    ...     [
    ...         items[['id']],
    ...         categories.head(4),
    ...         cat_descr.drop_duplicates('category_code')
    ...     ],
    ...     on=['id', ['category', 'category_code']]
    ... )

    The merging is performed pairwise, in the specified order.
    I.e. for a given list ``[df_A, df_B, df_C]``,
    the result is ``merge(merge(df_A, df_B), df_C)``

    Handling of the ``on`` argument is only slightly wrapped around Pandas behavior.
    The ``on`` argument can be omitted to join on the columns with the same name in all DataFrames.
    A single string may be provided if it's the common column to join on in all DataFrames.
    The general structure for a list of DataFrames ``[X1, X2, ...,  Xn]`` is
    ``[X1X2_on, X2X3_on, ..., Xn-1Xn_on]``,
    where ``XiXj_on`` can be a string (common column),
    a pair of strings (*left_on*, *right_on* arguments),
    or a pair of list of multiple columns to join on.
    See *Examples* below.

    Note:
        Since the merge operation is performed iteratively left-to-right,
        each ``XiXj_on`` specification can use any column in preceding DataFrames,
        not just the columns in the adjacent ``Xi`` and ``Xj`` DataFrames.

    Examples
    --------

    All DataFrames are to be merged on their ``'id'`` column:

    >>> grove.merge([items_df, categories_df, measurements_df], on='id')


    DataFrames ``items_df`` and ``categories_df`` are merged on ``'id'``, while
    ``cat_descr_df`` is merged on ``'category'``,
    which matches ``'category'`` in ``categories_df``:

    >>> grove.merge([items_df, categories_df, cat_descr_df],
    ...             on=['id', ['category', 'category_code']])

    Multi-column join between ``items_df`` and ``categories_df``
    (on columns ``'id'`` and ``'id2'``), followed by joining in ``measurements_df``
    on column ``'id_x'`` matching column ``'id2'`` in the ``items - categories`` merge.

    >>> grove.merge(
    ...     [items_df, categories_df, measurements_df],
    ...      on=[[['id', 'id2'], ['id', 'id2']], ['id2', 'id_x']]
    ... )

    For multi-column joins with common columns,
    make sure to put these in a nested list:

    >>> grove.merge(
    ...     [items_df, categories_df, measurements_df],
    ...      on=[[['A', 'B']], ['id2', 'id_x']]
    ... )

    :param df_list: DataFrames to be merged, in order they appear in the list
    :param on: Column names to join on.
               Either a single string or omitted if it's the same across all
               DataFrames. If column names to join on differ, these are given
               as a list.
    :return: Merged DataFrame
    """
    # Normalize id_list for uniform merging
    # Normal form is [[ids, ids], [ids, ids], ... ]
    if on is None:
        id_list = [[None, None] for _ in range(len(df_list) - 1)]
    elif isinstance(on, str):
        id_list = [[on, on] for _ in range(len(df_list) - 1)]
    elif isinstance(on, list):
        id_list = on
    else:
        raise GroveError(f'Specification of merge columns not supported: {on}')

    # Process list into (left_on, right_on pairs)
    id_list_norm = []
    for m_id in id_list:
        # Common column name
        if isinstance(m_id, str):
            merge_pair = [m_id, m_id]

        # Multi-column merge
        elif isinstance(m_id, list) and _depth(m_id) == 2:

            # same columns: [['A', 'B']]
            if len(m_id) == 1:
                merge_pair = [m_id[0], m_id[0]]

            # diff columns: [['A_left', 'B_left'], ['A_right', 'B_right']]
            elif len(m_id) == 2:
                merge_pair = m_id
            else:
                raise GroveError(f'Specification of merge columns not supported: {on}')

        #  Diff left and right columns: ['A', 'B']
        elif isinstance(m_id, list) and _depth(m_id) == 1 and len(m_id) == 2:
            merge_pair = m_id

        else:
            raise GroveError(f'Specification of merge columns not supported: {on}')

        id_list_norm.append(merge_pair)

    # id_list = [[m_id, m_id] if isinstance(m_id, str) else m_id
    #            for m_id in id_list]

    # Explicitly fail if columns not present
    for i, m_id in enumerate(id_list_norm):
        for k, col_id in enumerate(m_id):
            if col_id is not None:
                df_num = i + k
                try:
                    _ = df_list[df_num][col_id]
                except KeyError:
                    raise GroveError(f"'{col_id}' not present in DataFrame {df_num}")

    merged_df = df_list[0]
    for i, m_id in enumerate(id_list_norm):
        try:
            merged_df = pd.merge(
                merged_df,
                df_list[i + 1],
                left_on=m_id[0],
                right_on=m_id[1]
            )
        except Exception as e:
            print(f"Error merging in '{df_list[i]}'")
            raise e

    return merged_df


def reduce_mem_series(values: pd.Series, target_float: str = 'float32') -> pd.Series:
    """
    Minimize memory usage of a Series
    by using the smallest applicable Numpy datatype.
    Handles integers and floats only.

    For floats, the target float precision (default ``float32``)
    must be decided beforehand, since this is application-specific.

    Subsequent operations on the Series will likely promote the dtype to the
    platform default (e.g. ``int64``) so best used with read-only data.

    Example
    -------

    >>> rng = np.random.default_rng(42)
    >>> series = pd.Series(rng.integers(0, int(1e6), size=10))
    >>> print(series.dtype)
    >>> print(grove.reduce_mem_series(series).dtype)
    int64
    uint32

    :param values: A Pandas Series.
    :param target_float: A float Series will be converted to this precision.
    :return: A copy of the input Series with optimized dtypes.
    """
    if values.dtype.kind not in ['i', 'u', 'f']:
        return values
    if values.dtype.kind == 'f':
        return values.astype(target_float)

    min_val = values.min()
    max_val = values.max()
    new_type = values.dtype
    for dtype in [np.uint8, np.uint16, np.uint32, np.int8, np.int16, np.int32]:
        if np.iinfo(dtype).min <= min_val and max_val <= np.iinfo(dtype).max:
            new_type = dtype
            break
    opt = values.astype(new_type)

    assert np.allclose(values.values, opt.values), \
        'Grove bug: Type-optimized values differ from input'
    return opt


def reduce_mem_df(df: pd.DataFrame,
                  target_float: str = 'float32',
                  inplace=False) -> pd.DataFrame:
    """
    Minimize memory usage of a DataFrame
    by using the smallest applicable Numpy datatypes for all integer and float columns.

    For floats, the target float precision (default ``float32``)
    must be decided beforehand, since this is application-specific.

    Subsequent operations on the DataFrame will likely promote the dtypes to the
    platform default (e.g. ``int64``) so best used with read-only data.

    Good practice is usually to keep input data immutable, but due to sheer size of
    some DataFrames, this operation can be done in-place,
    by setting the ``inplace`` flag.

    Example
    -------

    >>> rng = np.random.default_rng(42)
    >>> df = pd.DataFrame.from_records(
    ... zip(rng.integers(0, int(1e6), size=df_len),
    ...     rng.random(size=df_len) * 1e6,
    ...     rng.integers(0, 1, size=df_len)
    ...     ),
    ... columns=['integers', 'floats', 'binaries'])
    >>> print(df.dtypes)
    integers      int64
    floats      float64
    binaries      int64
    dtype: object
    >>> print(grove.reduce_mem_df(df).dtypes)
    integers     uint32
    floats      float32
    binaries      uint8
    dtype: object

    :param df: Input DataFrame, which will not be changed unless ``inplace`` is set.
    :param target_float: Float columns will be converted to this precision.
    :param inplace: Defaults to ``False``. If ``True``, the input DataFrame will be changed.
    :return: A new DataFrame with reduced datatypes if ``inplace`` is not set,
             otherwise a reference will be returned to the changed input DataFrame.
    """
    if inplace:
        opt = df
    else:
        opt = df.copy(deep=True)
    for label, _ in opt.items():
        opt[label] = reduce_mem_series(opt[label], target_float=target_float)
    return opt


def sanity_check_df(df: pd.DataFrame, id_column: str = '') -> bool:
    """
    Check for typical desirable data properties for the given DataFrame:

    - Unique IDs (either in specified ``id_column`` or any non-float, no-N/As column)
    - No completely empty (N/A) columns
    - Warn about 'object' type columns since they cause merge problems

    Example
    -------

    >>> df = pd.DataFrame.from_records(
    ...     zip([10, 20, 30, 10, 40],
    ...         ['a', 'b', 'c', 'd', 'e'],
    ...         [np.nan] * 5,
    ...         ['10', '20', '30', '40', None]),
    ...     columns=['id', 'descr', 'values', 'serials'])

    >>> grove.sanity_check_df(df, id_column='id')
    WARN: ID column 'id' does not have unique values
    WARN: The following columns are completely empty (N/A): ['values']
    False

    >>> grove.sanity_check_df(df, id_column='serials')
    WARN: ID column 'serials' has N/A values
    WARN: The following columns are completely empty (N/A): ['values']
    False

    :param df: Pandas DataFrame
    :param id_column: If given, this column is checked for unique values
    :return: **True** if all checks passed, **False** otherwise
    """
    if all([
        _check_id_col(df, id_column),
        _check_null_cols(df),
        _check_obj_cols(df)
        ]
    ):
        logger.info('All checks passed')
        return True
    return False


def _check_id_col(df: pd.DataFrame, id_column: str = ''):
    """
    Return True (passed) if the provided `id_column` has unique values
    or if any column has unique values and can be used as ID, if `id_column` not given.
    """
    passed = True
    if id_column:
        if not _series_has_unique_values(df[id_column]):
            logger.warning(f"ID column '{id_column}' does not have unique values")
            passed = False
        elif df[id_column].isna().any():
            logger.warning(f"ID column '{id_column}' has N/A values")
            passed = False
    else:
        potential_id_columns = []
        for label, _ in df.items():
            if (df[label].dtype.kind != 'f'
                and _series_has_unique_values(df[label])
                and not df[label].isna().any()
            ):
                potential_id_columns.append(label)
        if not potential_id_columns:
            logger.warning('No columns in the DataFrame can be used as IDs (unique, non-float, no N/As)')
            passed = False
        else:
            logger.info(
                'Potential ID columns (unique values): ' + str(potential_id_columns)
            )
    return passed


def _check_null_cols(df: pd.DataFrame):
    """Return True (passed) if no columns in `df` are completely null."""
    null_columns = []
    for label, _ in df.items():
        if df[label].isna().all():
            null_columns.append(label)
    if null_columns:
        logger.warning(
            'The following columns are completely empty (N/A): ' + str(null_columns)
        )
        return False
    return True


def _check_obj_cols(df: pd.DataFrame):
    """Return True (passed) if no columns in `df` have `object` dtype."""
    obj_columns = []
    for label, _ in df.items():
        if df[label].dtype == np.dtype('O'):
            obj_columns.append(label)
    if obj_columns:
        logger.warning(
            "The following columns have dtype 'object' and will likely cause merge errors: " + str(obj_columns)
        )
        return False
    return True


def _series_has_unique_values(series: pd.Series) -> bool:
    if series.values.shape[0] > series.unique().shape[0]:
        return False
    else:
        return True


def _read_dataframe(df_source):
    """Light wrapper to uniformize parameters, any processing, etc."""
    return pd.read_table(df_source, sep=None, engine='python')


def _decoration_title(df_name: str) -> str:
    return df_name + '\n' + '=' * max(8, len(df_name))


def _depth(on_list: Union[list, str]) -> int:
    """
    Count the depth of a list expected to be a ``on`` merge spec.
    Expects symmetric list contents, i.e. [A, B] and [[A, B], [X, Y]].
    The depth probing is done recursively on the first element.
    """
    if isinstance(on_list, str):
        return 0
    elif isinstance(on_list, list):
        return _depth(on_list[0]) + 1
    else:
        return 0


class TextHeader:
    def __init__(self, text: str = ''):
        self.text = text

    def _repr_html_(self):
        return f"<span style = 'border-bottom: 1px solid #000'><u><b>{self.text}</b></u></span>"

    def __repr__(self):
        return _decoration_title(self.text)


class GroveError(Exception):
    pass
