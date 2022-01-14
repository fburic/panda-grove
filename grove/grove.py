from pathlib import Path
from typing import Iterable, Union

import pandas as pd


class Collection:
    """
    Collection: A container class to manage DataFrames

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

    def __init__(self, data_sources: Union[dict, Iterable] = None, schema=None):
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
        Merge multiple DataFrames in the Collection

        Examples
        --------
        >>> data.merge(['items', 'categories', 'measurements'], on='id')

        >>> data.merge(['items', 'categories', 'measurements'],
        ...            on=['id', 'id', 'id_2']

        :param df_name_list: Names of collection DataFrame to merge
        :param on: Column names to join on.
                   Either a single string or omitted if it's the same across all
                   DataFrames. If column names to join on differ, these are given
                   as a list.
        :return: Merged DataFrame
        """
        if on is None:
            id_list = [None for _ in range(len(df_name_list))]
        elif isinstance(on, str):
            id_list = [on for _ in range(len(df_name_list))]
        elif isinstance(on, list):
            id_list = on
        else:
            raise GroveError(f'Specification of columns to merge on not supported: {on}')

        for i, col_id in enumerate(id_list):
            if col_id is not None and col_id not in self._data_frames[df_name_list[i]]:
                raise GroveError(f"Column '{col_id}' not present in '{df_name_list[i]}'")

        merged_df = self._data_frames[df_name_list[0]]
        for i in range(1, len(df_name_list)):
            merged_df = pd.merge(
                merged_df,
                self._data_frames[df_name_list[i]],
                left_on=id_list[i - 1],
                right_on=id_list[i]
            )
        return merged_df

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
                print(df_name + '\n' + '=' * max(8, len(df_name)) + '\n')
                print(self._data_frames[df_name].info(memory_usage=not memory_usage))
                print()


def _read_dataframe(df_source):
    """Light wrapper to uniformize parameters, any processing, etc."""
    return pd.read_table(df_source, sep=None, engine='python')


class GroveError(Exception):
    pass
