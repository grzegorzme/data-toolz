"""Module providing a DataIO wrapper class"""

import os
import io
import time
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
from gzip import compress, decompress

import pandas as pd


class DataIO:
    """Class for data writing/reading"""

    def __init__(
        self,
        filesystem=None,
        partition_transformer=None,
    ):
        """
        DataIO initializer
        :param filesystem: optional, FileSystem object, default FileSystem("local")
        :param partition_transformer: optional,
          function(prefix, partitions, values, suffix): custom function
          for transforming output partition naming
        """
        self.filesystem = FileSystem() if filesystem is None else filesystem
        self.partition_transformer = (
            self.get_path if partition_transformer is None else partition_transformer
        )

    @staticmethod
    def get_path(prefix="", partitions=None, values=None, suffix=""):
        """
        Helper for formatting paths
        :param prefix: path prefix
        :param partitions: optional, list-like - partitioning fields
        :param values: optional, list-like - partition field-values
        :param suffix: optional, path-suffix (filename)
        :return: path-like
        e.g. ("prefix", ["a", "b"], [1, 2], "suffix") -> "prefix/a=1/b=2/suffix"
        """
        partitions = partitions or list()
        values = values or list()
        if len(partitions) != len(values) or any(v is None for v in values):
            raise ValueError(
                "`partitions` and `values` lengths must match "
                "and `values` must not contain NoneType"
            )

        return os.path.join(
            prefix,
            *(f"{field}={value}" for field, value in zip(partitions, values)),
            f"{time.time_ns()}-{uuid4()}" if suffix is None else suffix,
        )

    def get_partitions(
        self, dataframe, partition_by=None, prefix="", suffix="", drop=False
    ):
        """
        Partitions dataframe based on selected fields
        :param dataframe: pandas.DataFrame
        :param partition_by: optional, list of columns to partition the output
        :param prefix: optional, prefix for partition name
        :param suffix: optional, suffix for partition name
        :param drop: bool, should the partition fields be dropped from the output
        :return: generator of (partition_name: str, partition_data: pandas.DataFrame)
        """
        if partition_by is None:
            yield prefix, dataframe
        else:
            for group, partition in dataframe.groupby(partition_by):
                if isinstance(group, str):
                    group = (group,)
                partition_path = self.partition_transformer(
                    prefix=prefix, partitions=partition_by, values=group, suffix=suffix
                )
                if drop:
                    partition = partition.drop(partition_by, axis=1)
                yield partition_path, partition

    def read(self, path, filetype="parquet", gzip=False, header=None):
        """
        Function for reading (partitioned) data sets from a given path
        :param path: path-like
        :param filetype: input format type: parquet|tsv|jsonlines
        :param gzip: is the input compressed, only used for tsv|jsonlines
        :param header: bool - True if input files has headers
        :return: pandas.DataFrame
        """

        def _deserialize(data, filetype, gzip, header):
            if gzip:
                data = decompress(data)

            if filetype == "tsv":
                data = pd.read_csv(
                    io.StringIO(data.decode(encoding="utf-8")),
                    sep="\t",
                    header=0 if header is True else None,
                    dtype=str,
                    keep_default_na=False,
                )
            elif filetype == "jsonlines":
                data = pd.read_json(
                    io.StringIO(data.decode(encoding="utf-8")),
                    orient="records",
                    lines=True,
                    dtype=False,
                )
            else:
                raise ValueError(f"Unsupported output format: {filetype}")
            return data

        def _read(path, filetype, gzip, header):

            if filetype == "parquet":
                data = pd.read_parquet(path=path, filesystem=self.filesystem)
            else:
                with self.filesystem.open(path, mode="rb") as file_object:
                    data = _deserialize(
                        data=file_object.read(),
                        filetype=filetype,
                        gzip=gzip,
                        header=header,
                    )
            return data

        with ThreadPoolExecutor() as pool:
            data = list(
                pool.map(
                    lambda x: _read(
                        path=x,
                        filetype=filetype,
                        gzip=gzip,
                        header=header,
                    ),
                    self.filesystem.find(path=path),
                )
            )

        return (
            pd.concat(data).reset_index(drop=True) if len(data) > 0 else pd.DataFrame()
        )

    def write(
        self,
        dataframe,
        path,
        filetype="parquet",
        gzip=False,
        header=False,
        partition_by=None,
        suffix=None,
        drop_partitions=False,
    ):
        """
        Saves data in a given format
        :param dataframe: input pandas.DataFrame
        :param path: path-like
        :param filetype: output format: parquet|tsv|jsonlines
        :param gzip: should the output be gzipped: tsv|jsonlines
        :param header: should the columns names be included: tsv-only
        :param partition_by: optional, list of columns to partition the output
        :param suffix: optional, suffix to use for output partitions
        :param drop_partitions: optional, bool - if to drop partition fields from output
        """

        def _serialize(dataframe, filetype, gzip, header):
            if filetype == "tsv":
                data = dataframe.to_csv(sep="\t", index=False, header=header)
            elif filetype == "jsonlines":
                data = dataframe.to_json(orient="records", lines=True)
            else:
                raise ValueError(f"Unsupported output format: {filetype}")
            data = data.encode("utf-8")
            if gzip:
                data = compress(data)
            return data

        def _write(path, dataframe, filetype, gzip, header):
            self.filesystem.makedirs(os.path.dirname(path), exist_ok=True)
            if filetype == "parquet":
                dataframe.to_parquet(path=path, filesystem=self.filesystem)
            else:
                data = _serialize(
                    dataframe=dataframe, filetype=filetype, gzip=gzip, header=header
                )
                with self.filesystem.open(path, mode="wb") as file_object:
                    file_object.write(data)

        pipeline = self.get_partitions(
            dataframe=dataframe,
            partition_by=partition_by,
            prefix=path,
            suffix=suffix,
            drop=drop_partitions,
        )

        with ThreadPoolExecutor() as pool:
            list(
                pool.map(
                    lambda x: _write(
                        path=x[0],
                        dataframe=x[1],
                        filetype=filetype,
                        gzip=gzip,
                        header=header,
                    ),
                    pipeline,
                )
            )
