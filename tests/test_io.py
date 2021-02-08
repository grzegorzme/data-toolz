import os
import unittest
import shutil
import tempfile

import boto3
import pytest
import pandas as pd
from moto import mock_s3

from datatoolz.filesystem import FileSystem


@mock_s3
class TestDataIO(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

        self.bucket_name = "test_bucket"
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=self.bucket_name)

        self.sample_df = pd.DataFrame(
            {
                "col1": ["a", "a", "b", "b", "b"],
                "col2": [1, 1, 1, 1, 2],
                "col3": [1, None, 123, -42, 0],
            }
        )

        self.params = [
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "dsv",
                "gzip": False,
                "sep": "|",
                "header": True,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "dsv",
                "gzip": False,
                "sep": "|",
                "header": False,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "dsv",
                "gzip": True,
                "sep": "|",
                "header": True,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "dsv",
                "gzip": True,
                "sep": "|",
                "header": False,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "dsv",
                "gzip": False,
                "sep": "|",
                "header": True,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "dsv",
                "gzip": False,
                "sep": "|",
                "header": False,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "dsv",
                "gzip": True,
                "sep": "|",
                "header": True,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "dsv",
                "gzip": True,
                "sep": "|",
                "header": False,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "jsonlines",
                "gzip": True,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "jsonlines",
                "gzip": False,
            },
            {"filesystem": "local", "path": self.test_dir, "filetype": "parquet"},
            {
                "filesystem": "s3",
                "path": self.bucket_name,
                "filetype": "dsv",
                "gzip": False,
                "sep": "|",
                "header": True,
            },
            {
                "filesystem": "s3",
                "path": self.bucket_name,
                "filetype": "dsv",
                "gzip": False,
                "sep": "|",
                "header": False,
            },
            {
                "filesystem": "s3",
                "path": self.bucket_name,
                "filetype": "dsv",
                "gzip": True,
                "sep": "|",
                "header": True,
            },
            {
                "filesystem": "s3",
                "path": self.bucket_name,
                "filetype": "dsv",
                "gzip": True,
                "sep": "|",
                "header": False,
            },
            {
                "filesystem": "s3",
                "path": self.bucket_name,
                "filetype": "jsonlines",
                "gzip": True,
            },
            {
                "filesystem": "s3",
                "path": self.bucket_name,
                "filetype": "jsonlines",
                "gzip": False,
            },
            {"filesystem": "s3", "path": self.bucket_name, "filetype": "parquet"},
        ]

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def test_write_read_basics(self):
        from datatoolz.io import DataIO

        for i, params in enumerate(self.params):

            filesystem = FileSystem(params["filesystem"])

            dio = DataIO(filesystem=filesystem)

            path = os.path.join(
                params["path"],
                str(i),
                params["filetype"],
                f"my-file.{params['filetype']}",
            )

            dio.write(
                dataframe=self.sample_df,
                path=path,
                filetype=params["filetype"],
                gzip=params.get("gzip"),
                sep=params.get("sep"),
                header=params.get("header"),
            )

            df = dio.read(
                path=path,
                filetype=params["filetype"],
                gzip=params.get("gzip"),
                sep=params.get("sep"),
                header=params.get("header"),
            )

            assert self.sample_df.shape == df.shape

    def test_unsupported_filetype(self):
        from datatoolz.io import DataIO

        path = os.path.join(self.test_dir, "myfile.parquet")

        dio = DataIO()
        with pytest.raises(ValueError):
            dio.write(
                dataframe=self.sample_df, path=self.test_dir, filetype="unsupported"
            )

        dio.write(dataframe=self.sample_df, path=path)
        with pytest.raises(ValueError):
            dio.read(path=path, filetype="unsupported")

    def test_write_read_with_partitions(self):
        from datatoolz.io import DataIO

        filesystem = FileSystem()
        dio = DataIO(filesystem=filesystem)

        partition_by = [self.sample_df.columns[0]]

        path = os.path.join(self.test_dir, "parts1", "my-file")
        dio.write(dataframe=self.sample_df, path=path, partition_by=partition_by)
        files = filesystem.find(path=path)
        assert len(files) == self.sample_df[partition_by].drop_duplicates().shape[0]
        df = dio.read(path=files[0])
        head, suffix = os.path.split(files[0])
        col, val = os.path.split(head)[1].split("=")
        assert df.shape[0] == (self.sample_df[col] == val).sum()

        path = os.path.join(self.test_dir, "parts-drop", "my-file")
        dio.write(
            dataframe=self.sample_df,
            path=path,
            partition_by=partition_by,
            drop_partitions=True,
        )
        df = dio.read(path=path)
        assert df.shape == (
            self.sample_df.shape[0],
            self.sample_df.shape[1] - len(partition_by),
        )
        assert list(df.columns) == [
            col for col in self.sample_df.columns if col not in partition_by
        ]

    def test_write_custom_partition_formatting(self):

        from datatoolz.io import DataIO

        filesystem = FileSystem()

        def custom_formatter(prefix, partitions, values, suffix, drop_prefixes):
            partitions = partitions or list()
            values = values or list()
            drop_prefixes = drop_prefixes or list()
            return os.path.join(
                prefix,
                *(
                    f"{value}" if field in drop_prefixes else f"{field}*{value}"
                    for field, value in zip(partitions, values)
                ),
                "default" if suffix is None else suffix,
            )

        partition_by = [self.sample_df.columns[0], self.sample_df.columns[1]]
        dio = DataIO(
            filesystem=filesystem,
            partition_transformer=lambda prefix, partitions, values, suffix: custom_formatter(
                prefix,
                partitions,
                values,
                suffix,
                drop_prefixes=[self.sample_df.columns[0]],
            ),
        )

        path = os.path.join(self.test_dir, "parts-custom-format", "my-file")
        dio.write(
            dataframe=self.sample_df,
            path=path,
            partition_by=partition_by,
        )
        files = filesystem.find(path=path)

        partition_data = self.sample_df[partition_by].drop_duplicates()
        assert len(files) == partition_data.shape[0]

        files = [os.path.dirname(f) for f in files]

        for _, x in partition_data.iterrows():
            partition_name = list()
            for i, col in enumerate(partition_by):
                if i == 0:
                    partition_name.append(x[col])
                else:
                    partition_name.append(f"{col}*{x[col]}")
            partition_name = os.path.join(path, *partition_name)
            assert partition_name in files

        with pytest.raises(ValueError):
            dio.get_path(
                prefix="prefix", partitions=["a"], values=[1, 2], suffix="suffix"
            )

        with pytest.raises(ValueError):
            dio.get_path(
                prefix="prefix",
                partitions=["a", "b"],
                values=[1, None],
                suffix="suffix",
            )

    def test_tsv_deprecation(self):
        from datatoolz.io import DataIO

        dio = DataIO()

        path = os.path.join(
            self.test_dir,
            "tsv-deprecate",
            "my-file.tsv",
        )

        with pytest.warns(DeprecationWarning):
            dio.write(dataframe=self.sample_df, path=path, filetype="tsv")

        with pytest.warns(DeprecationWarning):
            df = dio.read(
                path=path,
                filetype="tsv",
            )

        assert self.sample_df.shape == df.shape
