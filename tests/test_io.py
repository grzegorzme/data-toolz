import os
import unittest
import shutil
import tempfile
import datetime

import boto3
import pytest
import pandas as pd
from moto import mock_s3, mock_sts


@mock_s3
@mock_sts
class TestFileSystem(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

        self.bucket_name = "test_bucket"
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=self.bucket_name)

        self.basic_params = [
            ("local", self.test_dir),
            ("s3", f"s3://{self.bucket_name}"),
        ]

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def test_unsupported_fs(self):
        from datatoolz.io import FileSystem

        with pytest.raises(ValueError):
            FileSystem("unsupported file system")

    def test_s3_with_assume(self):
        from datatoolz.io import FileSystem

        fs = FileSystem(name="s3", assumed_role="arn:some:random:long:enough:string")

        file_name = os.path.join(self.bucket_name, "with-assume", "test.txt")
        with fs.open(file_name, mode="wt") as fo:
            fo.write("test")

        result = list(fs.find(path=os.path.join(self.bucket_name, "with-assume")))
        assert [file_name] == result

    def test_filesystem_basics(self):
        from datatoolz.io import FileSystem

        for filesystem, path in self.basic_params:
            with self.subTest(msg=filesystem):
                fs = FileSystem(name=filesystem)

                path = os.path.join(path, filesystem)
                fs.makedirs(path=path, exist_ok=False)

                path1 = os.path.join(path, "file1")
                path2 = os.path.join(path, "file2")

                text_out = f"fs: {filesystem}"
                with fs.open(path1, mode="wt") as fo:
                    fo.write(f"fs: {filesystem}")

                with fs.open(path1, mode="rt") as fo:
                    text_in = fo.read()
                assert text_in == text_out

                binary_out = text_out.encode("utf-8")
                with fs.open(path1, mode="wb") as fo:
                    fo.write(binary_out)

                with fs.open(path1, mode="rb") as fo:
                    binary_in = fo.read()
                assert binary_out == binary_in

                if filesystem == "s3":
                    # not implemented for s3
                    with pytest.raises(NotImplementedError):
                        fs.created(path=path1)

                    # not implemented for s3
                    with pytest.raises(NotImplementedError):
                        fs.modified(path=path)

                    # not implemented for s3
                    with pytest.raises(NotImplementedError):
                        fs.sign(path=path1)

                elif filesystem == "local":
                    created = fs.created(path=path1)
                    assert isinstance(created, datetime.datetime)

                    mod = fs.modified(path=path)
                    assert isinstance(mod, datetime.datetime)

                    with pytest.raises(NotImplementedError):
                        fs.sign(path=path1)

                else:
                    raise ValueError(f"Unknown FS: {filesystem}")

                ls = fs.ls(path=path)
                assert len(ls) == 1

                fs.cp_file(path1=path1, path2=path2)
                ls = fs.ls(path=path)
                assert len(ls) == 2

                fs._rm(path=path1)
                ls = fs.ls(path=path)
                assert len(ls) == 1


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
                "filetype": "tsv",
                "gzip": False,
                "header": True,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "tsv",
                "gzip": False,
                "header": False,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "tsv",
                "gzip": True,
                "header": True,
            },
            {
                "filesystem": "local",
                "path": self.test_dir,
                "filetype": "tsv",
                "gzip": True,
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
                "filetype": "tsv",
                "gzip": False,
                "header": True,
            },
            {
                "filesystem": "s3",
                "path": self.bucket_name,
                "filetype": "tsv",
                "gzip": False,
                "header": False,
            },
            {
                "filesystem": "s3",
                "path": self.bucket_name,
                "filetype": "tsv",
                "gzip": True,
                "header": True,
            },
            {
                "filesystem": "s3",
                "path": self.bucket_name,
                "filetype": "tsv",
                "gzip": True,
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
        from datatoolz.io import DataIO, FileSystem

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
                header=params.get("header"),
            )

            df = dio.read(
                path=path,
                filetype=params["filetype"],
                gzip=params.get("gzip"),
                header=params.get("header"),
            )

            print(params)
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
        from datatoolz.io import FileSystem, DataIO

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
        from datatoolz.io import FileSystem, DataIO

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
