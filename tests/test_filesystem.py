import os
import unittest
import shutil
import tempfile
import datetime

import boto3
import pytest
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
        from datatoolz.filesystem import FileSystem

        with pytest.raises(ValueError):
            FileSystem("unsupported file system")

    def test_s3_with_assume(self):
        from datatoolz.filesystem import FileSystem

        assume_chains = [
            "arn:some:random:long:enough:string",
            ["arn:some:random:long:enough:string"],
            [
                "arn:some:random:long:enough:string",
                "arn:other:random:long:enough:string",
            ],
        ]

        for roles in assume_chains:
            fs = FileSystem(name="s3", assumed_role=roles)

            file_name = os.path.join(self.bucket_name, "with-assume", "test.txt")
            with fs.open(file_name, mode="wt") as fo:
                fo.write("test")

            result = list(fs.find(path=os.path.join(self.bucket_name, "with-assume")))
            assert [file_name] == result

    def test_filesystem_basics(self):
        from datatoolz.filesystem import FileSystem

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
