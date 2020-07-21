import os
import unittest
import shutil
import tempfile

import boto3
import pytest
from moto import mock_s3, mock_sts


@mock_s3
@mock_sts
class TestIO(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()

        self.bucket_name = self.test_dir.split("/")[1]
        s3 = boto3.resource("s3", region_name="eu-west-1")
        s3.create_bucket(Bucket=self.bucket_name)

        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.test_dir)

    def test_unsupported_fs(self):
        from eu_jobs.io import FileSystem

        with pytest.raises(ValueError):
            FileSystem("unsupported file system")

    def test_write_read_local(self):
        from eu_jobs.io import FileSystem

        data_out = "What is my purpose?"
        path = os.path.join(self.test_dir, "my-file.txt")

        with FileSystem().open(path, mode="wt") as fo:
            fo.write(data_out)

        with FileSystem().open(path, mode="rt") as fo:
            data_in = fo.read()

        assert data_out == data_in

    def test_write_read_s3(self):
        from eu_jobs.io import FileSystem

        data_out = b"What is my purpose?"

        path = f"s3://{self.bucket_name}/my-file.txt"

        with FileSystem("s3").open(path, mode="wb") as fo:
            fo.write(data_out)

        with FileSystem("s3").open(path, mode="rb") as fo:
            data_in = fo.read()

        assert data_out == data_in

    def test_list_files(self):
        from eu_jobs.io import FileSystem

        files_in = [
            (os.path.join(self.test_dir, path), file)
            for path, file in [
                ("files", "top"),
                ("files/one", "level"),
                ("files/one/two", "three"),
            ]
        ]

        fs = FileSystem()
        for path, file in files_in:
            fs.makedirs(path, exist_ok=True)
            with fs.open(os.path.join(path, file), mode="wt") as fo:
                fo.write(file)

        result = list(fs.list_files(os.path.join(self.test_dir, "files")))
        expected = [os.path.join(path, file) for path, file in files_in]

        assert sorted(result) == sorted(expected)

        result = list(fs.list_files(os.path.join(self.test_dir, "files", "one")))
        expected = [os.path.join(path, file) for path, file in files_in[1:]]

        assert sorted(result) == sorted(expected)

        result = list(
            fs.list_files(os.path.join(self.test_dir, "files", "one"), recursive=False)
        )
        expected = [os.path.join(path, file) for path, file in files_in[1:2]]

        assert sorted(result) == sorted(expected)

    def test_fs_with_assume(self):
        from eu_jobs.io import FileSystem

        fs = FileSystem("s3", assumed_role="arn:some:random:long:enough:string")

        file_name = os.path.join(self.bucket_name, "test.txt")
        with fs.open(file_name, mode="wt") as fo:
            fo.write("test")

        result = list(fs.list_files(self.bucket_name))

        assert result == [file_name]
