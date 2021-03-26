"""Module providing a FileSystem wrapper class"""

import inspect
from uuid import uuid4

import botocore.session
import botocore.credentials
import s3fs
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem


class FileSystem(AbstractFileSystem):
    """Wrapper for easier initialization of various file-system classes"""

    def __init__(self, name="local", assumed_role=None, endpoint_url=None):
        super().__init__()
        self.name = name
        self.assume_client = None
        self.assume_role = assumed_role
        self.endpoint_url = endpoint_url

        if self.name == "local":
            self.filesystem = LocalFileSystem()
        elif self.name == "s3":
            session = botocore.session.get_session()
            if self.assume_role:
                self.assume_client = session.create_client("sts")
                session_credentials = (
                    botocore.credentials.RefreshableCredentials.create_from_metadata(
                        metadata=self._sts_refresh(),
                        refresh_using=self._sts_refresh,
                        method="sts-assume-role",
                    )
                )
                session._credentials = session_credentials

            client_kwargs = {"endpoint_url": endpoint_url} if endpoint_url else None
            self.filesystem = s3fs.S3FileSystem(
                session=session, client_kwargs=client_kwargs
            )
        else:
            raise ValueError(f"Unsupported FileReader type: {type}")

        for method_name, method in inspect.getmembers(
            self.filesystem, predicate=inspect.ismethod
        ):
            if method_name not in (
                "__init__",
                "_rm",
                "cp_file",
                "created",
                "ls",
                "modified",
                "sign",
            ):
                setattr(self, method_name, method)

    def _sts_refresh(self):
        """Refresh tokens by calling assume_role again"""
        response = self.assume_client.assume_role(
            RoleArn=self.assume_role,
            RoleSessionName=f"data-toolz-filesystem-s3-{uuid4()}",
            DurationSeconds=3600,
        ).get("Credentials")
        return {
            "access_key": response.get("AccessKeyId"),
            "secret_key": response.get("SecretAccessKey"),
            "token": response.get("SessionToken"),
            "expiry_time": response.get("Expiration").isoformat(),
        }

    def _rm(self, path):
        return self.filesystem.rm(path=path)

    def cp_file(self, path1, path2, **kwargs):
        return self.filesystem.copy(path1=path1, path2=path2, **kwargs)

    def created(self, path):
        return self.filesystem.created(path=path)

    def ls(self, path, detail=True, **kwargs):
        return self.filesystem.ls(path=path, detail=detail, **kwargs)

    def modified(self, path):
        return self.filesystem.modified(path=path)

    def sign(self, path, expiration=100, **kwargs):
        return self.filesystem.sign(path=path, expiration=expiration, **kwargs)
