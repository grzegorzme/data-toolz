"""Module providing a FileSystem wrapper class"""

import inspect

import botocore.session
import botocore.credentials
import s3fs
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from .utils import _getmembers


class FileSystem(AbstractFileSystem):
    """Wrapper for easier initialization of various file-system classes"""

    def __init__(self, name="local", assumed_role=None, endpoint_url=None):
        """
        FileSystem initializer
        :param name: str, filesystem type, supported values [local|s3]
        :param assumed_role: optional str|list,
            permission assume chain - relevant for [s3]
        :param endpoint_url: optional str, override storage service url
        """

        super().__init__()
        self.name = name
        self.assume_client = None
        self.assume_role = assumed_role
        if isinstance(self.assume_role, str):
            self.assume_role = [self.assume_role]
        self.endpoint_url = endpoint_url

        if self.name == "local":
            self.filesystem = LocalFileSystem()
        elif self.name == "s3":
            session = botocore.session.get_session()
            if self.assume_role:
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

        for method_name, method in _getmembers(
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
                "fsid",
            ):
                setattr(self, method_name, method)

    def _sts_refresh(self):
        """Refresh tokens by calling assume_role again"""

        session = botocore.session.get_session()
        credentials = {}
        kwargs = {}

        for role in self.assume_role:
            if credentials:
                kwargs = {
                    "aws_access_key_id": credentials["AccessKeyId"],
                    "aws_secret_access_key": credentials["SecretAccessKey"],
                    "aws_session_token": credentials["SessionToken"],
                }
            assume_client = session.create_client("sts", **kwargs)

            credentials = assume_client.assume_role(
                RoleArn=role,
                RoleSessionName="data-toolz-filesystem-s3",
                DurationSeconds=3600,
            ).get("Credentials")

            setattr(
                session,
                "_credentials",
                botocore.credentials.Credentials(
                    access_key=credentials["AccessKeyId"],
                    secret_key=credentials["SecretAccessKey"],
                    token=credentials["SessionToken"],
                ),
            )

        del session
        return {
            "access_key": credentials.get("AccessKeyId"),
            "secret_key": credentials.get("SecretAccessKey"),
            "token": credentials.get("SessionToken"),
            "expiry_time": credentials.get("Expiration").isoformat(),
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

    @property
    def fsid(self):
        return self.filesystem.fsid()
