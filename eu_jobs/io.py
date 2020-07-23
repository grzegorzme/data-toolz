import inspect
from uuid import uuid4
import botocore.session
import s3fs
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

LOCAL = "local"
S3 = "s3"


class FileSystem(AbstractFileSystem):
    def __init__(self, type=LOCAL, assumed_role=None, endpoint_url=None):
        super().__init__()
        self.type = type
        self.assumed_role = assumed_role
        self.endpoint_url = endpoint_url

        if self.type == LOCAL:
            self.fs = LocalFileSystem()
        elif self.type == S3:
            session = botocore.session.get_session()
            if self.assumed_role:
                sts = session.create_client("sts")
                response = sts.assume_role(
                    RoleArn=self.assumed_role, RoleSessionName=str(uuid4())
                )
                session.set_credentials(
                    access_key=response["Credentials"]["AccessKeyId"],
                    secret_key=response["Credentials"]["SecretAccessKey"],
                    token=response["Credentials"]["SessionToken"],
                )
            client_kwargs = {"endpoint_url": endpoint_url} if endpoint_url else None
            self.fs = s3fs.S3FileSystem(session=session, client_kwargs=client_kwargs)
        else:
            raise ValueError(f"Unsupported FileReader type: {type}")

        for method_name, method in inspect.getmembers(
            self.fs, predicate=inspect.ismethod
        ):
            if method_name != "__init__":
                setattr(self, method_name, method)
