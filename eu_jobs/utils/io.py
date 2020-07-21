import os
from uuid import uuid4
import botocore.session
import s3fs


class FileSystem:
    def __init__(self, name="local", assumed_role=None, endpoint_url=None):
        self.name = name
        self.assumed_role = assumed_role
        self.endpoint_url = endpoint_url

        if self.name == "local":
            self.fs = None
            self.open = open
            self.walk = os.walk
            self.makedirs = os.makedirs
        elif self.name == "s3":
            session = self._get_session()
            client_kwargs = {"endpoint_url": endpoint_url} if endpoint_url else None
            self.fs = s3fs.S3FileSystem(session=session, client_kwargs=client_kwargs)
            self.open = self.fs.open
            self.walk = self.fs.walk
            self.makedirs = self.fs.makedirs
        else:
            raise ValueError(f"Unsupported FileReader type: {name}")

    def _get_session(self):
        session = botocore.session.get_session()

        # todo cleanup this after problem solved
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
        return session

    def list_files(self, path, recursive=True):
        for level, (r, d, f) in enumerate(self.walk(path)):
            for file in f:
                yield os.path.join(r, file)
            if not recursive and level >= 0:
                break
