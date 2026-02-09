from pydantic import BaseModel
from pydantic import ConfigDict


class DownloadTarget(BaseModel):
    ref: str
    ref_type: str
    commit_sha: str
    commit_sha_short: str

    model_config = ConfigDict(extra='ignore')
