from typing import Optional

from pydantic import Field

from backend.db.externals import AssetsOverviewResponse


class AssetResponse(AssetsOverviewResponse):
    asset_key_original: str = Field(exclude=True)
    asset_key_display: Optional[str] = Field(exclude=True)
    asset_key_llm: Optional[str] = Field(exclude=True)
    signed_asset_url: str
