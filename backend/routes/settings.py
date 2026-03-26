"""Settings 路由。"""

from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

from .. import db
from ..common import JSONDict, dump_model

router = APIRouter(prefix="/api", tags=["settings"])


@router.get("/settings")
def get_settings() -> JSONDict:
    from ..config import ADS_API, ADS_API_KEY, TABMAIL_URL, TABMAIL_ADMIN_KEY, TABMAIL_TENANT_ID, TABMAIL_ZONE_ID
    defaults = {
        "ads_api": ADS_API,
        "ads_api_key": ADS_API_KEY,
        "tabmail_url": TABMAIL_URL,
        "tabmail_admin_key": TABMAIL_ADMIN_KEY,
        "tabmail_tenant_id": TABMAIL_TENANT_ID,
        "tabmail_zone_id": TABMAIL_ZONE_ID,
    }
    stored = db.get_all_settings()
    merged = {**defaults, **stored}
    return {"settings": merged, "configured_keys": list(stored.keys())}


class SettingsUpdate(BaseModel):
    ads_api: Optional[str] = None
    ads_api_key: Optional[str] = None
    tabmail_url: Optional[str] = None
    tabmail_admin_key: Optional[str] = None
    tabmail_tenant_id: Optional[str] = None
    tabmail_zone_id: Optional[str] = None


@router.put("/settings")
def update_settings(body: SettingsUpdate) -> JSONDict:
    for key, val in dump_model(body, exclude_unset=True).items():
        db.set_setting(key, val if val is not None else "")
    return {"ok": True}
