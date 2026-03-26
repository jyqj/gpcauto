"""Cards 路由。"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from .. import db, card_service
from ..common import JSONDict, dump_model, BatchIds, validate_pagination
from ..constants import CARD_STATUS_AVAILABLE, CARD_STATUS_DISABLED

router = APIRouter(prefix="/api", tags=["cards"])


@router.get("/cards")
def list_cards(
    page: int = Query(0, ge=0),
    page_size: int = Query(0, ge=0, le=200),
    search: str = "",
    status: str = "",
    fail_tag: str = "",
    sort: str = "",
    order: str = "desc",
) -> JSONDict:
    validate_pagination(page, page_size)
    qkw = dict(search=search, status=status, fail_tag=fail_tag, sort=sort, order=order)
    result: JSONDict = {"cards": db.list_cards(page=page, page_size=page_size, **qkw)}
    if page > 0 and page_size > 0:
        result["total"] = db.count_cards(search=search, status=status, fail_tag=fail_tag)
        result["page"] = page
        result["page_size"] = page_size
    return result


class CardCreate(BaseModel):
    number: str = Field(..., min_length=13, max_length=19, pattern=r"^\d{13,19}$")
    exp_month: str = Field("", pattern=r"^(0[1-9]|1[0-2])?$")
    exp_year: str = Field("", pattern=r"^(\d{4})?$")
    cvv: str = Field("", pattern=r"^(\d{3,4})?$")
    holder_name: str = ""
    address_line1: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    country: str = Field("US", min_length=2, max_length=3)

    @field_validator("exp_year")
    @classmethod
    def _normalize_year(cls, v: str) -> str:
        if v and len(v) == 2:
            return "20" + v
        return v


@router.post("/cards")
def add_card(body: CardCreate) -> JSONDict:
    try:
        cid = card_service.add_card(dump_model(body))
    except card_service.DuplicateCardError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except card_service.CardExpiredError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"id": cid}


class CardBatch(BaseModel):
    raw: str


@router.post("/cards/batch")
def add_cards_batch(body: CardBatch) -> JSONDict:
    return card_service.add_cards_batch(body.raw)


@router.post("/cards/preview")
def preview_cards(body: CardBatch) -> JSONDict:
    results = []
    for line in body.raw.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parsed = card_service.parse_card_line(line)
        if parsed:
            results.append({"status": "ok", **parsed})
        else:
            results.append({"status": "fail", "raw": line[:120]})
    return {"results": results}


class CardSaveBatch(BaseModel):
    cards: list[CardCreate]


@router.post("/cards/save-batch")
def save_cards_batch(body: CardSaveBatch) -> JSONDict:
    ids = []
    skipped = 0
    expired = 0
    errors: list[JSONDict] = []
    for c in body.cards:
        try:
            cid = card_service.add_card(dump_model(c))
            ids.append(cid)
        except card_service.DuplicateCardError:
            skipped += 1
        except card_service.CardExpiredError as e:
            expired += 1
            errors.append({
                "status": "expired",
                "number": f"****{c.number[-4:]}",
                "message": str(e),
            })
    result = {
        "imported": len(ids),
        "skipped": skipped,
        "expired": expired,
        "ids": ids,
        "errors": errors,
    }
    if ids:
        db.audit_log("card", None, "batch_import", f"导入 {len(ids)} 张, 跳过 {skipped}, 过期 {expired}")
    return result


@router.delete("/cards/{card_id}")
def remove_card(card_id: int) -> JSONDict:
    db.delete_card(card_id)
    return {"ok": True}


@router.post("/cards/batch-delete")
def batch_delete_cards(body: BatchIds) -> JSONDict:
    db.delete_cards(body.ids)
    db.audit_log("card", None, "batch_delete", f"删除 {len(body.ids)} 张卡")
    return {"ok": True, "deleted": len(body.ids)}


class BatchCardStatus(BaseModel):
    ids: list[int]
    status: str


@router.post("/cards/batch-status")
def batch_update_card_status(body: BatchCardStatus) -> JSONDict:
    if body.status not in (CARD_STATUS_AVAILABLE, CARD_STATUS_DISABLED):
        raise HTTPException(status_code=400, detail="invalid status")
    db.batch_update_cards(body.ids, status=body.status)
    db.audit_log("card", None, "batch_status", f"{len(body.ids)} 张卡 → {body.status}")
    return {"ok": True}


class BatchCardFailTag(BaseModel):
    ids: list[int]
    fail_tag: str = ""


@router.post("/cards/batch-fail-tag")
def batch_update_card_fail_tag(body: BatchCardFailTag) -> JSONDict:
    db.batch_update_cards(body.ids, fail_tag=body.fail_tag)
    return {"ok": True}


class BatchCardAddress(BaseModel):
    ids: list[int]
    address_line1: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    country: str = "US"


@router.post("/cards/batch-address")
def batch_update_card_address(body: BatchCardAddress) -> JSONDict:
    addr = {k: v for k, v in dump_model(body).items() if k != "ids"}
    db.batch_update_card_address(body.ids, **addr)
    return {"ok": True, "updated": len(body.ids)}
