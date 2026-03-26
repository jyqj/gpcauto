"""Address 路由。"""

from typing import Optional

from fastapi import APIRouter

from .. import address_data
from ..common import JSONDict

router = APIRouter(prefix="/api", tags=["addresses"])


@router.get("/addresses/states")
def list_states(tax_free_only: bool = False) -> JSONDict:
    return {"states": address_data.get_states_list(tax_free_only=tax_free_only)}


@router.get("/addresses/random")
def random_address(
    state: Optional[str] = None,
    zip: Optional[str] = None,
    tax_free_only: bool = False,
) -> JSONDict:
    addr = address_data.get_random_address(state=state, zip_code=zip, tax_free_only=tax_free_only)
    if not addr:
        return {"address": None}
    return {"address": addr}


@router.get("/addresses/reroll-street")
def reroll_street() -> JSONDict:
    return {"address_line1": address_data.generate_street()}


@router.post("/addresses/random-batch")
def random_addresses_batch(
    count: int = 1,
    state: Optional[str] = None,
    zip: Optional[str] = None,
    tax_free_only: bool = False,
) -> JSONDict:
    addrs = address_data.get_random_addresses(
        count=count, state=state, zip_code=zip, tax_free_only=tax_free_only,
    )
    return {"addresses": addrs}
