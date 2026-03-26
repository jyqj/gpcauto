"""公共类型别名、工具函数和共用 Pydantic 模型。"""

from typing import Any

from pydantic import BaseModel

JSONDict = dict[str, Any]


def dump_model(m, **kwargs) -> JSONDict:
    """Pydantic v1/v2 兼容的 model → dict 序列化。"""
    return m.model_dump(**kwargs) if hasattr(m, "model_dump") else m.dict(**kwargs)


class BatchIds(BaseModel):
    """多个路由共用的批量 ID 请求体。"""
    ids: list[int]


def validate_pagination(page: int, page_size: int) -> None:
    """校验分页参数：要么都为 0（全量），要么都为正数（分页）。

    抛出 RequestValidationError 以保持与 FastAPI Query 校验一致的 422 格式。
    """
    if (page > 0) != (page_size > 0):
        from fastapi.exceptions import RequestValidationError

        raise RequestValidationError(
            [
                {
                    "type": "value_error",
                    "loc": ("query", "page" if page_size > 0 else "page_size"),
                    "msg": "page 和 page_size 必须同时为正数或同时不传",
                    "input": page if page_size > 0 else page_size,
                }
            ]
        )
