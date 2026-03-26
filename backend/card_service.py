"""卡池管理 & 智能卡片解析 & 绑卡自动化"""

import datetime
import logging
import random
import re
import time
from typing import Callable, Optional

from playwright.sync_api import Page

from . import db, browser_utils

log = logging.getLogger("reg.card")


def _interruptible_sleep(seconds: float, check_abort: Optional[Callable[[], bool]] = None) -> None:
    """Sleep that checks abort every 0.5s."""
    if not check_abort:
        time.sleep(seconds)
        return
    end = time.time() + seconds
    while time.time() < end:
        if check_abort():
            from .register import TaskAborted
            raise TaskAborted("任务已中断")
        time.sleep(min(0.5, end - time.time()))


class CardBindError(Exception):
    """Base exception for card binding failures."""
    pass

class DuplicateCardError(Exception):
    """Card number already exists in the pool."""
    def __init__(self, existing_id: int) -> None:
        self.existing_id = existing_id
        super().__init__(f"卡号已存在 (ID #{existing_id})")


class CardExpiredError(Exception):
    """Card is already expired."""
    def __init__(self, exp_month: str, exp_year: str) -> None:
        self.exp_month = exp_month
        self.exp_year = exp_year
        super().__init__(f"卡片已过期: {exp_month}/{exp_year}")


def is_card_expired(exp_month: str, exp_year: str) -> bool:
    """判断卡片是否已过期。无过期日期的卡不判为过期。"""
    if not exp_month or not exp_year:
        return False
    try:
        year = int(exp_year)
        month = int(exp_month)
        now = datetime.datetime.now()
        # 卡片在到期月的最后一天才真正过期
        if year < now.year:
            return True
        if year == now.year and month < now.month:
            return True
        return False
    except (ValueError, TypeError):
        return False

class CardDeclined(CardBindError):
    """Card was declined (insufficient funds, expired, invalid, etc.)."""
    pass

class Card3DSFailed(CardBindError):
    """3D Secure verification triggered — cannot auto-complete."""
    pass


def classify_bind_error(msg: str) -> CardBindError:
    """Turn an error message string into a typed exception."""
    low = msg.lower()
    if "3ds" in low or "3d secure" in low or "3DS_CHALLENGE" in msg:
        return Card3DSFailed(msg)
    if any(kw in low for kw in [
        "insufficient", "declined", "expired", "invalid",
        "not supported", "try a different", "unable to process",
    ]):
        return CardDeclined(msg)
    return CardBindError(msg)


def _make_flow_helpers(
    on_log: Optional[Callable[[str], None]],
    check_abort: Optional[Callable[[], bool]],
) -> tuple[Callable[[str], None], Callable[[float, float], None]]:
    def _log(msg: str) -> None:
        log.info(msg)
        if on_log:
            on_log(msg)

    def _pause(lo: float = 1.0, hi: float = 2.5) -> None:
        _interruptible_sleep(random.uniform(lo, hi), check_abort)

    return _log, _pause


def _raise_typed_error(error: Optional[str], prefix: str, on_log: Callable[[str], None]) -> None:
    if not error:
        return
    exc = classify_bind_error(error)
    on_log(f"{prefix}: {type(exc).__name__} — {error}")
    raise exc


def _confirm_payment_if_present(
    page: Page,
    amount: int,
    on_log: Callable[[str], None],
    pause: Callable[[float, float], None],
) -> None:
    confirm_text = page.evaluate("""() => {
        for (const b of document.querySelectorAll('button')) {
            if (b.textContent.trim() === 'Confirm payment' && b.offsetParent)
                return true;
        }
        return false;
    }""")
    if confirm_text:
        on_log(f"确认付款 ${amount}...")
        browser_utils.click_button_by_text(page, "Confirm payment")
        pause(2.0, 4.0)


# ─── 解析辅助 ──────────────────────────────────────────────

_CARD_KEYWORDS = {
    "mastercard", "visa", "amex", "discover", "jcb", "diners",
    "unionpay", "mir", "elo", "maestro",
    "credit", "debit", "prepaid",
    "standard", "platinum", "gold", "classic", "signature", "business",
    "world", "elite", "corporate", "infinite",
    "live", "dead",
}


def _is_email(s: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s.strip()))


def _has_letters(s: str) -> bool:
    """Any Unicode letter character (Latin, Hebrew, CJK, etc.)."""
    return bool(re.search(r"[^\W\d_]", s, re.UNICODE))


def _is_name_part(s: str) -> bool:
    s = s.strip()
    if not s or _is_email(s):
        return False
    if re.match(r"^[\d\s+\-()\/.]+$", s):
        return False
    if s.lower() in _CARD_KEYWORDS:
        return False
    return _has_letters(s)


def _is_country_like(s: str) -> bool:
    """All-uppercase Latin text that looks like a country name or ISO code."""
    s = s.strip()
    if not s:
        return False
    if re.match(r"^[A-Z]{2,3}$", s):
        return True
    if re.match(r"^[A-Z][A-Z\s]{0,25}$", s) and len(s.split()) <= 3:
        return True
    return False


def _norm_exp(raw: str) -> tuple[str, str]:
    """Parse 'MM/YY' or 'MM/YYYY' into (month, year_4digit)."""
    m = re.match(r"(\d{1,2})\s*/\s*(\d{2,4})", raw.strip())
    if not m:
        return "", ""
    month = m.group(1).zfill(2)
    year = m.group(2)
    if len(year) == 2:
        year = "20" + year
    return month, year


# ─── Pipe-separated parser (formats 1/2/3) ────────────────


def _extract_holder_pipe(fields: list[str]) -> str:
    """从 card|exp|cvv 之后的字段中提取持卡人姓名。

    Format 1: Name|Address|City|State|Zip|Country|…
    Format 2: email|LastName|FirstName|…
    Format 3: COUNTRY  (no name)
    """
    if not fields:
        return ""

    f0 = fields[0].strip()

    if _is_email(f0):
        # email|Last|First pattern
        if len(fields) >= 3 and _is_name_part(fields[1]) and _is_name_part(fields[2]):
            return f"{fields[2].strip()} {fields[1].strip()}"
        return ""

    if _is_country_like(f0):
        return ""
    if f0.lower() in _CARD_KEYWORDS:
        return ""
    if _has_letters(f0):
        return f0
    return ""


def _parse_pipe(line: str) -> Optional[dict]:
    parts = [p.strip() for p in line.split("|")]
    if len(parts) < 3:
        return None

    card_number = re.sub(r"\D", "", parts[0])
    if not 13 <= len(card_number) <= 19:
        return None

    exp_month, exp_year = _norm_exp(parts[1])
    if not exp_month:
        return None

    cvv = parts[2]
    if not re.match(r"^\d{3,4}$", cvv):
        return None

    holder = _extract_holder_pipe(parts[3:])
    return {
        "number": card_number,
        "exp_month": exp_month,
        "exp_year": exp_year,
        "cvv": cvv,
        "holder_name": holder,
    }


# ─── Space/tab-separated parser (format 4) ────────────────


def _parse_space(line: str) -> Optional[dict]:
    parts = re.split(r"\t+|\s{2,}", line.strip())
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) < 3:
        return None

    card_number = ""
    card_idx = -1
    for i, p in enumerate(parts):
        d = re.sub(r"\D", "", p)
        if 13 <= len(d) <= 19:
            card_number = d
            card_idx = i
            break
    if not card_number:
        return None

    short_nums: list[tuple[str, str]] = []  # (tag, value)
    name_candidates: list[str] = []

    for i, p in enumerate(parts):
        if i == card_idx or not p:
            continue
        if re.match(r"^\d{3,4}$", p):
            short_nums.append(("cvv", p))
        elif re.match(r"^\d{1,2}$", p):
            short_nums.append(("small", p))
        elif _is_email(p) or re.match(r"^\d{5,}$", p):
            continue
        elif p.lower() in _CARD_KEYWORDS or _is_country_like(p):
            continue
        elif _has_letters(p):
            name_candidates.append(p)

    cvv = ""
    remaining: list[str] = []
    for tag, val in short_nums:
        if not cvv and tag == "cvv":
            cvv = val
        else:
            remaining.append(val)

    exp_month = exp_year = ""
    if len(remaining) >= 2:
        a, b = remaining[0], remaining[1]
        try:
            if 1 <= int(a) <= 12:
                exp_month = a.zfill(2)
                exp_year = b if len(b) == 4 else "20" + b.zfill(2)
            else:
                exp_year = a if len(a) == 4 else "20" + a.zfill(2)
                exp_month = b.zfill(2)
        except ValueError:
            pass

    if not cvv:
        return None
    # 过期日期缺失则视为解析失败，避免绑卡时发空值给 Stripe
    if not exp_month or not exp_year:
        return None

    return {
        "number": card_number,
        "exp_month": exp_month,
        "exp_year": exp_year,
        "cvv": cvv,
        "holder_name": name_candidates[0] if name_candidates else "",
    }


# ─── Unified entry point ──────────────────────────────────


def parse_card_line(line: str) -> Optional[dict]:
    """自动识别格式并解析一行卡片数据。"""
    line = line.strip()
    if not line:
        return None
    return _parse_pipe(line) if "|" in line else _parse_space(line)


# ─── Public API ────────────────────────────────────────────


def add_card(data: dict[str, str]) -> int:
    number = data.get("number", "")
    existing = db.card_exists(number)
    if existing:
        raise DuplicateCardError(existing)
    if is_card_expired(data.get("exp_month", ""), data.get("exp_year", "")):
        raise CardExpiredError(data.get("exp_month", ""), data.get("exp_year", ""))
    return db.insert_card(data)


def add_cards_batch(raw: str) -> dict[str, object]:
    """批量导入，返回 {imported, failed, results}。"""
    results = []
    imported = failed = 0
    duplicated = 0
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parsed = parse_card_line(line)
        if parsed:
            existing = db.card_exists(parsed["number"])
            if existing:
                duplicated += 1
                results.append({
                    "status": "duplicate",
                    "number": "****" + parsed["number"][-4:],
                    "existing_id": existing,
                })
                continue
            if is_card_expired(parsed.get("exp_month", ""), parsed.get("exp_year", "")):
                failed += 1
                results.append({
                    "status": "expired",
                    "number": "****" + parsed["number"][-4:],
                    "exp": f"{parsed['exp_month']}/{parsed['exp_year']}",
                })
                continue
            cid = db.insert_card(parsed)
            imported += 1
            results.append({
                "status": "ok",
                "id": cid,
                "number": "****" + parsed["number"][-4:],
                "exp": f"{parsed['exp_month']}/{parsed['exp_year']}",
                "holder_name": parsed["holder_name"],
            })
        else:
            failed += 1
            results.append({
                "status": "fail",
                "raw": line[:80],
            })
    return {"imported": imported, "failed": failed, "duplicated": duplicated, "results": results}


def get_available_card() -> Optional[dict[str, object]]:
    return db.get_available_card()


def mark_card_used(card_id: int, account_id: int) -> None:
    db.mark_card_used(card_id, account_id)


def bind_card_billing(
    page: Page,
    card: dict,
    address: dict,
    holder_name: str,
    credit_amount: int = 5,
    on_log: Optional[Callable[[str], None]] = None,
    check_abort: Optional[Callable[[], bool]] = None,
) -> None:
    """Bind card and purchase credits via the Billing page."""
    _log, _pause = _make_flow_helpers(on_log, check_abort)

    credit_amount = max(5, min(100, credit_amount))

    _log("打开 Payment methods 页面...")
    browser_utils.goto_interruptible(
        page,
        "https://platform.openai.com/settings/organization/billing/payment-methods",
        timeout=30000,
        check_abort=check_abort,
        wait_until="commit",
    )
    _pause(2.0, 4.0)

    _log("点击 Add payment method...")
    clicked = _click_add_payment(page)
    if not clicked:
        raise CardBindError("找不到 Add payment method 按钮")
    _pause(2.5, 4.5)

    browser_utils.wait_for_selector_interruptible(
        page,
        "iframe[src*='stripe.com'], [role='dialog']",
        timeout=15000,
        check_abort=check_abort,
    )
    _pause(0.8, 1.5)

    _log(f"填写卡号 ****{card['number'][-4:]}...")
    browser_utils.fill_stripe_iframe(
        page,
        card["number"],
        card["exp_month"],
        card["exp_year"],
        card["cvv"],
        check_abort=check_abort,
    )

    _pause(0.5, 1.2)
    _log(f"填写持卡人: {holder_name}")
    browser_utils.fill_billing_name(page, holder_name)
    _pause(0.3, 0.8)

    addr_summary = f"{address.get('city','')}, {address.get('state','')} {address.get('zip','')}"
    _log(f"填写账单地址: {addr_summary}")
    browser_utils.fill_billing_address(page, address)
    _pause(0.8, 1.5)

    _log("提交绑卡...")
    page.evaluate("""() => {
        const btn = [...document.querySelectorAll('button')]
            .find(b => b.textContent.trim() === 'Add payment method');
        if (btn) btn.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }""")
    _pause(0.4, 0.8)
    browser_utils.click_button_by_text(page, "Add payment method")
    _pause(2.0, 4.0)

    _log("等待绑卡结果...")
    _raise_typed_error(
        browser_utils.wait_for_billing_result(page, timeout=30, check_abort=check_abort),
        "绑卡失败",
        _log,
    )

    _log("绑卡成功! 开始充值...")
    _pause(1.0, 2.0)

    add_credits(page, credit_amount, on_log=on_log, check_abort=check_abort)


def add_credits(
    page: Page,
    credit_amount: int = 5,
    on_log: Optional[Callable[[str], None]] = None,
    check_abort: Optional[Callable[[], bool]] = None,
) -> None:
    """Add credits to an already-bound account via billing overview."""
    _log, _pause = _make_flow_helpers(on_log, check_abort)

    credit_amount = max(5, min(100, credit_amount))

    _log("打开 Billing Overview...")
    browser_utils.goto_interruptible(
        page,
        "https://platform.openai.com/settings/organization/billing/overview",
        timeout=30000,
        check_abort=check_abort,
        wait_until="commit",
    )
    _pause(2.0, 3.5)

    _log(f"点击 Add to credit balance (${credit_amount})...")
    browser_utils.click_button_by_text(page, "Add to credit balance")
    _pause(2.0, 3.5)

    browser_utils.wait_for_selector_interruptible(
        page, "[role='dialog']", timeout=15000, check_abort=check_abort
    )
    _pause(0.5, 1.0)

    browser_utils.fill_credit_amount(page, credit_amount)
    _pause(0.5, 1.0)

    _log("确认充值...")
    browser_utils.click_button_by_text(page, "Continue")
    _pause(2.0, 4.0)

    _confirm_payment_if_present(page, credit_amount, _log, _pause)

    _log("等待付款完成...")
    _raise_typed_error(
        browser_utils.wait_for_payment_done(page, timeout=45, check_abort=check_abort),
        "付款失败",
        _log,
    )

    _log(f"充值 ${credit_amount} 完成!")


_CREDIT_OPTIONS = {5: "$5 credits", 10: "$10 credits", 20: "$20 credits"}

# ─── Tier-based auto-recharge strategy ─────────────────────

RECHARGE_TIERS = [0, 20, 10, 5]   # 0 = fill remaining; descending order
MAX_CREDIT_PER_CARD = 100


def _tier_label(val: int) -> str:
    return "充满" if val == 0 else f"${val}"


def recharge_with_tiers(
    page: Page,
    current_total: float,
    upper: int = 20,
    lower: int = 5,
    on_log: Optional[Callable[[str], None]] = None,
    on_charged: Optional[Callable[[int], None]] = None,
    check_abort: Optional[Callable[[], bool]] = None,
) -> tuple[float, str]:
    """Tier-based auto-recharge: start at *upper* tier, on 3DS drop to next,
    on insufficient stop, cap at MAX_CREDIT_PER_CARD.

    Args:
        current_total: credits already loaded (first charge included).
        upper / lower: tier values from RECHARGE_TIERS.
        on_log: progress callback.
        on_charged(amount): called after each successful charge (for DB update).

    Returns:
        (new_total, stop_reason)
        stop_reason: "full" | "3ds" | "insufficient" | "declined" | "error"
    """
    def _log(msg: str) -> None:
        log.info(msg)
        if on_log:
            on_log(msg)

    try:
        upper_idx = RECHARGE_TIERS.index(upper)
    except ValueError:
        upper_idx = 1
    try:
        lower_idx = RECHARGE_TIERS.index(lower)
    except ValueError:
        lower_idx = len(RECHARGE_TIERS) - 1

    if upper_idx > lower_idx:
        upper_idx, lower_idx = lower_idx, upper_idx

    tiers = RECHARGE_TIERS[upper_idx:lower_idx + 1]
    tier_idx = 0
    loaded = current_total

    while loaded < MAX_CREDIT_PER_CARD:
        remaining = MAX_CREDIT_PER_CARD - loaded
        if remaining < 5:
            _log(f"剩余 ${remaining:.0f} 不足最低 $5, 追充完成")
            return loaded, "full"

        tier_val = tiers[tier_idx]
        if tier_val == 0:
            amount = int(remaining)
        else:
            amount = min(tier_val, int(remaining))
        amount = max(5, min(100, amount))

        try:
            _log(f"充值 ${amount} (档位 {_tier_label(tier_val)})...")
            add_credits(page, amount, on_log=on_log, check_abort=check_abort)
            loaded += amount
            if on_charged:
                on_charged(amount)
            _log(f"成功 +${amount}, 累计 ${loaded:.0f}")
        except Card3DSFailed:
            _log(f"${amount} 触发 3DS")
            if tier_idx + 1 < len(tiers):
                tier_idx += 1
                _log(f"降档至 {_tier_label(tiers[tier_idx])} 继续...")
                continue
            else:
                _log(f"已达最低档 {_tier_label(tier_val)}, 停止 (3DS)")
                return loaded, "3ds"
        except CardDeclined as e:
            reason = "insufficient" if "insufficient" in str(e).lower() else "declined"
            _log(f"被拒 [{reason}]: {e}")
            return loaded, reason
        except Exception as e:
            _log(f"充值异常: {e}")
            return loaded, "error"

    _log(f"已达上限 ${MAX_CREDIT_PER_CARD}, 追充完成")
    return loaded, "full"


def bind_card_onboarding(
    page: Page,
    card: dict,
    address: dict,
    holder_name: str,
    credit_amount: int = 5,
    on_log: Optional[Callable[[str], None]] = None,
    check_abort: Optional[Callable[[], bool]] = None,
) -> None:
    """Bind card via the onboarding credits flow."""
    _log, _pause = _make_flow_helpers(on_log, check_abort)

    if credit_amount not in _CREDIT_OPTIONS:
        log.warning(f"Onboarding 路径仅支持 $5/$10/$20, 请求 ${credit_amount} 已回退至 $5 "
                    f"(如需更高额度请使用 billing 路径)")
        credit_amount = 5
    label = _CREDIT_OPTIONS[credit_amount]

    _log("点击 Continue 进入充值页...")
    browser_utils.click_button_by_text(page, "Continue")
    _pause(2.0, 3.5)

    browser_utils.wait_for_selector_interruptible(
        page, "text=Purchase credits", timeout=15000, check_abort=check_abort
    )
    _pause(0.5, 1.5)
    if credit_amount != 5:
        _log(f"选择 {label}...")
        page.evaluate(
            """(label) => {
                for (const btn of document.querySelectorAll('button')) {
                    if (btn.textContent.includes(label)) { btn.click(); return true; }
                }
                return false;
            }""",
            label,
        )
        _pause(0.3, 0.8)
    _log(f"选择 {label}, 点击 Purchase credits...")
    browser_utils.click_button_by_text(page, "Purchase credits")
    _pause(2.5, 4.5)

    browser_utils.wait_for_selector_interruptible(
        page, "[role='dialog']", timeout=15000, check_abort=check_abort
    )
    browser_utils.wait_for_selector_interruptible(
        page,
        "iframe[title='Secure payment input frame']",
        timeout=15000,
        check_abort=check_abort,
    )
    _pause(1.0, 2.0)

    _log(f"填写卡号 ****{card['number'][-4:]}...")
    browser_utils.fill_onboarding_card(
        page,
        card["number"],
        card["exp_month"],
        card["exp_year"],
        card["cvv"],
        check_abort=check_abort,
    )

    _pause(0.5, 1.2)
    addr_summary = f"{address.get('city','')}, {address.get('state','')} {address.get('zip','')}"
    _log(f"填写持卡人 {holder_name}, 账单地址: {addr_summary}")
    browser_utils.fill_onboarding_address(page, holder_name, address, check_abort=check_abort)
    _pause(0.8, 1.5)

    _log("提交绑卡...")
    browser_utils.click_button_by_text(page, "Add payment method")
    _pause(2.0, 4.0)

    _log("等待确认付款页...")
    _raise_typed_error(
        browser_utils.wait_for_confirm_or_error(page, timeout=45, check_abort=check_abort),
        "绑卡失败",
        _log,
    )

    _confirm_payment_if_present(page, credit_amount, _log, _pause)

    _log("等待付款完成...")
    _raise_typed_error(
        browser_utils.wait_for_payment_done(page, timeout=45, check_abort=check_abort),
        "付款失败",
        _log,
    )

    _log(f"绑卡+充值 ${credit_amount} 完成!")


def _click_add_payment(page: Page) -> bool:
    """Click the Add payment button using known labels."""
    exact_texts = ["Add payment details", "Add payment method", "Set up paid account"]
    contains_texts = ["Add payment", "payment details", "paid account"]

    for text in exact_texts:
        if browser_utils.click_button_by_text(page, text):
            return True

    for partial in contains_texts:
        if browser_utils.click_button_containing(page, partial):
            return True

    return page.evaluate("""() => {
        for (const el of document.querySelectorAll('a, button')) {
            const t = el.textContent.toLowerCase();
            if (t.includes('payment') || t.includes('add') && t.includes('billing')) {
                el.click(); return true;
            }
        }
        return false;
    }""")
