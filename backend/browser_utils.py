"""Playwright 页面操作工具 — 针对 OpenAI 注册页 & 绑卡页的 React 组件"""

import fnmatch
import logging
import random
import time
from typing import Any, Callable, Optional

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

log = logging.getLogger("reg.browser")


def _check_control(check_abort: Optional[Callable[[], bool]] = None) -> None:
    if check_abort and check_abort():
        from .register import TaskAborted
        raise TaskAborted("任务已中断")


def _interruptible_wait_loop(
    timeout_ms: int,
    check_abort: Optional[Callable[[], bool]],
    predicate: Callable[[], bool],
    interval: float = 0.25,
) -> bool:
    deadline = time.time() + timeout_ms / 1000.0
    while time.time() < deadline:
        _check_control(check_abort)
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    _check_control(check_abort)
    return False


def goto_interruptible(
    page: Page,
    url: str,
    timeout: int = 30000,
    check_abort: Optional[Callable[[], bool]] = None,
    wait_until: str = "commit",
) -> None:
    """Navigate with short slices so cancel/pause can be observed promptly."""
    deadline = time.time() + timeout / 1000.0
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        _check_control(check_abort)
        slice_timeout = int(min(3000, max(800, (deadline - time.time()) * 1000)))
        try:
            page.goto(url, wait_until=wait_until, timeout=slice_timeout)
            return
        except PlaywrightTimeoutError as e:
            last_error = e
        except Exception:
            raise
    if last_error:
        raise last_error
    raise PlaywrightTimeoutError(f"Navigation timeout: {url}")


def wait_for_selector_interruptible(
    page: Page,
    selector: str,
    timeout: int = 15000,
    check_abort: Optional[Callable[[], bool]] = None,
    visible: bool = False,
) -> None:
    def _predicate() -> bool:
        loc = page.locator(selector)
        if loc.count() <= 0:
            return False
        return loc.first.is_visible() if visible else True

    if not _interruptible_wait_loop(timeout, check_abort, _predicate):
        raise PlaywrightTimeoutError(f"Timeout waiting for selector: {selector}")


def wait_for_url_interruptible(
    page: Page,
    pattern: str,
    timeout: int = 15000,
    check_abort: Optional[Callable[[], bool]] = None,
) -> None:
    if not _interruptible_wait_loop(timeout, check_abort, lambda: fnmatch.fnmatch(page.url, pattern)):
        raise PlaywrightTimeoutError(f"Timeout waiting for URL: {pattern} (current: {page.url})")


def _has_onboarding_create_page(page: Page) -> bool:
    if fnmatch.fnmatch(page.url, "**/welcome?step=create*"):
        return True
    try:
        return bool(page.evaluate("""() => {
            const orgInput = document.querySelector('#organization-name');
            if (orgInput && orgInput.offsetParent) return true;
            for (const el of document.querySelectorAll('button, h1, h2, h3, label, span, div')) {
                if (!el.offsetParent) continue;
                const t = (el.textContent || '').trim().toLowerCase();
                if (!t) continue;
                if (t.includes('create organization') || t.includes('organization name')) return true;
            }
            return false;
        }"""))
    except Exception:
        return False


def wait_for_onboarding_create_interruptible(
    page: Page,
    timeout: int = 30000,
    check_abort: Optional[Callable[[], bool]] = None,
) -> None:
    if _interruptible_wait_loop(timeout, check_abort, lambda: _has_onboarding_create_page(page)):
        return
    err = _page_error_text(page)
    submit = _submit_button_state(page)
    extra = []
    if submit:
        extra.append(f"submit={submit}")
    if err:
        extra.append(f"page_error={err}")
    suffix = f" ({', '.join(extra)})" if extra else ""
    raise PlaywrightTimeoutError(
        f"Timeout waiting for onboarding create page (current: {page.url}){suffix}"
    )


def _has_onboarding_invite_page(page: Page) -> bool:
    if fnmatch.fnmatch(page.url, "**/welcome?step=invite*"):
        return True
    try:
        return bool(page.evaluate("""() => {
            const texts = [
                'invite my team later',
                'invite your team',
                'invite teammates',
                'invite team',
            ];
            for (const el of document.querySelectorAll('button, h1, h2, h3, p, span, div')) {
                if (!el.offsetParent) continue;
                const t = (el.textContent || '').trim().toLowerCase();
                if (!t) continue;
                if (texts.some(part => t.includes(part))) return true;
            }
            return false;
        }"""))
    except Exception:
        return False


def _has_onboarding_try_page(page: Page) -> bool:
    if fnmatch.fnmatch(page.url, "**/welcome?step=try*"):
        return True
    try:
        return bool(page.evaluate("""() => {
            for (const el of document.querySelectorAll('button, a, [role="button"]')) {
                if (!el.offsetParent) continue;
                const t = (el.textContent || '').trim().toLowerCase();
                if (!t) continue;
                if (t.includes('generate api key') || t.includes('create api key') || t.includes('api keys')) {
                    return true;
                }
            }
            return false;
        }"""))
    except Exception:
        return False


def wait_for_onboarding_invite_or_try_interruptible(
    page: Page,
    timeout: int = 15000,
    check_abort: Optional[Callable[[], bool]] = None,
) -> str:
    state = {"step": ""}

    def _predicate() -> bool:
        if _has_onboarding_try_page(page):
            state["step"] = "try"
            return True
        if _has_onboarding_invite_page(page):
            state["step"] = "invite"
            return True
        return False

    if _interruptible_wait_loop(timeout, check_abort, _predicate):
        return state["step"]

    err = _page_error_text(page)
    submit = _submit_button_state(page)
    extra = []
    if submit:
        extra.append(f"submit={submit}")
    if err:
        extra.append(f"page_error={err}")
    suffix = f" ({', '.join(extra)})" if extra else ""
    raise PlaywrightTimeoutError(
        f"Timeout waiting for onboarding invite/try page (current: {page.url}){suffix}"
    )


_PAGE_ERROR_JS = """() => {
    for (const sel of ['[role="alert"]', '[role="status"]',
                        '.error-message', '.text-danger', '.err-message']) {
        for (const el of document.querySelectorAll(sel)) {
            const t = el.textContent.trim();
            if (t && t.length > 5 && el.offsetParent) return t;
        }
    }
    const keywords = ['insufficient', 'declined', 'expired', 'invalid',
                      'failed', 'not supported', 'Try a different',
                      'unable to process', 'authentication required',
                      'verification failed', '3D Secure', 'security code'];
    for (const el of document.querySelectorAll('div, span, p')) {
        if (!el.offsetParent || el.children.length > 3) continue;
        const t = el.textContent.trim().toLowerCase();
        if (t.length > 10 && t.length < 200) {
            for (const kw of keywords) {
                if (t.includes(kw.toLowerCase())) return el.textContent.trim();
            }
        }
    }
    return null;
}"""


def _click_buttons(
    page: Page,
    *,
    exact_texts: Optional[list[str]] = None,
    contains_texts: Optional[list[str]] = None,
    selectors: str = "button",
) -> bool:
    """Click the first visible element whose text matches any supplied rule."""
    return page.evaluate(
        """({ exactTexts, containsTexts, selectors }) => {
            const exact = exactTexts || [];
            const contains = containsTexts || [];
            for (const el of document.querySelectorAll(selectors)) {
                if (!el.offsetParent) continue;
                const text = el.textContent.trim();
                if (exact.includes(text) || contains.some(part => text.includes(part))) {
                    el.click();
                    return true;
                }
            }
            return false;
        }""",
        {
            "exactTexts": exact_texts or [],
            "containsTexts": contains_texts or [],
            "selectors": selectors,
        },
    )


def _page_error_text(page: Page) -> Optional[str]:
    return page.evaluate(_PAGE_ERROR_JS)


def _dialog_gone(page: Page) -> bool:
    return page.evaluate("""() => {
        const m = document.querySelector('[role="dialog"]');
        return !m || !m.offsetParent;
    }""")


def _has_confirm_payment(page: Page) -> bool:
    return page.evaluate("""() => {
        for (const el of document.querySelectorAll('h1, h2, h3, button')) {
            if (el.textContent.trim().includes('Confirm payment') && el.offsetParent) {
                return true;
            }
        }
        return false;
    }""")


def _has_3ds_challenge(page: Page) -> bool:
    return page.evaluate("""() => {
        for (const f of document.querySelectorAll('iframe')) {
            const s = (f.src || '') + (f.name || '');
            if (s.includes('3ds') || s.includes('three-ds') || s.includes('challenge')) {
                return true;
            }
        }
        return false;
    }""")


def _payment_completed(page: Page) -> bool:
    return page.evaluate("""() => {
        const body = document.body.innerText;

        // 明确的成功标志
        if (body.includes('Payment successful')) return true;

        // Billing overview 页面的标志性元素 — 出现即说明充值完成并已跳转回主页
        if (body.includes('Credit balance') && body.includes('Usage this month')) {
            return true;
        }

        // URL 已跳转到 billing overview（最可靠的信号）
        if (window.location.pathname.includes('/billing/overview') ||
            window.location.pathname.includes('/billing')) {
            // 确认页面不在充值/确认流程中
            const hasConfirm = [...document.querySelectorAll('h1, h2, h3')]
                .some(el => el.textContent.includes('Confirm payment') && el.offsetParent);
            const hasCreditsPage = [...document.querySelectorAll('h1')]
                .some(el => el.textContent.includes('Add some API credits'));
            const hasAddPayment = [...document.querySelectorAll('h1, h2, h3')]
                .some(el => el.textContent.includes('Add payment details') && el.offsetParent);
            if (!hasConfirm && !hasCreditsPage && !hasAddPayment) return true;
        }

        // dialog 已关闭 且 URL 不在支付流程中
        const dialog = document.querySelector('[role="dialog"]');
        if (!dialog || !dialog.offsetParent) {
            const hasConfirm = [...document.querySelectorAll('h1, h2, h3')]
                .some(el => el.textContent.includes('Confirm payment') && el.offsetParent);
            const hasCreditsPage = [...document.querySelectorAll('h1')]
                .some(el => el.textContent.includes('Add some API credits'));
            const hasAddPayment = [...document.querySelectorAll('h1, h2, h3')]
                .some(el => el.textContent.includes('Add payment details') && el.offsetParent);
            if (!hasConfirm && !hasCreditsPage && !hasAddPayment) return true;
        }
        return false;
    }""")


def react_set_value(page: Page, selector: str, value: str) -> None:
    """通过 nativeInputValueSetter 设置 React 受控 input 的值"""
    page.evaluate(
        """([sel, val]) => {
            const el = document.querySelector(sel);
            const setter = Object.getOwnPropertyDescriptor(
                window.HTMLInputElement.prototype, 'value'
            ).set;
            setter.call(el, val);
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
        }""",
        [selector, value],
    )


def click_button_by_text(page: Page, text: str) -> bool:
    return _click_buttons(page, exact_texts=[text])


def click_button_containing(page: Page, text: str) -> bool:
    return _click_buttons(page, contains_texts=[text])


def click_invite_later(page: Page) -> bool:
    clicked = _click_buttons(
        page,
        exact_texts=[
            "Invite my team later",
            "invite my team later",
            "Later",
            "later",
            "Skip",
            "Skip for now",
        ],
        contains_texts=[
            "invite my team later",
            "later",
            "skip",
        ],
        selectors='button, [role="button"]',
    )
    if clicked:
        return True

    try:
        page.locator("button:has-text('invite my team later')").first.click(timeout=3000)
        return True
    except Exception:
        pass
    try:
        page.locator("button:has-text('later')").first.click(timeout=3000)
        return True
    except Exception:
        pass

    raise RuntimeError("未找到 invite later / skip 按钮")


def _has_api_key_cta(page: Page) -> bool:
    try:
        return bool(page.evaluate("""() => {
            for (const el of document.querySelectorAll('button, a, [role="button"]')) {
                if (!el.offsetParent) continue;
                const t = (el.textContent || '').trim().toLowerCase();
                if (!t) continue;
                if (
                    t.includes('generate api key')
                    || t.includes('create api key')
                    || t.includes('api key')
                    || t.includes('api keys')
                ) {
                    return true;
                }
            }
            return false;
        }"""))
    except Exception:
        return False


def wait_for_api_key_cta_interruptible(
    page: Page,
    timeout: int = 15000,
    check_abort: Optional[Callable[[], bool]] = None,
) -> None:
    if _interruptible_wait_loop(timeout, check_abort, lambda: _has_api_key_cta(page)):
        return
    raise PlaywrightTimeoutError(f"Timeout waiting for API key CTA (current: {page.url})")


def click_api_key_cta(page: Page) -> bool:
    clicked = _click_buttons(
        page,
        exact_texts=[
            "Generate API Key",
            "Generate API key",
            "Create API Key",
            "Create API key",
            "API Keys",
            "API keys",
        ],
        contains_texts=[
            "Generate API Key",
            "Generate API key",
            "Create API Key",
            "Create API key",
            "API Key",
            "API key",
            "API Keys",
            "API keys",
        ],
        selectors='button, a, [role="button"]',
    )
    if clicked:
        return True

    for sel in [
        "button:has-text('Generate API Key')",
        "button:has-text('Generate API key')",
        "button:has-text('Create API Key')",
        "button:has-text('Create API key')",
        "a:has-text('API Keys')",
        "a:has-text('API keys')",
        "[role='button']:has-text('API Key')",
        "[role='button']:has-text('API key')",
    ]:
        try:
            page.locator(sel).first.click(timeout=3000)
            return True
        except Exception:
            pass

    raise RuntimeError(f"未找到 API Key 入口，当前 URL: {page.url}")


def _submit_button_state(page: Page) -> dict[str, Any]:
    return page.evaluate(
        """() => {
            const btn = document.querySelector('button[type="submit"], button[data-testid="submit-button"]');
            if (!btn || !btn.offsetParent) {
                return { found: false, disabled: null, text: '' };
            }
            return {
                found: true,
                disabled: !!btn.disabled || btn.getAttribute('aria-disabled') === 'true',
                text: (btn.innerText || btn.textContent || '').trim(),
            };
        }"""
    )


def click_submit(page: Page, retries: int = 3, delay: float = 0.5) -> dict[str, Any]:
    """更稳地点击 submit 按钮；若按钮不可用会带状态信息抛错。"""
    last_state: dict[str, Any] = {"found": False, "disabled": None, "text": ""}
    for _ in range(max(1, retries)):
        last_state = _submit_button_state(page)
        if not last_state.get("found"):
            time.sleep(delay)
            continue
        if last_state.get("disabled"):
            time.sleep(delay)
            continue

        clicked = False
        try:
            btn = page.locator("button[type='submit']").first
            if btn.count() > 0:
                btn.click(timeout=3000)
                clicked = True
        except Exception:
            pass

        if not clicked:
            clicked = bool(page.evaluate(
                """() => {
                    const btn = document.querySelector('button[type="submit"], button[data-testid="submit-button"]');
                    if (!btn || !btn.offsetParent) return false;
                    btn.focus();
                    try { btn.click(); } catch (_) {}
                    const form = btn.form || btn.closest('form');
                    if (form && typeof form.requestSubmit === 'function') {
                        try { form.requestSubmit(btn); } catch (_) {}
                    }
                    ['pointerdown','mousedown','pointerup','mouseup','click'].forEach(t =>
                        btn.dispatchEvent(new MouseEvent(t, { bubbles: true, cancelable: true, view: window }))
                    );
                    return true;
                }"""
            ))
        if clicked:
            return last_state
        time.sleep(delay)

    raise RuntimeError(
        f"submit 按钮未就绪或点击失败: found={last_state.get('found')} "
        f"disabled={last_state.get('disabled')} text={last_state.get('text', '')!r}"
    )


def _type_digits(page: Page, digits: list[str]) -> None:
    for d in digits:
        page.keyboard.press(d)
        time.sleep(0.08)


def click_radix_select(page: Page, option_text: str) -> bool:
    """点击 Radix UI Select 触发器并选择选项（不依赖 hash class 名）"""
    opened = page.evaluate("""() => {
        const trigger = document.querySelector('[role="combobox"]')
            || document.querySelector('[aria-haspopup="listbox"]');
        if (trigger) { trigger.click(); return true; }
        return false;
    }""")
    if not opened:
        _click_buttons(page, exact_texts=["Select...", "Select"], selectors="span, button, div")
    time.sleep(1)
    return _click_buttons(
        page,
        exact_texts=[option_text],
        selectors='[role="option"], div[data-radix-collection-item], div, span',
    )


def fill_birthday(page: Page, year: str, month: str = "02", day: str = "15") -> None:
    """自动检测生日 UI 类型并填写。"""
    has_dropdowns = len(page.query_selector_all("button[aria-haspopup='listbox']")) >= 3
    has_datefield = page.query_selector("[data-type='month']") is not None

    if has_dropdowns:
        log.info("生日 UI: 下拉选择器")
        btns = page.query_selector_all("button[aria-haspopup='listbox']")
        btns[2].click()
        time.sleep(0.5)
        page.evaluate(
            """(yr) => {
                for (const o of document.querySelectorAll('[role="listbox"] [role="option"]')) {
                    if (o.textContent.trim() === yr) { o.click(); return true; }
                }
                return false;
            }""",
            year,
        )
        time.sleep(0.3)
    elif has_datefield:
        log.info("生日 UI: DateField")
        page.locator("[data-type='month']").click(force=True)
        time.sleep(0.3)
        _type_digits(page, list(month))
        time.sleep(0.3)

        day_seg = page.locator("[data-type='day']")
        if day_seg.count():
            day_seg.click(force=True)
            time.sleep(0.2)
        _type_digits(page, list(day))
        time.sleep(0.3)

        year_seg = page.locator("[data-type='year']")
        if year_seg.count():
            year_seg.click(force=True)
            time.sleep(0.2)
        _type_digits(page, list(year))
        time.sleep(0.3)
    else:
        bday_input = page.query_selector("input[name='birthday']")
        if bday_input:
            log.info("生日 UI: 直接设置 hidden input")
            react_set_value(page, "input[name='birthday']", f"{year}-{month}-{day}")
        else:
            raise RuntimeError("未识别的生日 UI 类型")


# ═══════════════════════════════════════════════════════════
# Human-like primitives (绑卡拟人操作基础层)
# ═══════════════════════════════════════════════════════════


def _rs(lo: float = 0.05, hi: float = 0.15) -> None:
    """Random sleep."""
    time.sleep(random.uniform(lo, hi))


def _human_move(page: Page, x: float, y: float) -> None:
    """Smooth mouse movement with jitter."""
    page.mouse.move(
        x + random.uniform(-3, 3),
        y + random.uniform(-2, 2),
        steps=random.randint(8, 20),
    )
    _rs(0.02, 0.07)


def _el_point(locator) -> Optional[tuple[float, float]]:
    """Random interior point of an element (viewport coords)."""
    try:
        box = locator.bounding_box(timeout=5000)
    except Exception:
        return None
    if not box or box["width"] < 1:
        return None
    return (
        box["x"] + box["width"] * random.uniform(0.2, 0.8),
        box["y"] + box["height"] * random.uniform(0.25, 0.75),
    )


def _human_click(page: Page, locator: Any) -> None:
    """Move mouse to element → click.  Falls back to .click() if invisible."""
    pt = _el_point(locator)
    if pt:
        _human_move(page, *pt)
        page.mouse.click(
            pt[0] + random.uniform(-1, 1),
            pt[1] + random.uniform(-1, 1),
        )
        _rs(0.08, 0.2)
    else:
        locator.click()
        _rs(0.1, 0.2)


def _human_type(
    page: Page,
    text: str,
    lo: float = 0.05,
    hi: float = 0.14,
    pause_chance: float = 0.05,
) -> None:
    """Per-key random delay + occasional hesitation."""
    for ch in text:
        page.keyboard.press(ch)
        d = random.uniform(lo, hi)
        if random.random() < pause_chance:
            d += random.uniform(0.15, 0.45)
        time.sleep(d)


def _human_type_card(page: Page, number: str) -> None:
    """Card number with natural 4-digit-group pauses (reading from card)."""
    for i, d in enumerate(number):
        page.keyboard.press(d)
        if (i + 1) % 4 == 0 and i < len(number) - 1:
            _rs(0.20, 0.50)
        else:
            _rs(0.05, 0.14)
        if random.random() < 0.04:
            _rs(0.15, 0.40)


def _field_transition(page: Page, locator: Any) -> None:
    """Move to next field — Tab (40%) or mouse click (60%)."""
    if random.random() < 0.4:
        page.keyboard.press("Tab")
        _rs(0.15, 0.35)
    else:
        _human_click(page, locator)


def _human_fill_field(page: Page, selector: str, value: str) -> None:
    """Human-like React input with verification and fallback."""
    try:
        el = page.locator(selector)
        _human_click(page, el)
        _rs(0.15, 0.35)
        el.click(click_count=3)
        _rs(0.03, 0.08)
        page.keyboard.press("Backspace")
        _rs(0.08, 0.15)
        _human_type(page, value, 0.04, 0.11)
        _rs(0.1, 0.25)
        try:
            actual = el.input_value(timeout=500)
            if actual.strip() != value.strip():
                log.debug("键入值未生效, 回退 react_set_value")
                react_set_value(page, selector, value)
        except Exception:
            pass
    except Exception:
        react_set_value(page, selector, value)


def _first_frame_locator(
    page: Page,
    selectors: list[str],
    label: str,
    timeout: int = 10000,
    check_abort: Optional[Callable[[], bool]] = None,
) -> Any:
    """Return the first frame locator whose selector becomes available."""
    for sel in selectors:
        try:
            wait_for_selector_interruptible(page, sel, timeout=timeout, check_abort=check_abort)
            return page.frame_locator(sel).first
        except Exception:
            continue
    raise RuntimeError(f"找不到 {label} iframe")


def _clear_and_type_locator(
    locator: Any,
    value: str,
    delay_lo: int = 40,
    delay_hi: int = 110,
) -> None:
    """Clear an input-like locator, then type sequentially."""
    locator.click(click_count=3)
    _rs(0.05, 0.1)
    locator.press("Backspace")
    _rs(0.1, 0.2)
    if value:
        locator.press_sequentially(value, delay=random.randint(delay_lo, delay_hi))


def _dismiss_address_autocomplete(addr_el: Any, name_el: Any) -> None:
    """Collapse Stripe address autocomplete overlays after addressLine1 input."""
    _rs(0.8, 1.5)
    addr_el.press("Escape")
    _rs(0.3, 0.5)
    addr_el.press("Escape")
    _rs(0.5, 1.0)
    name_el.click()
    _rs(0.3, 0.5)
    addr_el.click()
    _rs(0.2, 0.4)
    addr_el.press("Escape")
    _rs(0.3, 0.5)
    addr_el.press("Tab")
    _rs(1.0, 2.0)


# ═══════════════════════════════════════════════════════════
# Stripe iframe (拟人填卡)
# ═══════════════════════════════════════════════════════════

_CARD_SELS = [
    "input[name='cardnumber']",
    "input[data-elements-stable-field-name='cardNumber']",
    "input[autocomplete='cc-number']",
]
_EXP_SELS = [
    "input[name='exp-date']",
    "input[data-elements-stable-field-name='cardExpiry']",
    "input[autocomplete='cc-exp']",
]
_CVC_SELS = [
    "input[name='cvc']",
    "input[data-elements-stable-field-name='cardCvc']",
    "input[autocomplete='cc-csc']",
]


def _stripe_find(stripe: Any, selectors: list[str]) -> Any:
    """Return the first visible input inside the Stripe frame."""
    for sel in selectors:
        loc = stripe.locator(sel)
        try:
            if loc.bounding_box(timeout=3000):
                return loc
        except Exception:
            continue
    raise RuntimeError(f"Stripe iframe 内找不到: {selectors[0]}")


def fill_stripe_iframe(
    page: Page,
    card_number: str,
    exp_month: str,
    exp_year: str,
    cvc: str,
    check_abort: Optional[Callable[[], bool]] = None,
) -> None:
    """Human-like card entry inside Stripe iframe.

    Key anti-detection behaviours:
    - Variable per-key delay (not uniform 50ms)
    - 4-digit group pauses (natural card-reading rhythm)
    - Random Tab / mouse-click transitions between fields
    - 'Flip card' hesitation before CVC
    - Mouse movement before every click
    """
    stripe = _first_frame_locator(page, [
        "iframe[src*='elements-inner-card']",
        "iframe[src*='js.stripe.com']",
    ], "Stripe 卡号", check_abort=check_abort)

    _rs(0.6, 1.2)

    # ── card number ──────────────────────────────────────
    _human_click(page, _stripe_find(stripe, _CARD_SELS))
    _rs(0.25, 0.5)
    _human_type_card(page, card_number)
    _rs(0.4, 0.9)

    # ── expiry (MMYY) ───────────────────────────────────
    _field_transition(page, _stripe_find(stripe, _EXP_SELS))
    exp_yy = exp_year[-2:] if len(exp_year) >= 4 else exp_year
    _human_type(page, f"{exp_month}{exp_yy}", 0.07, 0.17)
    _rs(0.35, 0.75)

    # ── CVC ──────────────────────────────────────────────
    _field_transition(page, _stripe_find(stripe, _CVC_SELS))
    _rs(0.4, 1.0)                        # "flipping card over"
    _human_type(page, cvc, 0.08, 0.19)
    _rs(0.3, 0.6)

    log.info("Stripe iframe 卡号/日期/CVC 填写完成")


# ═══════════════════════════════════════════════════════════
# Billing form (拟人填地址/持卡人)
# ═══════════════════════════════════════════════════════════


def fill_billing_name(page: Page, name: str) -> bool:
    """Human-like 'Name on card' entry.

    Locates the field (no name attr) → marks it with a data attribute →
    fills it via _human_fill_field for realistic keystrokes.
    """
    found = page.evaluate("""() => {
        let el = null;
        el = document.querySelector('input[autocomplete="cc-name"]');
        if (!el) {
            for (const inp of document.querySelectorAll('input')) {
                const ph = (inp.placeholder || '').toLowerCase();
                if (ph.includes('name') && inp.offsetParent && !inp.closest('iframe')) {
                    el = inp; break;
                }
            }
        }
        if (!el) {
            for (const label of document.querySelectorAll('label')) {
                const lt = label.textContent.toLowerCase();
                if (lt.includes('name on card') || lt.includes('cardholder')) {
                    const inp = label.querySelector('input')
                        || label.parentElement?.querySelector('input')
                        || (label.htmlFor && document.getElementById(label.htmlFor));
                    if (inp && inp.offsetParent) { el = inp; break; }
                }
            }
        }
        if (!el) {
            const modal = document.querySelector('[role="dialog"]') || document;
            for (const inp of modal.querySelectorAll('input')) {
                if (!inp.name && inp.type !== 'hidden' && inp.type !== 'checkbox'
                    && inp.offsetParent && !inp.closest('iframe')) {
                    el = inp; break;
                }
            }
        }
        if (!el) return false;
        el.setAttribute('data-ht', '1');
        return true;
    }""")
    if not found:
        log.warning("未找到 Name on card 输入框")
        return False

    _human_fill_field(page, "[data-ht='1']", name)
    page.evaluate("document.querySelector('[data-ht]')?.removeAttribute('data-ht')")
    return True


def fill_billing_address(page: Page, address: dict[str, str]) -> None:
    """Human-like billing address — per-field click+type with natural pauses."""
    for field_name, value in [
        ("billing-address-line-1", address.get("address_line1", "")),
        ("billing-city", address.get("city", "")),
        ("billing-postal-code", address.get("zip", "")),
    ]:
        if value:
            _human_fill_field(page, f"input[name='{field_name}']", value)
            _rs(0.25, 0.55)

    state = address.get("state", "")
    if state:
        _select_billing_state(page, state)
    _rs(0.2, 0.4)


def _select_billing_state(page: Page, state_code: str) -> None:
    """Handle state selection — native <select> or custom dropdown."""
    from .address_data import US_STATES
    state_name = US_STATES.get(state_code, state_code)

    done = page.evaluate("""([code, name]) => {
        const sel = document.querySelector('select[name="billing-state"]');
        if (sel && sel.offsetParent) {
            for (const opt of sel.options) {
                if (opt.value === code || opt.textContent.trim() === name
                    || opt.textContent.trim().startsWith(name)) {
                    sel.value = opt.value;
                    sel.dispatchEvent(new Event('change', { bubbles: true }));
                    return true;
                }
            }
        }
        return false;
    }""", [state_code, state_name])
    if done:
        return

    page.evaluate("""() => {
        const el = document.querySelector('[name="billing-state"]');
        if (el) { el.click(); el.focus(); return true; }
        const triggers = document.querySelectorAll(
            '[role="combobox"], [aria-haspopup="listbox"]');
        for (const t of triggers) {
            const wrap = t.closest('.field, .form-group, div');
            if (wrap && wrap.textContent.toLowerCase().includes('state')) {
                t.click(); return true;
            }
        }
        return false;
    }""")
    _rs(0.5, 1.0)

    page.evaluate("""([code, name]) => {
        for (const el of document.querySelectorAll(
            '[role="option"], [data-radix-collection-item]')) {
            const t = el.textContent.trim();
            if (t === code || t === name || t.includes(name)) {
                el.click(); return true;
            }
        }
        return false;
    }""", [state_code, state_name])


# ═══════════════════════════════════════════════════════════
# Onboarding Stripe Elements (注册后直充流程 — 卡+地址均在 iframe)
# ═══════════════════════════════════════════════════════════


def _find_onboarding_frame(page: Page, kind: str, check_abort: Optional[Callable[[], bool]] = None) -> Any:
    """Find Stripe onboarding iframe by title or src componentName."""
    if kind == "payment":
        selectors = [
            "iframe[title='Secure payment input frame']",
            "iframe[src*='componentName=payment']",
        ]
    else:
        selectors = [
            "iframe[title='Secure address input frame']",
            "iframe[src*='componentName=address']",
        ]
    return _first_frame_locator(page, selectors, f"Stripe {kind}", check_abort=check_abort)


def fill_onboarding_card(
    page: Page,
    card_number: str,
    exp_month: str,
    exp_year: str,
    cvc: str,
    check_abort: Optional[Callable[[], bool]] = None,
) -> None:
    """Human-like card entry in Stripe Payment Element iframe (onboarding flow).

    page.keyboard.press() sends keys to the MAIN page, not iframes.
    Must use locator.press_sequentially() to type inside cross-origin iframes.
    """
    frame = _find_onboarding_frame(page, "payment", check_abort=check_abort)
    _rs(0.6, 1.2)

    num_loc = frame.locator("input[name='number']")
    num_loc.click()
    _rs(0.25, 0.5)
    num_loc.press_sequentially(card_number, delay=random.randint(60, 140))
    _rs(0.4, 0.9)

    exp_loc = frame.locator("input[name='expiry']")
    exp_loc.click()
    _rs(0.15, 0.35)
    exp_yy = exp_year[-2:] if len(exp_year) >= 4 else exp_year
    exp_loc.press_sequentially(f"{exp_month}{exp_yy}", delay=random.randint(70, 170))
    _rs(0.35, 0.75)

    cvc_loc = frame.locator("input[name='cvc']")
    cvc_loc.click()
    _rs(0.4, 1.0)
    cvc_loc.press_sequentially(cvc, delay=random.randint(80, 190))
    _rs(0.3, 0.6)

    log.info("Onboarding Payment iframe 填写完成")


def fill_onboarding_address(
    page: Page,
    holder_name: str,
    address: dict[str, str],
    check_abort: Optional[Callable[[], bool]] = None,
) -> None:
    """Human-like address entry in Stripe Address Element iframe (onboarding flow).

    address dict keys: address_line1, city, state (2-letter code), zip

    page.keyboard sends keys to MAIN page. Must use locator methods for iframes.
    """
    frame = _find_onboarding_frame(page, "address", check_abort=check_abort)
    _rs(0.5, 1.0)

    name_el = frame.locator("input[name='name']")
    _clear_and_type_locator(name_el, holder_name)
    _rs(0.3, 0.6)

    addr_el = frame.locator("input[name='addressLine1']")
    addr_el.click()
    _rs(0.2, 0.4)
    addr_el.press_sequentially(address.get("address_line1", ""), delay=random.randint(40, 100))
    _dismiss_address_autocomplete(addr_el, name_el)

    city_el = frame.locator("input[name='locality']")
    _clear_and_type_locator(city_el, address.get("city", ""), delay_lo=40, delay_hi=110)
    _rs(0.25, 0.5)

    state = address.get("state", "")
    if state:
        frame.locator("select[name='administrativeArea']").select_option(value=state)
        _rs(0.3, 0.6)

    zip_el = frame.locator("input[name='postalCode']")
    _clear_and_type_locator(zip_el, address.get("zip", ""), delay_lo=50, delay_hi=120)
    _rs(0.3, 0.5)

    log.info("Onboarding Address iframe 填写完成")


# ═══════════════════════════════════════════════════════════
# Billing credit amount (dashboard 充值弹窗)
# ═══════════════════════════════════════════════════════════


def fill_credit_amount(page: Page, amount: int) -> None:
    """Set the dollar amount in the 'Add to credit balance' dialog ($5-$100)."""
    amount = max(5, min(100, amount))
    sel = "input[name='amountDollars'], #billing-add-balance-amount"
    page.locator(sel).first.click(click_count=3)
    _rs(0.05, 0.1)
    page.keyboard.press("Backspace")
    _rs(0.08, 0.15)
    page.locator(sel).first.press_sequentially(str(amount), delay=random.randint(60, 120))
    _rs(0.2, 0.4)
    log.info(f"充值金额已设置: ${amount}")


# ═══════════════════════════════════════════════════════════
# Post-submit waiting
# ═══════════════════════════════════════════════════════════


def wait_for_billing_result(
    page: Page,
    timeout: int = 30,
    check_abort: Optional[Callable[[], bool]] = None,
) -> Optional[str]:
    """Poll for payment result. Returns error text or None on success."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        _check_control(check_abort)
        error = _page_error_text(page)
        if error:
            return error

        if _dialog_gone(page):
            return None

        time.sleep(0.5)
    _check_control(check_abort)
    return "绑卡处理超时"


def wait_for_confirm_or_error(
    page: Page,
    timeout: int = 45,
    check_abort: Optional[Callable[[], bool]] = None,
) -> Optional[str]:
    """Wait for 'Confirm payment' page or an error after submitting card.
    Returns error text, or None when confirm page appears."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        _check_control(check_abort)
        if _has_confirm_payment(page):
            return None

        if _has_3ds_challenge(page):
            return "3DS_CHALLENGE"

        error = _page_error_text(page)
        if error:
            return error

        time.sleep(0.5)
    _check_control(check_abort)
    return "等待确认页超时"


def wait_for_payment_done(
    page: Page,
    timeout: int = 45,
    check_abort: Optional[Callable[[], bool]] = None,
) -> Optional[str]:
    """Wait for payment completion after clicking 'Confirm payment'.
    Success = page navigates away from confirm, or shows success/dashboard.
    Returns error text, or None on success."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        _check_control(check_abort)
        if _payment_completed(page):
            return None

        error = _page_error_text(page)
        if error:
            return error

        time.sleep(0.5)
    _check_control(check_abort)
    return "付款处理超时"
