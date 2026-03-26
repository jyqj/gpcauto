"""OpenAI Platform 注册主流程"""

import logging
import random
import string
import time
from typing import Any, Callable, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from . import adspower, email_service, browser_utils, card_service, db, address_data
from .constants import CARD_FAIL_3DS, CARD_FAIL_DECLINE, CARD_FAIL_INSUFFICIENT, CARD_STATUS_DISABLED

log = logging.getLogger("reg.flow")


class BindFailure(Exception):
    """Card bind failed — carries structured card info for the UI."""
    def __init__(self, message: str, card_id: Optional[int] = None, fail_tag: str = "") -> None:
        super().__init__(message)
        self.card_id = card_id
        self.fail_tag = fail_tag


class TaskAborted(Exception):
    """Raised when the task is cancelled mid-registration."""
    pass

# ─── 随机数据池 ───────────────────────────────────────────

_FIRST_NAMES = [
    "James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph",
    "Thomas", "Charles", "Christopher", "Daniel", "Matthew", "Anthony", "Mark",
    "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth", "Susan",
    "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
    "Emily", "Hannah", "Madison", "Ashley", "Olivia", "Sophia", "Emma", "Abigail",
    "Alexander", "Benjamin", "Nathan", "Samuel", "Ryan", "Andrew", "Joshua", "Brandon",
    "Tyler", "Austin", "Jacob", "Ethan", "Noah", "Lucas", "Mason", "Logan",
    "Chloe", "Grace", "Ella", "Lily", "Zoe", "Mia", "Aria", "Layla",
    "Henry", "Jack", "Owen", "Caleb", "Luke", "Isaac", "Dylan", "Connor",
    "Adrian", "Cole", "Max", "Leo", "Ian", "Evan", "Gavin", "Nolan",
    "Rachel", "Natalie", "Victoria", "Samantha", "Haley", "Brooke", "Paige", "Audrey",
]

_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
    "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera",
    "Campbell", "Mitchell", "Carter", "Roberts", "Turner", "Phillips", "Parker",
    "Evans", "Edwards", "Collins", "Stewart", "Morris", "Reed", "Cook",
    "Morgan", "Bell", "Murphy", "Bailey", "Rivera", "Cooper", "Cox", "Ward",
    "Peterson", "Gray", "James", "Watson", "Brooks", "Kelly", "Sanders", "Price",
]

_ORG_NAMES = [
    "Personal", "My Projects", "Dev Lab", "Research", "Side Projects",
    "Learning", "Experiments", "Workshop", "Studio", "Sandbox",
    "Tech Hub", "Innovation Lab", "Creative Works", "Digital Studio",
    "Open Lab", "Code Space", "Build Lab", "Project Hub", "Test Bench",
]

_ORG_ROLES = ["Student", "Hobbyist", "Engineer", "Data Scientist", "Business Professional"]


def _random_name() -> str:
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"


def _random_birthday() -> tuple[str, str, str]:
    year = random.randint(1985, 2002)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return str(year), str(month).zfill(2), str(day).zfill(2)


def _random_org() -> str:
    return random.choice(_ORG_NAMES)


def _random_role() -> str:
    return random.choice(_ORG_ROLES)


def _gen_password(length: int = 16) -> str:
    chars = string.ascii_letters + string.digits + "!@#$%&"
    pwd = list(random.choices(chars, k=length))
    positions = random.sample(range(length), 3)
    pwd[positions[0]] = random.choice(string.ascii_uppercase)
    pwd[positions[1]] = random.choice(string.digits)
    pwd[positions[2]] = random.choice("!@#$%&")
    return "".join(pwd)


def _cleanup_profile(profile_id: str, account_id: Optional[int] = None, reason: str = "") -> bool:
    """统一清理入口的内部别名。"""
    return adspower.cleanup_profile(profile_id, account_id=account_id, reason=reason)


def _get_card_address(card: dict[str, Any]) -> Optional[dict[str, str]]:
    """Extract stored address from card, or None if empty."""
    if card.get("address_line1"):
        return {
            "address_line1": card["address_line1"],
            "city": card.get("city", ""),
            "state": card.get("state", ""),
            "zip": card.get("zip", ""),
            "country": card.get("country", "US"),
        }
    return None


def run(
    proxy: Optional[dict[str, str]] = None,
    mode: str = "register",
    card_id: Optional[int] = None,
    credit_amount: int = 5,
    bind_path: str = "onboarding",
    recharge_upper: int = -1,
    recharge_lower: int = 5,
    on_step: Optional[Callable[[int, int, str], None]] = None,
    check_abort: Optional[Callable[[], bool]] = None,
    cleanup_mode: str = "always",  # "always" | "defer_manual"
) -> dict[str, Any]:
    """
    完整注册流程。
    check_abort: 返回 True 时立即中断（用于 cancel/stop）
    """
    has_recharge = mode == "register_and_bind" and recharge_upper >= 0
    if mode == "register_and_bind":
        total = 13 if has_recharge else 12
    else:
        total = 11
    profile_id = None
    reg_success = False

    def _check() -> None:
        if check_abort and check_abort():
            raise TaskAborted("任务已中断")

    def _sleep(seconds: float) -> None:
        end = time.time() + seconds
        while time.time() < end:
            _check()
            time.sleep(min(0.5, end - time.time()))

    def step(n: int, msg: str) -> None:
        _check()
        log.info(f"[{n}/{total}] {msg}")
        if on_step:
            on_step(n, total, msg)

    name = _random_name()
    year, month, day = _random_birthday()
    org_name = _random_org()
    role = _random_role()

    try:
        # 1 — AdsPower
        step(1, "创建 AdsPower 浏览器环境...")
        profile_id = adspower.create_profile(proxy=proxy)

        # 2 — 启动浏览器
        step(2, "启动浏览器...")
        info = adspower.start_browser(profile_id)
        _sleep(3)

        # 3 — 邮箱
        step(3, "创建临时邮箱...")
        email = email_service.create_email()
        password = _gen_password()
        step(3, f"邮箱: {email}")

        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(info["ws_url"])
            _sleep(1)
            ctx = browser.contexts[0] if browser.contexts else browser.new_context()
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.set_default_timeout(30000)
            _sleep(1)

            # 4 — 登录页
            step(4, "打开 platform.openai.com ...")
            browser_utils.goto_interruptible(
                page,
                "https://platform.openai.com/login",
                timeout=45000,
                check_abort=check_abort,
                wait_until="commit",
            )
            _check()
            browser_utils.wait_for_selector_interruptible(
                page, "#login-email", timeout=15000, check_abort=check_abort
            )
            _sleep(1)

            # 5 — 邮箱
            step(5, "填写邮箱...")
            browser_utils.react_set_value(page, "#login-email", email)
            _sleep(0.5)
            browser_utils.click_button_by_text(page, "Continue")

            # 6 — 密码
            browser_utils.wait_for_selector_interruptible(
                page, "input[name='new-password']", timeout=15000, check_abort=check_abort
            )
            _check()
            _sleep(1)
            step(6, "设置密码...")
            browser_utils.react_set_value(page, "input[name='new-password']", password)
            _sleep(0.5)
            browser_utils.click_button_by_text(page, "Continue")

            # 7 — OTP
            browser_utils.wait_for_selector_interruptible(
                page, "input[name='code']", timeout=15000, check_abort=check_abort
            )
            _check()
            step(7, "等待 OTP 邮件...")
            otp = email_service.poll_otp(email, timeout=120, check_abort=check_abort)
            if not otp:
                raise RuntimeError("OTP 超时")
            step(7, f"OTP: {otp}")

            # 8 — 输入 OTP
            step(8, "填入 OTP...")
            browser_utils.react_set_value(page, "input[name='code']", otp)
            _sleep(0.5)
            browser_utils.click_button_by_text(page, "Continue")

            # 9 — 姓名 + 生日
            browser_utils.wait_for_selector_interruptible(
                page, "input[name='name']", timeout=15000, check_abort=check_abort
            )
            _check()
            _sleep(1)
            step(9, f"填写姓名({name})和生日...")
            browser_utils.react_set_value(page, "input[name='name']", name)
            _sleep(0.3)
            browser_utils.fill_birthday(page, year, month, day)
            birthday_val = page.evaluate(
                "document.querySelector('input[name=\"birthday\"]')?.value"
            )
            step(9, f"生日: {birthday_val}")
            browser_utils.click_submit(page)
            _sleep(1)

            # 10 — 组织
            try:
                browser_utils.wait_for_onboarding_create_interruptible(
                    page, timeout=30000, check_abort=check_abort
                )
            except PlaywrightTimeoutError:
                if "about-you" not in page.url and "callback" not in page.url:
                    raise
                if "callback" in page.url:
                    step(9, "OAuth callback 仍在跳转，继续等待...")
                    browser_utils.wait_for_onboarding_create_interruptible(
                        page, timeout=30000, check_abort=check_abort
                    )
                step(9, "about-you 未跳转，重试提交...")
                browser_utils.click_submit(page)
                _sleep(1)
                browser_utils.wait_for_onboarding_create_interruptible(
                    page, timeout=30000, check_abort=check_abort
                )
            _check()
            _sleep(2)
            step(10, f"创建组织({org_name} / {role})...")
            browser_utils.react_set_value(page, "#organization-name", org_name)
            _sleep(0.5)
            browser_utils.click_radix_select(page, role)
            _sleep(0.8)

            # 这里不能只依赖 locator.click()：
            # 当前 welcome/create 页面偶发“按钮点了但表单没真正提交”，
            # 表现为看起来像是在 invite 步骤闪退，实际上 URL 仍停在 step=create。
            try:
                browser_utils.click_submit(page, retries=5, delay=0.8)
            except Exception:
                page.locator("button:has-text('Create organization')").first.click(timeout=5000)
            _sleep(1.5)

            next_step = browser_utils.wait_for_onboarding_invite_or_try_interruptible(
                page, timeout=20000, check_abort=check_abort
            )
            _check()
            _sleep(1)
            if next_step == "invite":
                step(10, "跳过邀请...")
                browser_utils.click_invite_later(page)
                browser_utils.wait_for_api_key_cta_interruptible(
                    page, timeout=20000, check_abort=check_abort
                )
            else:
                step(10, "已直接进入 API Key 引导，跳过邀请页")
                browser_utils.wait_for_api_key_cta_interruptible(
                    page, timeout=20000, check_abort=check_abort
                )
            _check()
            _sleep(2)

            # 11 — API Key
            step(11, "生成 API Key...")
            browser_utils.click_api_key_cta(page)
            _sleep(5)
            api_key = None
            for _attempt in range(10):
                api_key = page.evaluate("""(() => {
                    for (const inp of document.querySelectorAll('input')) {
                        if (inp.value && inp.value.startsWith('sk-')) return inp.value;
                    }
                    return null;
                })()""")
                if api_key:
                    break
                _sleep(2)
            device_id = page.evaluate(
                "document.cookie.match(/oai-did=([^;]+)/)?.[1] || ''"
            )

            account_data = {
                "email": email,
                "password": password,
                "api_key": api_key or "",
                "device_id": device_id,
                "name": name,
                "birthday": birthday_val or "",
                "organization": org_name,
                "ads_profile_id": profile_id,  # 先存真实值，cleanup 成功后再清空
            }
            step(11, f"注册完成! API Key: {(api_key or '')[:40]}...")

            # 12 — 绑卡（可选）
            if mode == "register_and_bind":
                step(12, "准备绑卡...")
                card = None
                auto_assigned = False
                if card_id:
                    card = db.get_card(card_id)
                else:
                    card = card_service.get_available_card()
                    auto_assigned = card is not None

                if not card:
                    step(12, "无可用卡，跳过")
                    raise RuntimeError("无可用卡")

                bind_address = _get_card_address(card)
                if not bind_address:
                    bind_address = address_data.get_random_address(tax_free_only=True)
                holder = card.get("holder_name") or name

                try:
                    if bind_path == "billing":
                        step(12, "走 Billing 页面绑卡路径...")
                        card_service.bind_card_billing(
                            page, card, bind_address, holder,
                            credit_amount=credit_amount,
                            on_log=lambda msg: step(12, msg),
                            check_abort=check_abort,
                        )
                    else:
                        card_service.bind_card_onboarding(
                            page, card, bind_address, holder,
                            credit_amount=credit_amount,
                            on_log=lambda msg: step(12, msg),
                            check_abort=check_abort,
                        )
                except card_service.CardDeclined as e:
                    tag = CARD_FAIL_INSUFFICIENT if "insufficient" in str(e).lower() else CARD_FAIL_DECLINE
                    db.set_card_fail_tag(card["id"], tag)
                    db.batch_update_cards([card["id"]], status=CARD_STATUS_DISABLED)
                    raise BindFailure(f"绑卡被拒 [{tag}]: {e}", card_id=card["id"], fail_tag=tag)
                except card_service.Card3DSFailed as e:
                    db.set_card_fail_tag(card["id"], CARD_FAIL_3DS)
                    db.batch_update_cards([card["id"]], status=CARD_STATUS_DISABLED)
                    raise BindFailure(f"绑卡失败 [3ds]: {e}", card_id=card["id"], fail_tag=CARD_FAIL_3DS)
                except card_service.CardBindError as e:
                    db.set_card_fail_tag(card["id"], CARD_FAIL_DECLINE)
                    db.batch_update_cards([card["id"]], status=CARD_STATUS_DISABLED)
                    raise BindFailure(f"绑卡被拒: {e}", card_id=card["id"], fail_tag=CARD_FAIL_DECLINE)
                except BindFailure:
                    raise
                except Exception as e:
                    raise BindFailure(f"绑卡异常: {e}", card_id=card["id"], fail_tag="")

                # 绑卡成功才入库（事务化：账号入库 + 卡计数 + 清标签 原子完成）
                account_data["card_id"] = card["id"]
                account_data["credit_loaded"] = credit_amount
                account_id = db.insert_account_with_card(
                    account_data,
                    card_id=card["id"],
                    increment_use=not auto_assigned,
                )
                reg_success = True
                step(12, f"绑卡完成! 已充 ${credit_amount}")

                # 13 — 追充（可选，tier 策略）
                if has_recharge:
                    upper_label = card_service._tier_label(recharge_upper)
                    lower_label = card_service._tier_label(recharge_lower)
                    step(13, f"开始追充: {upper_label} → {lower_label}, 上限 ${card_service.MAX_CREDIT_PER_CARD}...")

                    def _on_charged(amt: int) -> None:
                        db.add_credit(account_id, amt)

                    loaded, reason = card_service.recharge_with_tiers(
                        page, credit_amount,
                        upper=recharge_upper, lower=recharge_lower,
                        on_log=lambda msg: step(13, msg),
                        on_charged=_on_charged,
                        check_abort=check_abort,
                    )
                    account_data["credit_loaded"] = loaded
                    step(13, f"追充完成, 总额度 ${loaded:.0f} ({reason})")
            else:
                account_id = db.insert_account(account_data)
                reg_success = True

        # 返回结果中携带 _profile_id 供调用方使用（如手动续充需要浏览器）
        return {**account_data, "id": account_id, "_profile_id": profile_id}

    finally:
        if profile_id:
            if not reg_success:
                log.info(f"注册失败，清理 AdsPower 环境: {profile_id}")
                _cleanup_profile(profile_id, reason="register_failed")
            elif cleanup_mode == "always":
                log.info(f"流程完成，清理 AdsPower 环境: {profile_id}")
                _cleanup_profile(profile_id, account_id=account_id, reason="register_success")
            # cleanup_mode == "defer_manual" 时由调用方负责清理
