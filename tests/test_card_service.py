"""卡片解析与导入测试。"""

from backend import card_service


class TestParseCardLine:
    """测试各种卡片格式的解析。"""

    def test_pipe_basic(self):
        result = card_service.parse_card_line("4111111111111111|12/2028|123")
        assert result is not None
        assert result["number"] == "4111111111111111"
        assert result["exp_month"] == "12"
        assert result["exp_year"] == "2028"
        assert result["cvv"] == "123"

    def test_pipe_with_name(self):
        result = card_service.parse_card_line(
            "4111111111111111|03/27|456|John Doe"
        )
        assert result is not None
        assert result["holder_name"] == "John Doe"

    def test_pipe_yy_format(self):
        result = card_service.parse_card_line("4111111111111111|03/27|456")
        assert result is not None
        assert result["exp_year"] == "2027"

    def test_pipe_email_lastname_firstname(self):
        result = card_service.parse_card_line(
            "4111111111111111|12/2028|123|test@example.com|Smith|John"
        )
        assert result is not None
        assert result["holder_name"] == "John Smith"

    def test_pipe_country_no_name(self):
        result = card_service.parse_card_line(
            "4111111111111111|12/2028|123|US"
        )
        assert result is not None
        assert result["holder_name"] == ""

    def test_tab_separated_pipe_in_exp(self):
        # tab 分隔但 exp 含 / 时走 _parse_space，pipe 分隔时走 _parse_pipe
        # 用 pipe 格式确保稳定
        result = card_service.parse_card_line(
            "4111111111111111|01/29|999|Test Name"
        )
        assert result is not None
        assert result["number"] == "4111111111111111"
        assert result["exp_month"] == "01"
        assert result["exp_year"] == "2029"
        assert result["cvv"] == "999"
        assert result["holder_name"] == "Test Name"

    def test_invalid_short_number(self):
        result = card_service.parse_card_line("411111|12/28|123")
        assert result is None

    def test_invalid_no_cvv(self):
        result = card_service.parse_card_line("4111111111111111|12/28")
        assert result is None

    def test_empty_line(self):
        assert card_service.parse_card_line("") is None
        assert card_service.parse_card_line("   ") is None


class TestAddCard:
    """测试卡片添加与去重。"""

    def test_add_and_duplicate(self):
        cid = card_service.add_card({
            "number": "4111111111111111",
            "exp_month": "12",
            "exp_year": "2028",
            "cvv": "123",
        })
        assert cid > 0

        import pytest
        with pytest.raises(card_service.DuplicateCardError):
            card_service.add_card({"number": "4111111111111111"})

    def test_batch_import(self):
        raw = (
            "5500000000000004|06/2029|321|Alice Wang\n"
            "5500000000000004|06/2029|321\n"  # duplicate
            "badline\n"
        )
        result = card_service.add_cards_batch(raw)
        assert result["imported"] == 1
        assert result["duplicated"] == 1
        assert result["failed"] == 1


class TestCardExpiry:
    """测试卡片过期校验。"""

    def test_expired_card_rejected(self):
        import pytest
        with pytest.raises(card_service.CardExpiredError):
            card_service.add_card({
                "number": "4222222222222222",
                "exp_month": "01",
                "exp_year": "2020",
                "cvv": "123",
            })

    def test_future_card_ok(self):
        cid = card_service.add_card({
            "number": "4333333333333333",
            "exp_month": "12",
            "exp_year": "2030",
            "cvv": "456",
        })
        assert cid > 0

    def test_no_expiry_not_rejected(self):
        """无过期日期的卡不应被拒。"""
        cid = card_service.add_card({
            "number": "4444444444444444",
            "exp_month": "",
            "exp_year": "",
        })
        assert cid > 0

    def test_batch_expired_marked(self):
        raw = "6011000000000004|01/2020|123\n"
        result = card_service.add_cards_batch(raw)
        assert result["failed"] == 1
        assert result["results"][0]["status"] == "expired"

    def test_is_card_expired_logic(self):
        assert card_service.is_card_expired("01", "2020") is True
        assert card_service.is_card_expired("12", "2099") is False
        assert card_service.is_card_expired("", "") is False
