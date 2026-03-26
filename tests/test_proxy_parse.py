"""代理格式解析测试。"""

from backend.adspower import parse_proxy


class TestParseProxy:

    def test_socks5_url(self):
        result = parse_proxy("socks5", "socks5://user:pass@1.2.3.4:1080")
        assert result is not None
        assert result["host"] == "1.2.3.4"
        assert result["port"] == "1080"
        assert result["user"] == "user"
        assert result["pass"] == "pass"

    def test_http_url(self):
        result = parse_proxy("http", "http://proxy.example.com:8080")
        assert result is not None
        assert result["host"] == "proxy.example.com"
        assert result["port"] == "8080"
        assert result["user"] == ""

    def test_host_port_user_pass(self):
        result = parse_proxy("socks5", "1.2.3.4:1080:admin:secret")
        assert result is not None
        assert result["host"] == "1.2.3.4"
        assert result["port"] == "1080"
        assert result["user"] == "admin"
        assert result["pass"] == "secret"

    def test_host_port_only(self):
        result = parse_proxy("http", "1.2.3.4:8080")
        assert result is not None
        assert result["host"] == "1.2.3.4"
        assert result["port"] == "8080"
        assert result["user"] == ""

    def test_direct_returns_none(self):
        result = parse_proxy("direct", "anything")
        assert result is None

    def test_empty_string_returns_none(self):
        result = parse_proxy("socks5", "  ")
        assert result is None
