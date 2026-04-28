import time
import hmac
import hashlib
import base64
import httpx


class DingTalkNotifier:
    def __init__(self, webhook, secret=None):
        self.webhook = webhook
        self.secret = secret

    def _sign(self, timestamp):
        if not self.secret:
            return ""
        sign_str = f"{timestamp}\n{self.secret}"
        digest = hmac.new(
            self.secret.encode("utf-8"),
            sign_str.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def send_markdown(self, title, text):
        timestamp = str(round(time.time() * 1000))
        url = self.webhook
        if self.secret:
            sign = self._sign(timestamp)
            url = f"{self.webhook}&timestamp={timestamp}&sign={sign}"

        payload = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": text},
        }
        resp = httpx.post(url, json=payload, timeout=10)
        return resp.json()

    def send_signal(self, buy_list, sell_list, date_str):
        title = f"交易信号 - {date_str}"
        text = f"# {title}\n\n"
        if buy_list:
            text += "## 买入信号\n| 代码 | 名称 |\n|------|------|\n"
            for item in buy_list:
                text += f"| {item['code']} | {item['name']} |\n"
        if sell_list:
            text += "\n## 卖出信号\n| 代码 | 名称 |\n|------|------|\n"
            for item in sell_list:
                text += f"| {item['code']} | {item['name']} |\n"
        return self.send_markdown(title, text)
