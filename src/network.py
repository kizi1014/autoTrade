import akshare.utils.request as _ak_req

_original = _ak_req.request_with_retry

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Referer": "https://quote.eastmoney.com/",
}


def _patched(url, params=None, timeout=15, **kwargs):
    kwargs.setdefault("headers", {})
    kwargs["headers"].update(HEADERS)
    return _original(url, params=params, timeout=timeout, **kwargs)


_ak_req.request_with_retry = _patched
