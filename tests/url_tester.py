from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

try:
    req = Request(
             url = "https://macrometa.com/docs/",
             headers = {"User-Agent": "Mozilla/5.0"}
          )

    # Add your urls below
    urls = [
        "https://macrometa.com/docs/account-management/api-keys/",
        "https://macrometa.com/docs/account-management/auth/user-auth"
    ]

    for url in urls:
        req.full_url = url
        urlopen(req)

except HTTPError as e:
    print("Page down: ", req.full_url, " ", e)

except URLError as e:
    print("Page down: ", req.full_url, " ", e)

else:
    print("All pages are up!")
