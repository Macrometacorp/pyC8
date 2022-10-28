from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


req = Request(
        url = "https://macrometa.com/docs/",
        headers = {"User-Agent": "Mozilla/5.0"}
      )

# Add your urls below
urls = [
    "https://macrometa.com/docs/account-management/api-keys/",
    "https://macrometa.com/docs/account-management/auth/user-auth"
]

is_broken = False
for url in urls:
    req.full_url = url
    try:
        resp = urlopen(req)
    except HTTPError as e:
        is_broken = True
        print("Page down: ", req.full_url, " ", e)
    except URLError as e:
        is_broken = True
        print("Page down: ", req.full_url, " ", e)

if(is_broken is False):
    print("All pages are up!")
