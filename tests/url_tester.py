# flake8: noqa
import glob
import os
import re
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def get_md_files():
    """Return list of markdown files in root directory and subdirectories"""
    files = []
    absolute_path = os.path.dirname(__file__)
    dir_path = r"{}/../**/*.md".format(absolute_path)
    for file in glob.glob(dir_path, recursive=True):
        files.append(file)

    return files


def find_md_links(md):
    """Return list of links in markdown"""
    regex_url = re.compile(
        "((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)"
        "(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\("
        "([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    )
    links = list(regex_url.findall(md))

    return links


files = get_md_files()

is_broken = False
for file in files:
    with open(file, "r") as f:
        text = f.read()
        urls = find_md_links(text)

    for url in urls:
        req = Request(url=url[0], headers={"User-Agent": "Mozilla/5.0"})
        try:
            resp = urlopen(req)
        except HTTPError as e:
            is_broken = True
            print("Page down: ", req.full_url, " ", e)
        except URLError as e:
            is_broken = True
            print("Page down: ", req.full_url, " ", e)

if is_broken is False:
    print("All pages are up!")
else:
    raise RuntimeError("Above pages have broken links")
