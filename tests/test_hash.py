"""
Test the implementation of the dropbox hash.
"""

import sys

import requests

sys.path.append(".")

from dropbox_xcode import DropboxHash

def test_hash():
    expected_value = "485291fa0ee50c016982abbfa943957bcd231aae0492ccbaa22c58e3997b35e0"
    hasher = DropboxHash()

    r = requests.get('https://www.dropbox.com/static/images/developers/milky-way-nasa.jpg')
    for chunk in r.iter_content(3000):
        hasher.update(chunk)

    digest = hasher.hexdigest()

    print(digest)

    assert(hasher.hexdigest() == expected_value)

test_hash()
