# Third-party notices

## names.csv
Nickname relationships vendored unchanged from https://github.com/carltonnorthern/nicknames
(`names.csv`, master branch, fetched 2026-10-22). The same data ships in the PyPI `nicknames`
package, whose metadata declares the Apache-2.0 license. Used by `lambda_function.py` to treat
first names like Zach / Zachary as the same person when matching client Drive folders.

## pypdf
Installed into the Lambda deploy zip by `.github/workflows/deploy.yml` (pinned version, BSD-3-Clause).
Used to extract text from PDF attachments when resolving client folders.
