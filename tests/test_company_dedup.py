"""Unit tests for the live company duplicate check used by create_company / search_companies.

lambda_function.py reads S3 at import time, so the helpers are pulled out with `ast` and
call_pipeline_api is stubbed. Run: python3 -m unittest discover tests
"""
import ast
import logging
import pathlib
import re
import unittest
import urllib.parse

_SRC = pathlib.Path(__file__).resolve().parent.parent / "lambda_function.py"
_tree = ast.parse(_SRC.read_text())
_FUNCS = {"_company_tokens", "_company_match_score", "_normalize_domain", "_company_search_terms",
          "_live_company_search", "find_live_company_duplicates", "live_person_company"}
_nodes = [n for n in _tree.body
          if (isinstance(n, ast.FunctionDef) and n.name in _FUNCS)
          or (isinstance(n, ast.Assign) and any(getattr(t, "id", None) == "_COMPANY_NOISE_WORDS" for t in n.targets))]


def _load(api):
    ns = {"re": re, "urllib": urllib, "logger": logging.getLogger("t"), "call_pipeline_api": api}
    exec(compile(ast.Module(body=_nodes, type_ignores=[]), str(_SRC), "exec"), ns)
    return ns


# Live Pipeline state at the time of the incident: the signup-created company exists, the snapshot is stale.
LIVE = [{"id": 138879673, "name": "Abseil Automation", "web": "https://www.abseil.ai/"},
        {"id": 5, "name": "Hadrian", "web": "hadrian.co"}]


def fake_api(companies=LIVE, fail=False, person=None):
    calls = []

    def api(method, endpoint, payload=None):
        calls.append(endpoint)
        if fail:
            return {"status": 500, "data": "boom"}
        if endpoint.startswith("/people/"):
            return {"status": 200, "data": person or {}}
        term = urllib.parse.unquote(re.search(r"conditions\[company_name\]=([^&]*)", endpoint).group(1)).lower()
        hits = [c for c in companies if term in c["name"].lower()]
        return {"status": 200, "data": {"entries": hits, "pagination": {"pages": 1}}}
    api.calls = calls
    return api


class NormalizeDomain(unittest.TestCase):
    def test_variants(self):
        nd = _load(fake_api())["_normalize_domain"]
        for raw in ("abseil.ai", "https://abseil.ai", "http://www.abseil.ai/", "WWW.Abseil.AI/team?x=1", "https://abseil.ai:443"):
            self.assertEqual(nd(raw), "abseil.ai", raw)
        self.assertEqual(nd(""), "")
        self.assertEqual(nd(None), "")


class LiveDuplicates(unittest.TestCase):
    def test_incident_exact_name(self):
        ns = _load(fake_api())
        m, err = ns["find_live_company_duplicates"]("Abseil Automation")
        self.assertIsNone(err)
        self.assertEqual(m[0]["id"], 138879673)

    def test_root_and_suffix_variants(self):
        ns = _load(fake_api())
        for name in ("Abseil", "Abseil Automation Inc.", "Abseil Automation, LLC"):
            m, _ = ns["find_live_company_duplicates"](name)
            self.assertTrue(m and m[0]["id"] == 138879673, name)

    def test_domain_match_with_different_name(self):
        ns = _load(fake_api())
        m, _ = ns["find_live_company_duplicates"]("Abseil Robotics Corp", "http://abseil.ai")
        self.assertEqual(m[0]["id"], 138879673)
        self.assertIn("domain", m[0]["match"])

    def test_no_match(self):
        ns = _load(fake_api())
        m, err = ns["find_live_company_duplicates"]("Zipline")
        self.assertEqual(m, [])
        self.assertIsNone(err)

    def test_api_failure_reported(self):
        ns = _load(fake_api(fail=True))
        m, err = ns["find_live_company_duplicates"]("Abseil Automation")
        self.assertEqual(m, [])
        self.assertTrue(err)

    def test_pagination(self):
        pages = {1: [{"id": 1, "name": "Other Abseil Thing"}], 2: [{"id": 2, "name": "Abseil Automation"}]}

        def api(method, endpoint, payload=None):
            p = int(re.search(r"[?&]page=(\d+)", endpoint).group(1))
            return {"status": 200, "data": {"entries": pages.get(p, []), "pagination": {"pages": 2}}}
        ns = _load(api)
        self.assertEqual({c["id"] for c in ns["_live_company_search"]("Abseil")}, {1, 2})


class LivePersonCompany(unittest.TestCase):
    def test_reads_company_id(self):
        ns = _load(fake_api(person={"id": 1310500564, "company_id": 138879673, "company_name": "Abseil Automation"}))
        self.assertEqual(ns["live_person_company"](1310500564), (138879673, "Abseil Automation"))

    def test_no_company(self):
        ns = _load(fake_api(person={"id": 1}))
        self.assertEqual(ns["live_person_company"](1), (None, None))

    def test_api_failure_raises(self):
        ns = _load(fake_api(fail=True))
        with self.assertRaises(RuntimeError):
            ns["live_person_company"](1)


if __name__ == "__main__":
    unittest.main()
