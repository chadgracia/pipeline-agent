"""Unit tests for person client-folder resolution (identity clues + nickname matching).

lambda_function.py reads S3 at import time, so the helpers are pulled out with `ast` and exec'd
against fake Drive / Pipeline functions. Run: python3 -m unittest discover tests
"""
import ast
import json
import logging
import os
import pathlib
import re
import unicodedata
import unittest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SRC = _ROOT / "lambda_function.py"
_FUNCS = {"_load_nicknames", "_name_tokens", "_split_person_name", "_first_name_relation",
          "_first_names_consistent", "_extract_emails", "_is_internal_email", "_names_with_last",
          "_names_signed", "_display_part", "_pick_identity", "_person_folder_candidates",
          "_match_person_folders", "_attachment_text", "_gather_identity_clues", "_resolve_person_folder",
          "_folder_result", "_pdf_text"}
_CONSTS = {"_INTERNAL_DOMAINS", "_NAME_SUFFIXES", "_NOT_FIRST_NAMES", "_EMAIL_RE", "_PDF_MAX_CHARS", "_PDF_MAX_PAGES"}

_tree = ast.parse(_SRC.read_text())
_nodes = [n for n in _tree.body
          if (isinstance(n, ast.FunctionDef) and n.name in _FUNCS)
          or (isinstance(n, ast.Assign) and any(getattr(t, "id", None) in _CONSTS for t in n.targets))]
NICK = None

CLIENTS = "15tJGEiOe4eKszNHDLrm5wG_8C6Icn5wo"
ZACHARY = {"id": "18eBlIEiTZqf_SoalFUwN7Rm9ayyXmw5R", "name": "Miltenberger, Zachary"}
ZACH = {"id": "1DQoUdg8kpIZ7DSXpz5biRo_dZYuDQD1n", "name": "Miltenberger, Zach"}
OTHERS = [{"id": "o1", "name": "Miller, Zach"}, {"id": "o2", "name": "Acme Capital Partners LLC"}]
ZIPLINE_PDF = "Zipline-Stock Transfer Agreement signed by Zachary Miltenberger.pdf"


def _env(folders, people=None, email=None, contents=None):
    """Fresh namespace with fake Drive/Pipeline. Returns (ns, created_names)."""
    created = []
    ns = {"re": re, "os": os, "json": json, "unicodedata": unicodedata,
          "logger": logging.getLogger("test"), "PdfReader": None,
          "DRIVE_CLIENTS_FOLDER_ID": CLIENTS, "DRIVE_FILES_URL": "https://drive/files"}
    exec(compile(ast.Module(body=_nodes, type_ignores=[]), str(_SRC), "exec"), ns)
    nick = ns["_load_nicknames"](str(_ROOT / "names.csv"))

    def drive_request(method, url, params=None, payload=None):
        created.append(payload["name"])
        return {"status": 200, "data": {"id": "new1", "name": payload["name"]}}

    ns.update({
        "_nickname_map": lambda: nick,
        "_CURRENT_EMAIL": email or {},
        "_list_client_folders": lambda: (list(folders), None),
        "_drive_request": drive_request,
        "_lookup_person_by_email": lambda e: (people or {}).get(e),
        "_folders_mentioning": lambda fs, terms: {f["id"] for f in fs if f["id"] in (contents or set())},
    })
    return ns, created


def _resolve(ns, name, attachments=None, create_if_missing=True, create_new=False):
    return ns["_resolve_person_folder"](name, attachments or [], create_if_missing, create_new)


def _att(filename, text=b"", mime="text/plain"):
    return {"index": 1, "filename": filename, "mime": mime, "data": text}


class NicknameDataTest(unittest.TestCase):
    def test_zach_zachary_are_variants(self):
        ns, _ = _env([])
        nick = ns["_nickname_map"]()
        self.assertEqual(ns["_first_name_relation"]("zach", "zachary", nick), "nickname")
        self.assertEqual(ns["_first_name_relation"]("zachary", "zachary", nick), "first name")
        self.assertIsNone(ns["_first_name_relation"]("zach", "sarah", nick))


class ZachZacharyTest(unittest.TestCase):
    def test_document_name_upgrades_nickname_and_reuses_folder(self):
        ns, created = _env([ZACHARY] + OTHERS)
        r = _resolve(ns, "Zach Miltenberger", [_att(ZIPLINE_PDF, mime="application/pdf")])
        self.assertEqual(r["status"], "exists")
        self.assertEqual(r["folder_id"], ZACHARY["id"])
        self.assertEqual(r["matched_by"], "first name")
        self.assertEqual(r["identity"]["source"], "document name")
        self.assertEqual(r["identity"]["first_name"], "Zachary")
        self.assertEqual(created, [])

    def test_nickname_alone_matches_existing_folder(self):
        ns, created = _env([ZACHARY] + OTHERS)
        r = _resolve(ns, "Zach Miltenberger")
        self.assertEqual((r["status"], r["folder_id"], r["matched_by"]), ("exists", ZACHARY["id"], "nickname"))
        self.assertEqual(created, [])

    def test_pipeline_email_beats_document_and_body(self):
        people = {"zach@example.com": {"id": 42, "first_name": "Zachary", "last_name": "Miltenberger",
                                       "email": "zach@example.com"}}
        email = {"from": "cgracia@rainmakersecurities.com", "subject": "Fwd: transfer",
                 "body": "From: Zach Miltenberger <zach@example.com>\nThanks,\nZach Miltenberger"}
        ns, created = _env([ZACHARY] + OTHERS, people=people, email=email)
        r = _resolve(ns, "Zach Miltenberger")
        self.assertEqual(r["identity"]["source"], "email")
        self.assertEqual(r["folder_id"], ZACHARY["id"])
        self.assertEqual(created, [])

    def test_last_first_form_and_accents(self):
        ns, _ = _env([{"id": "j1", "name": "Muñoz, José"}])
        r = _resolve(ns, "Munoz, Jose")
        self.assertEqual((r["status"], r["folder_id"], r["matched_by"]), ("exists", "j1", "first name"))


class TwoCandidatesTest(unittest.TestCase):
    def test_zach_and_zachary_folders_ask_instead_of_choosing(self):
        ns, created = _env([ZACHARY, ZACH] + OTHERS)
        r = _resolve(ns, "Zach Miltenberger", [_att(ZIPLINE_PDF, mime="application/pdf")])
        self.assertEqual(r["status"], "ambiguous")
        self.assertEqual(r["reason"], "multiple_person_folders")
        self.assertEqual({f["folder_id"]: f["matched_by"] for f in r["folders"]},
                         {ZACHARY["id"]: "first name", ZACH["id"]: "nickname"})
        self.assertEqual(created, [])

    def test_folder_contents_adds_a_candidate(self):
        trust = {"id": "t1", "name": "Miltenberger Family Trust"}
        ns, created = _env([ZACHARY, trust], contents={"t1"})
        r = _resolve(ns, "Zachary Miltenberger")
        self.assertEqual(r["status"], "ambiguous")
        self.assertEqual({f["folder_id"]: f["matched_by"] for f in r["folders"]},
                         {ZACHARY["id"]: "first name", "t1": "folder contents"})
        self.assertEqual(created, [])


class NoMatchTest(unittest.TestCase):
    def test_creates_last_first_with_full_first_name_from_document(self):
        ns, created = _env([{"id": "s1", "name": "Miltenberger, Sarah"}] + OTHERS)
        r = _resolve(ns, "Zach Miltenberger", [_att(ZIPLINE_PDF, mime="application/pdf")])
        self.assertEqual(r["status"], "created")
        self.assertEqual(r["matched_by"], "new folder")
        self.assertEqual(created, ["Miltenberger, Zachary"])

    def test_signature_nickname_never_beats_pdf_text(self):
        email = {"subject": "Signed docs", "body": "Here you go.\n\nBest,\nZach Miltenberger"}
        pdf_text = b"TRANSFEREE: ZACHARY J. MILTENBERGER\nBy: ____________"
        ns, created = _env(OTHERS, email=email)
        r = _resolve(ns, "Zach Miltenberger", [_att("agreement.txt", pdf_text)])
        self.assertEqual(r["identity"]["source"], "document name")
        self.assertEqual(created, ["Miltenberger, Zachary"])

    def test_not_found_when_create_disabled(self):
        ns, created = _env(OTHERS)
        r = _resolve(ns, "Zachary Miltenberger", create_if_missing=False)
        self.assertEqual(r["status"], "not_found")
        self.assertEqual(created, [])

    def test_unrelated_party_in_document_is_ignored(self):
        text = b"Agreement between Jane Doe (jane@buyer.com) and Zachary Miltenberger"
        people = {"jane@buyer.com": {"id": 7, "first_name": "Jane", "last_name": "Doe", "email": "jane@buyer.com"}}
        ns, created = _env(OTHERS, people=people)
        r = _resolve(ns, "Zach Miltenberger", [_att("agreement.txt", text)])
        self.assertEqual(r["identity"]["first_name"], "Zachary")
        self.assertEqual(created, ["Miltenberger, Zachary"])


if __name__ == "__main__":
    unittest.main()
