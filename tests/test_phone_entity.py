"""Phone normalization + entity-form detection.

lambda_function.py reads S3 at import time, so the helpers are pulled out with `ast`.
Run: python3 -m unittest discover tests
"""
import ast
import json
import logging
import pathlib
import re
import unittest

_SRC = pathlib.Path(__file__).resolve().parent.parent / "lambda_function.py"
_FUNCS = {"_calling_code", "normalize_phone", "guard_phone_fields", "entity_form_trigger",
          "_same_value", "put_company_checked"}
_ASSIGNS = {"_CALLING_CODES", "PHONE_FIELDS", "_ENTITY_FORM_RE", "PERSON_ADDRESS_FIELDS"}

_tree = ast.parse(_SRC.read_text())
_body = [n for n in _tree.body
         if (isinstance(n, ast.FunctionDef) and n.name in _FUNCS)
         or (isinstance(n, ast.Assign) and any(getattr(t, "id", None) in _ASSIGNS for t in n.targets))]
_ns = {"re": re, "json": json, "logger": logging.getLogger("test"), "_CURRENT_EMAIL": {}, "_CURRENT_ATTACHMENTS": [],
       "_attachment_text": lambda a: a.get("_text", "")}
exec(compile(ast.Module(body=_body, type_ignores=[]), str(_SRC), "exec"), _ns)
normalize_phone = _ns["normalize_phone"]
guard_phone_fields = _ns["guard_phone_fields"]


class NormalizePhoneTest(unittest.TestCase):
    def test_t2d3_trunk_zero_before_country_code(self):
        self.assertEqual(normalize_phone("(0971) 585-629481", "United Arab Emirates"), ("+971585629481", None))

    def test_already_e164(self):
        self.assertEqual(normalize_phone("+380993464098"), ("+380993464098", None))
        self.assertEqual(normalize_phone("+971 58 562 9481"), ("+971585629481", None))

    def test_double_zero_prefix(self):
        self.assertEqual(normalize_phone("00971 585 629481"), ("+971585629481", None))

    def test_us_numbers(self):
        self.assertEqual(normalize_phone("(212) 555-0142"), ("+12125550142", None))
        self.assertEqual(normalize_phone("1-212-555-0142", "US"), ("+12125550142", None))

    def test_national_trunk_zero(self):
        self.assertEqual(normalize_phone("020 7946 0958", "United Kingdom"), ("+442079460958", None))
        self.assertEqual(normalize_phone("058 562 9481", "AE"), ("+971585629481", None))

    def test_country_code_without_plus(self):
        self.assertEqual(normalize_phone("971585629481", "UAE"), ("+971585629481", None))

    def test_no_country_flags_instead_of_guessing(self):
        e164, problem = normalize_phone("(0971) 585-629481")
        self.assertIsNone(e164)
        self.assertIn("no country code", problem)

    def test_bracketed_trunk_zero(self):
        self.assertEqual(normalize_phone("+44 (0) 20 7946 0958"), ("+442079460958", None))

    def test_national_number_starting_with_country_code_digits(self):
        self.assertEqual(normalize_phone("9198765432", "India"), ("+919198765432", None))

    def test_plus_zero_with_hint(self):
        self.assertEqual(normalize_phone("+0971585629481", "United Arab Emirates"), ("+971585629481", None))

    def test_too_long_rejected(self):
        self.assertIsNone(normalize_phone("+1234567890123456")[0])


class GuardPhoneFieldsTest(unittest.TestCase):
    def test_normalizes_in_place(self):
        f = {"phone": "(0971) 585-629481", "first_name": "Abishek"}
        self.assertEqual(guard_phone_fields(f, "United Arab Emirates"), [])
        self.assertEqual(f["phone"], "+971585629481")

    def test_unnormalizable_is_dropped_and_flagged(self):
        f = {"phone": "(0971) 585-629481", "mobile": "+447700900123"}
        flags = guard_phone_fields(f)
        self.assertNotIn("phone", f)
        self.assertEqual(f["mobile"], "+447700900123")
        self.assertEqual(len(flags), 1)


class EntityFormDetectionTest(unittest.TestCase):
    def detect(self, subject="", body="", attachments=()):
        _ns["_CURRENT_EMAIL"] = {"subject": subject, "body": body}
        _ns["_CURRENT_ATTACHMENTS"] = [{"_text": t} for t in attachments]
        return bool(_ns["entity_form_trigger"]())

    def test_entity_iqf(self):
        self.assertTrue(self.detect("Investor Qualification Form for Legal Entities - T2D3 Capital"))
        self.assertTrue(self.detect(attachments=["Principal Place of Business of Entity\n2210 Peninsula Five"]))
        self.assertTrue(self.detect(body="Entity Investor Name: T2D3 Capital"))

    def test_natural_person_forms(self):
        self.assertFalse(self.detect("Investor Qualification Form for Natural Persons - Jane Doe",
                                     "Residence Address: 1 Main St"))
        self.assertFalse(self.detect("Client Engagement Form for Natural Persons - Jane Doe",
                                     attachments=["Name of Natural Person Client\nJane Doe"]))


class EntityTriggerTest(unittest.TestCase):
    def test_names_the_source(self):
        _ns["_CURRENT_EMAIL"] = {"subject": "Fwd: docs", "body": ""}
        _ns["_CURRENT_ATTACHMENTS"] = [{"filename": "iqf.pdf", "_text": "Principal  Place of\nBusiness of Entity"}]
        self.assertEqual(_ns["entity_form_trigger"](), "attachment 'iqf.pdf': 'Principal Place of Business'")

    def test_none_for_ordinary_email(self):
        _ns["_CURRENT_EMAIL"] = {"subject": "Re: SpaceX allocation", "body": "Office: 1 Main St, Austin TX"}
        _ns["_CURRENT_ATTACHMENTS"] = []
        self.assertIsNone(_ns["entity_form_trigger"]())


class FakePipeline:
    """Company store behind call_pipeline_api. GET omits null-valued keys, like Pipeline."""
    def __init__(self, record, accepted, reject=()):
        self.record, self.accepted, self.reject, self.calls = dict(record), set(accepted), set(reject), []

    def __call__(self, method, endpoint, payload=None):
        self.calls.append((method, json.loads(json.dumps(payload)) if payload else None))
        if method == "GET":
            return {"status": 200, "data": {k: v for k, v in self.record.items() if v is not None}}
        bad = [k for k in payload["company"] if k in self.reject]
        if bad:
            return {"status": 422, "data": {"errors": [f"unknown attribute '{bad[0]}'"]}}
        for k, v in payload["company"].items():
            if k in self.accepted:
                self.record[k] = v
        return {"status": 200, "data": self.record}


class PutCompanyCheckedTest(unittest.TestCase):
    KEYS = ["address_1", "city", "postal_code", "country"]

    def run_put(self, fake):
        _ns["call_pipeline_api"] = fake
        payload = {"company": {"address_1": "2210 Peninsula Five, Business Bay", "city": "Dubai",
                               "postal_code": "00000", "country": "United Arab Emirates"}}
        return _ns["put_company_checked"](138490920, payload, self.KEYS)

    def test_key_missing_from_get_is_still_written(self):
        fake = FakePipeline({"address_1": "old", "city": "old", "postal_code": None, "country": "old"},
                            accepted=self.KEYS)
        result, unknown = self.run_put(fake)
        self.assertEqual(result["status"], 200)
        self.assertEqual(unknown, [])
        self.assertIn("postal_code", fake.calls[0][1]["company"])
        self.assertEqual(fake.record["postal_code"], "00000")

    def test_key_put_rejects_is_reported_and_rest_retried(self):
        fake = FakePipeline({}, accepted=["address_1", "city", "country"], reject=["postal_code"])
        result, unknown = self.run_put(fake)
        self.assertEqual(result["status"], 200)
        self.assertEqual(unknown, ["postal_code"])
        self.assertEqual(fake.record["city"], "Dubai")

    def test_key_silently_ignored_by_put_is_reported(self):
        fake = FakePipeline({}, accepted=["address_1", "city", "country"])
        _, unknown = self.run_put(fake)
        self.assertEqual(unknown, ["postal_code"])


if __name__ == "__main__":
    unittest.main()
