"""Unit tests for the public company-Summary guard (check_company_summary).

lambda_function.py reads S3 at import time, so the guard is pulled out with `ast`.
Run: python3 -m unittest discover tests
"""
import ast
import pathlib
import re
import unittest

_SRC = pathlib.Path(__file__).resolve().parent.parent / "lambda_function.py"
_tree = ast.parse(_SRC.read_text())
_nodes = [n for n in _tree.body
          if (isinstance(n, ast.FunctionDef) and n.name == "check_company_summary")
          or (isinstance(n, ast.Assign) and any(getattr(t, "id", None) in
                                                {"SUMMARY_RULES", "SUMMARY_MAX_WORDS", "_SUMMARY_BLOCKS"}
                                                for t in n.targets))]
_ns = {"re": re}
exec(compile(ast.Module(body=_nodes, type_ignores=[]), str(_SRC), "exec"), _ns)
check = _ns["check_company_summary"]

GOOD = ("Technology investment firm founded in 2020 by Abishek Surendran; started in Bengaluru, now "
        "headquartered in Dubai with a presence in the US and India. Says it invests in AI, space, defense, "
        "energy and frontier tech at the growth stage. PitchBook records 18 investments, including SpaceX and "
        "Mach Industries. (Sources: PitchBook, CB Insights, firm website)")


class CompanySummaryGuard(unittest.TestCase):
    def test_accepts_compliant_example(self):
        self.assertIsNone(check(GOOD))

    def test_empty_allowed(self):
        self.assertIsNone(check(""))
        self.assertIsNone(check(None))

    def test_rejects_marketing_first_person(self):
        err = check("We are a world-class investor and our team backs exceptional founders. (Sources: firm website)")
        self.assertIn("first person", err)
        self.assertIn("nothing was written", err)

    def test_rejects_contractions(self):
        self.assertIsNotNone(check("WE'VE backed 40 companies. (Sources: press)"))

    def test_first_person_is_case_sensitive(self):
        # "US" (country) and "IT" must not trip the first-person rule.
        self.assertIsNone(check("Venture firm with offices in the US. (Sources: PitchBook)"))

    def test_rejects_internal_tags(self):
        for bad in ("*** INACTIVE *** Family office in Miami. (Sources: PitchBook)",
                    "Family office; Rainmaker client since 2021. (Sources: PitchBook)",
                    "See https://app.pipelinecrm.com/companies/1. (Sources: PitchBook)",
                    "Family office. Chad met the CIO in 2023. (Sources: PitchBook)"):
            self.assertIsNotNone(check(bad), bad)

    def test_rejects_missing_sources(self):
        err = check(GOOD.split(" (Sources:")[0])
        self.assertIn("Sources", err)

    def test_rejects_too_long(self):
        err = check(("word " * 121) + "(Sources: PitchBook)")
        self.assertIn("max 120", err)


if __name__ == "__main__":
    unittest.main()
