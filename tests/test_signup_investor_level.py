"""New web signups start on Hold (7161646), never Substantive (7162165).
Run: python3 -m unittest discover tests"""
import pathlib
import re
import unittest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SRC = (_ROOT / "lambda_function.py").read_text()
_PROMPT = (_ROOT / "agent-system-prompt.txt").read_text()


class SignupInvestorLevel(unittest.TestCase):
    def test_sns_signup_block_uses_hold(self):
        i = _SRC.index("AWS SIGNUP AUTOMATION")
        block = _SRC[i:_SRC.index("Return the Pipeline URL", i)]
        self.assertIn("Investor Level=Hold: Screen for Substantive (7161646)", block)
        self.assertNotIn("7162165", block)

    def test_schema_default_is_hold_for_new_signups(self):
        m = re.search(r'"custom_label_3923758": \{"type": "integer", "description": "([^"]*)"', _SRC)
        self.assertIsNotNone(m)
        desc = m.group(1)
        self.assertTrue(desc.startswith("Investor Level: 7161646=Hold: Screen for Substantive (default for NEW web signups"))
        self.assertNotIn("7162165=Substantive (default)", desc)

    def test_existing_person_path_never_sets_level(self):
        i = _SRC.index("Existing lead found:")
        block = _SRC[_SRC.rindex("if ", 0, i) - 2000:i]
        self.assertNotIn("custom_label_3923758", block)

    def test_system_prompt_signup_exception(self):
        self.assertIn("AWS SIGNUP AUTOMATION (new web signups at trades.graciagroup.com) ALWAYS uses "
                      "Hold: Screen for Substantive (7161646)", _PROMPT)


if __name__ == "__main__":
    unittest.main()
