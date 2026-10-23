"""Structure on deals created from forwarded web-bid offers/bids.

Email text below mirrors chadgracia/web-bid _send_bid_email. Run: python3 -m unittest discover tests
"""
import ast
import pathlib
import re
import unittest

_SRC = pathlib.Path(__file__).resolve().parent.parent / "lambda_function.py"
_WANTED = {"_parse_web_offer", "_offer_structure", "_offer_applies"}
_tree = ast.parse(_SRC.read_text())
_ns = {"re": re}
exec(compile(ast.Module(body=[n for n in _tree.body
                              if (isinstance(n, ast.FunctionDef) and n.name in _WANTED)
                              or (isinstance(n, ast.Assign) and any(getattr(t, "id", None) == "_WEB_OFFER_STRUCTURES" for t in n.targets))],
                        type_ignores=[]), str(_SRC), "exec"), _ns)
parse, structure, applies = _ns["_parse_web_offer"], _ns["_offer_structure"], _ns["_offer_applies"]


def offer_email(structure_txt="Direct", side="sell"):
    word = "offer" if side == "sell" else "bid"
    subject = f"Fwd: New web {word}: Kraken @ $30.0/sh"
    body = (
        "---------- Forwarded message ---------\n"
        "From: agent@agent.graciagroup.com\n\n"
        f"A firm {'sell offer' if side == 'sell' else 'buy bid'} was submitted through the web-bid page.\n\n"
        "Company:      Kraken (id 6012345)\n"
        "Bidder:       Logged-in user: jason@carbynecap.com\n"
        "Gross price:  $30.0/share\n"
        "Min size:     $2000000.0\n"
        "Max size:     $10000000.0\n"
        f"Structure:    {structure_txt}\n\n"
        "(No Pipeline deal was created — this is the email-only step.)\n"
        "\nJason Ben Yair\nCarbyne Capital Secondaries Fund\n"
    )
    return subject, body


class WebOfferStructureTest(unittest.TestCase):
    def test_fund_named_seller_with_direct_offer_gets_direct(self):
        offer = parse(*offer_email("Direct"))
        self.assertEqual((offer["side"], offer["company"], offer["company_id"]), ("sell", "Kraken", "6012345"))
        self.assertTrue(applies(offer, 6012345, "Kraken", "sell"))
        self.assertEqual(structure(offer), (["Direct"], "from offer form", None))

    def test_fund_ticked_on_form_is_kept(self):
        self.assertEqual(structure(parse(*offer_email("Fund"))), (["Fund"], "from offer form", None))
        self.assertEqual(structure(parse(*offer_email("Direct, Forward"))), (["Direct", "Forward"], "from offer form", None))

    def test_not_stated_defaults_to_direct_with_notice(self):
        vals, source, notice = structure(parse(*offer_email("(none)")))
        self.assertEqual((vals, source), (["Direct"], "default, not stated"))
        self.assertEqual(notice, "Structure not stated, defaulted to Direct, please confirm.")
        subject, body = offer_email()
        body = "\n".join(l for l in body.splitlines() if not l.startswith("Structure:"))
        self.assertEqual(structure(parse(subject, body))[0], ["Direct"])

    def test_direct_and_fund_on_sell_offer_defaults_to_direct(self):
        vals, _, notice = structure(parse(*offer_email("Direct, Fund")))
        self.assertEqual(vals, ["Direct"])
        self.assertIn("please confirm", notice)

    def test_quoted_forward_still_parses(self):
        subject, body = offer_email("Direct")
        quoted = "\n".join("> " + l for l in body.splitlines())
        self.assertEqual(parse("Re: something", quoted)["structure"], ["Direct"])

    def test_other_deals_and_emails_untouched(self):
        offer = parse(*offer_email("Direct"))
        self.assertFalse(applies(offer, 999, "SpaceX", "sell"))
        self.assertFalse(applies(offer, 6012345, "Kraken", "buy"))
        self.assertIsNone(parse("Kraken interest", "We'd sell $2M of Kraken via our fund."))


if __name__ == "__main__":
    unittest.main()
