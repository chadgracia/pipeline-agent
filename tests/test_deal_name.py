"""Unit tests for the deal-name formatter.

lambda_function.py reads S3 at import time, so the pure helpers are pulled out with `ast`
and exec'd in isolation. Run: python3 -m unittest discover tests
"""
import ast
import pathlib
import unittest

_SRC = pathlib.Path(__file__).resolve().parent.parent / "lambda_function.py"
_WANTED = {"_to_amount", "format_deal_amount", "build_deal_name"}

_tree = ast.parse(_SRC.read_text())
_ns = {}
exec(compile(ast.Module(body=[n for n in _tree.body if isinstance(n, ast.FunctionDef) and n.name in _WANTED],
                        type_ignores=[]), str(_SRC), "exec"), _ns)
format_deal_amount = _ns["format_deal_amount"]
build_deal_name = _ns["build_deal_name"]


class FormatDealAmountTest(unittest.TestCase):
    def test_amounts(self):
        cases = {250_000: "$250K", 500_000: "$500K", 1_000_000: "$1M", 1_500_000: "$1.5M",
                 5_000_000: "$5M", 12_500_000: "$12.5M", 999_999: "$1M"}
        for amount, expected in cases.items():
            self.assertEqual(format_deal_amount(amount), expected, amount)


class BuildDealNameTest(unittest.TestCase):
    def test_range_k_to_m(self):
        self.assertEqual(build_deal_name("Applied Intuition", 250_000, 5_000_000, "sell"),
                         "Applied Intuition: $250K - $5M Sell")

    def test_range_decimal_millions(self):
        self.assertEqual(build_deal_name("SpaceX", 1_500_000, 12_500_000, "buy"),
                         "SpaceX: $1.5M - $12.5M Buy")

    def test_min_missing(self):
        self.assertEqual(build_deal_name("Applied Intuition", None, 5_000_000, "sell"),
                         "Applied Intuition: $5M Sell")
        self.assertEqual(build_deal_name("Applied Intuition", 0, 5_000_000, "sell"),
                         "Applied Intuition: $5M Sell")

    def test_min_equals_max(self):
        self.assertEqual(build_deal_name("Anthropic", 100_000, 100_000, "buy"),
                         "Anthropic: $100K Buy")

    def test_max_missing(self):
        self.assertEqual(build_deal_name("Anthropic", 500_000, None, "buy"),
                         "Anthropic: $500K+ Buy")

    def test_no_size(self):
        self.assertEqual(build_deal_name("Together AI", None, None, "sell"), "Together AI: Sell")

    def test_string_inputs_with_commas(self):
        self.assertEqual(build_deal_name("Stripe", "250,000", "5,000,000", "Sell"),
                         "Stripe: $250K - $5M Sell")


if __name__ == "__main__":
    unittest.main()
