import unittest

from curamentorworker.s3_utils import apply_s3_prefix


class ApplyS3PrefixTests(unittest.TestCase):
    def test_returns_key_when_prefix_missing(self):
        self.assertEqual(apply_s3_prefix("", "foo/bar.pdf"), "foo/bar.pdf")

    def test_applies_prefix_when_provided(self):
        self.assertEqual(apply_s3_prefix("uploads", "foo/bar.pdf"), "uploads/foo/bar.pdf")

    def test_does_not_double_prefix(self):
        self.assertEqual(apply_s3_prefix("uploads", "uploads/foo.pdf"), "uploads/foo.pdf")

    def test_handles_leading_slashes_consistently(self):
        self.assertEqual(apply_s3_prefix("uploads/", "/foo.pdf"), "uploads/foo.pdf")

    def test_returns_prefix_when_key_empty(self):
        self.assertEqual(apply_s3_prefix("uploads", ""), "uploads")
