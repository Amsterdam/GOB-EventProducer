from unittest import TestCase

from gobeventproducer.naming import camel_case


class TestNaming(TestCase):

    def test_camel_case(self):
        cases = [
            ("test_case", "testCase"),
            ("test_case_2", "testCase2"),
            ("test", "test"),
        ]

        for _in, _out in cases:
            self.assertEqual(_out, camel_case(_in))
