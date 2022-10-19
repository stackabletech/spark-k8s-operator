"""
Given the location of a file, trims all trailing blank lines and
places a single one. Used as post-processing step for README rendering.
"""
import re
import unittest

BLANK_LINE_REGEX_PATTERN = r"^\s*$"


def has_trailing_newline(line):
    return line[-1:] == "\n"


def process_lines(lines):
    trim_count = 0
    # trim trailing blank lines
    for line in lines[::-1]:
        if re.match(BLANK_LINE_REGEX_PATTERN, line):
            trim_count += 1
        else:
            break

    cutoff_index = len(lines) - trim_count
    new_lines = lines[:cutoff_index]

    # maybe add a newline character to the last sensible line
    if not has_trailing_newline(new_lines[-1]):
        new_lines[-1] = new_lines[-1] + "\n"

    # add a trailing blank line without newline
    new_lines.append("")
    return new_lines


class TestCoreMethods(unittest.TestCase):

    def test_trailing_new_line(self):
        self.assertTrue(has_trailing_newline("something\n"))
        self.assertTrue(has_trailing_newline("\n"))
        self.assertFalse(has_trailing_newline("nope"))

    def test_trailing_real_line(self):
        lines = ["bla\n", "useful"]
        processed_lines = process_lines(lines)
        self.assertEqual(len(processed_lines), 3)
        self.assertTrue(has_trailing_newline(processed_lines[0]))
        self.assertTrue(has_trailing_newline(processed_lines[1]))
        self.assertFalse(has_trailing_newline(processed_lines[2]))

    def test_lots_of_empties(self):
        lines = ["bla\n", "\n", "\n", "\n", "\n"]
        processed_lines = process_lines(lines)
        self.assertEqual(len(processed_lines), 2)
        self.assertEqual(processed_lines[-1], "")

    def test_one_trailing_new_line(self):
        lines = ["bla\n", "\n"]
        processed_lines = process_lines(lines)
        self.assertEqual(len(processed_lines), 2)
        self.assertEqual(processed_lines[-1], "")

    def test_one_trailing_blank_line(self):
        lines = ["bla\n", ""]
        processed_lines = process_lines(lines)
        self.assertEqual(len(processed_lines), 2)
        self.assertEqual(processed_lines[-1], "")


if __name__ == "__main__":
    # to run tests for this script:
    # python3 -m unittest ensure_one_trailing_newline.py

    import sys

    if len(sys.argv) != 2:
        print("Usage: {} filename_to_trim".format(sys.argv[0]))
        exit(1)

    file_name = sys.argv[1]

    lines = []
    with open(file_name, "r") as f:
        lines = f.readlines()

    lines = process_lines(lines)

    with open(file_name, "w") as f:
        f.write("".join(lines))
