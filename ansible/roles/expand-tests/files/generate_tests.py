#!/usr/bin/env python3

import argparse
from argparse import Namespace
from dataclasses import dataclass, field, asdict
import itertools
import logging
import sys
import yaml


@dataclass
class TestCase:
    testcase: str
    values: dict[str, str]
    name: str = field(init=False)

    def __post_init__(self):
        self.name = "_".join(
            "-".join([x, self.values.get(x)]) for x in self.values.keys()
        )


def check_args() -> Namespace:
    parser = argparse.ArgumentParser(
        description="This tool is used by the Stackable integration tests to create the final matrix of tests to run "
        "based on test dimensions defined for the test suite it is running in."
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="The input file (yaml format) which specifies the dimensions and their values",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="A yaml file to be read by Ansible which contains the test cases",
    )
    parser.add_argument(
        "--debug",
        "-d",
        action="store_true",
        required=False,
        help="Will print additional debug statements (e.g. output from all run commands)",
    )
    args = parser.parse_args()

    log_level = "DEBUG" if args.debug else "INFO"
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )
    return args


def main() -> int:
    args = check_args()
    result = []
    with open(args.input, encoding="utf8") as stream:
        input_dimensions = yaml.safe_load(stream)

        for test_case in input_dimensions["tests"]:
            dimensions = test_case["dimensions"]
            used_dimensions = [
                v for v in input_dimensions["dimensions"] if v["name"] in dimensions
            ]
            tmp = []
            for value in used_dimensions:
                tmp.append([(value["name"], x) for x in value["values"]])

            for materialized_case in itertools.product(*tmp):
                result.append(
                    TestCase(testcase=test_case["name"], values=dict(materialized_case))
                )

        with open(args.output, "w", encoding="utf8") as outstream:
            outputstruct = {"tests": [asdict(r) for r in result]}
            logging.debug(f"Got the following output: {outputstruct}")
            yaml.dump(outputstruct, outstream)


if __name__ == "__main__":
    sys.exit(main())
