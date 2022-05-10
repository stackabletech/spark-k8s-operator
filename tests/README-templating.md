# Test Scenario Templating

## Introduction

The tests in this directory are designed to be expanded into multiple test scenarios based on test dimensions that can be defined in a dimensions file.

## Defining Test Dimensions

The dimensions file currently has to be named `test-definition.yaml` and reside in the same directory as the `kuttl-test.yaml.jinja2` file.

An example of a minimal folder structure will be given further down in this file.

An example of the content for the test definition file is shown here:

````yaml
dimensions:
  - name: spark
    values:
      - 3.2.1
      - 3.2.2
      - 3.2.3
  - name: hadoop
    values:
      - 3.1.0
      - 3.2.0
  - name: aws
      - abc
      - xyz
tests:
  - name: spark-pi-public-s3
    dimensions:
      - spark
      - hadoop
````

This file defines three dimensions for this test to be considered.
It also defines one test case named _spark-pi-public-s3_ and the dimensions that this test case should use.
In this example the test case uses only two of the three dimensions defined, so a run of this test case would be expanded into the following test structure:

````text
└── spark-pi-public-s3
    ├── spark-3.2.1_hadoop-3.1.0
    ├── spark-3.2.1_hadoop-3.2.0
    ├── spark-3.2.2_hadoop-3.1.0
    ├── spark-3.2.2_hadoop-3.2.0
    ├── spark-3.2.3_hadoop-3.1.0
    └── spark-3.2.3_hadoop-3.2.0
````

The name of a test case defined under `tests` in this file has to refer back to a directory in the `templates/kuttl` directory, which will be used to create the test scenarios.

Given the example of a test-definition.yaml shown above, the following folder structure would create the test scenarios shown above.

````text
tests
├── kuttl-test.yaml.j2
├── templates
│         └── kuttl
│             └── spark-pi-public-s3
└── test-definition.yaml
````

The `kuttl-test.yaml.jinja2` cannot currently be edited, as it comes from the operator templating and any changes would be overwritten again.
This should be fairly easy to solve and we can look at this as soon as it becomes necessary.

## Using

### Requirements

To run tests locally you need the following things installed:

- python3 (version >= 3.9)
  - pyyaml library installed
- ansible

### Running

To run tests please execute the following command from the gitroot of the operator repository:

`scripts/run_tests.sh`

This will install the necessary ansible role into `tests/ansible/roles`, expand the test templates into all defined test scenarios and execute kuttl to test these scenarios.
