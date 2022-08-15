#!/usr/bin/env bash
set -euo pipefail

# Reads a file with variables to insert into templates, and templates all .*.j2 files
# in the 'docs' directory.
#
# dependencies
# pip install jinja2-cli

docs_dir=../docs
templating_vars_file=$docs_dir/templating_vars.yaml

# Check if files need templating
if [[ $(find "$docs_dir" | grep --count .j2\$) -eq "0" ]];
then
  echo "No files need templating, exiting."
  exit
fi

# Check if jinja2 is there
if ! command -v jinja2 &> /dev/null
then
  echo "jinja2 could not be found. Use 'pip install jinja2-cli' to install it."
  exit
fi

# Check if templating vars file exists
if [[ ! -f "$templating_vars_file" ]];
then
  echo "$templating_vars_file does not exist, cannot start templating."
fi

for file in $(find "$docs_dir" | grep .j2\$)
do
  new_file_name=$(echo "$file" | sed 's/\(.*\).j2/\1/g')  # cut of the '.j2'
  echo "templating $new_file_name"
  jinja2 "$file" "$templating_vars_file" -o "$new_file_name"
done

echo "done"
