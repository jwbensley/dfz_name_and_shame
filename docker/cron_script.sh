#!/usr/bin/env bash

# Error if undefined variables are present:
set -u

# Error if any command in a pipe has a non-zero exit status, not just the last command:
set -o pipefail

# Error if any command returns a non-zero exist status
set -e

PYPY="/opt/pypy"
SCRIPTS="/opt/dnas/dnas/scripts"

# Generate the daily stats for yesterday:
${PYPY} ${SCRIPTS}/stats.py --yesterday --update --enabled

# Generate the text report from the generated stats:
${PYPY} ${SCRIPTS}/git_reports.py --yesterday

# Tweet the report summary and link to full report:
${PYPY} ${SCRIPTS}/tweet.py --yesterday

