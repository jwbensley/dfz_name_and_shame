#!/usr/bin/env bash

# Error if undefined variables are present:
set -u

# Error if any command in a pipe has a non-zero exit status, not just the last command:
set -o pipefail

# Error if any command returns a non-zero exist status
set -e

PYPY="/opt/pypy"
SCRIPTS="/opt/dnas/dnas/scripts"

# Ensure the latest AS stat allocations have been downloaded:
${PYPY} ${SCRIPTS}/update_asn_allocations.py

# Ensure no MRTs are missing for yesterday:
${PYPY} ${SCRIPTS}/get_mrts.py --yesterday --update --enabled --backfill

# Ensure no stats are missing for yesterday:
${PYPY} ${SCRIPTS}/parse_mrts.py --yesterday --update --enabled --remove

# Generate the daily stats for yesterday:
${PYPY} ${SCRIPTS}/stats.py --yesterday --update --enabled

# Generate the text report from the generated stats:
${PYPY} ${SCRIPTS}/git_reports.py --yesterday

# Tweet the report summary and link to full report:
${PYPY} ${SCRIPTS}/tweet.py --yesterday

