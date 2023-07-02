#!/usr/bin/env bash

# Error if undefined variables are present:
set -u

# Error if any command in a pipe has a non-zero exit status, not just the last command:
set -o pipefail

# Error if any command returns a non-zero exist status
set -e

pypy="/opt/pypy"

# Generate the daily stats for yesterday:
${pypy} /opt/dnas/scripts/stats.py --yesterday --update --enabled

# Generate the text report from the generated stats:
${pypy} /opt/dnas/scripts/git_reports.py --yesterday

# Tweet the report summary and link to full report:
${pypy} /opt/dnas/scripts/tweet.py --yesterday

