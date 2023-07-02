#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import sys
import typing

# Accomodate the use of the dnas library, even when the library isn't installed
sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)

from dnas.config import config as cfg
from dnas.log import log
from dnas.git import git
from dnas.mrt_stats import mrt_stats
from dnas.redis_db import redis_db
from dnas.report import report

def check_git():
    """
    Check if the git repo exists locally, if not, clone it.
    If it already exists, make a pull to ensure it's up to date.
    """
    if not git.git_exists():
        git.clone()
    else:
        git.clear()
        git.pull()

def generate(ymd: str = None):
    """
    Generate the stats files for a specific day.
    """
    if not ymd:
        raise ValueError(
            f"Missing required arguments: ymd={ymd}."
        )

    if type(ymd) != str:
        raise TypeError(
            f"ymd is not a string: {type(ymd)}"
        )

    git_dir = git.gen_git_path_ymd(ymd)
    os.makedirs(git_dir, exist_ok=True)

    rdb = redis_db()
    day_key = mrt_stats.gen_daily_key(ymd)
    day_stats = rdb.get_stats(day_key)
    if not day_stats:
        logging.info(f"No stats stored in DB for day {ymd}")
        rdb.close()
        return

    txt_filename = os.path.join(git_dir, report.gen_txt_report_fn_ymd(ymd))
    txt_report = report.gen_txt_report(day_stats)
    with open(txt_filename, "w", encoding="utf-8") as f:
        f.writelines(txt_report)
    logging.info(f"Wrote text report to {txt_filename}")

    rdb.close()

def generate_range(end: str = "", start: str = ""):
    """
    A wrapper around the generate function to generate report files for a
    range of days from start to end inclusive.
    """
    if (not end and not start):
        raise ValueError(
            f"Missing required arguments: end={end}, start={start}"
        )

    if (type(end) != str and type(start) != str):
        raise TypeError(
            f"Both end and start must be strings, not: {type(end)} and "
            f"{type(start)}"
        )

    start_day = datetime.datetime.strptime(start, cfg.DAY_FORMAT)
    end_day = datetime.datetime.strptime(end, cfg.DAY_FORMAT)

    if end_day < start_day:
        raise ValueError(
            f"End date {end_day} is before start date {start_day}!"
        )

    diff = end_day - start_day
    logging.info(
        f"Generating reports for {diff.days + 1} days from {start_day} to "
        f"{end_day}"
    )

    total = (diff.days + 1)
    for i in range(0, total):
        delta = datetime.timedelta(days=i)
        ymd = datetime.datetime.strftime(start_day + delta, cfg.DAY_FORMAT)
        generate(ymd)
        logging.info(f"Done {i+1}/{total}")

def parse_args():
    """
    Parse the CLI args to this script.
    """
    parser = argparse.ArgumentParser(
        description="Generate and publish reports to git based on the stats "
        "in redis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--debug",
        help="Enable debug logging for this script.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--end",
        help="End date in format 'yyyymmdd' e.g., '20220102'.",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--generate",
        help="Use with --range or --ymd to generate stats reports.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--publish",
        help="Use with --range or --ymd to push generated reports to GitHub.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--range",
        help="Generate a range of reports from --start to --end inclusive. "
        "Use with --generate instead of --ymd. "
        "Reqires --start and --end.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--start",
        help="Start date in format 'yyyymmdd' e.g., '20220101'.",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--yesterday",
        help="Generate stats for yesterday AND commit and publish to git. "
        "A shortcut for: --generate --ymd yyyymmdd --publish.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--ymd",
        help="Day in format 'yyyymmdd' e.g., '20220101'.",
        type=str,
        default=None,
        required=False,
    )
    return vars(parser.parse_args())

def publish(ymd: str = None):
    """
    Commit and push the report files for a specific day to GitHub.
    """
    if not ymd:
        raise ValueError(
            f"Missing required arguments: ymd={ymd}."
        )

    if type(ymd) != str:
        raise TypeError(
            f"ymd is not a string: {type(ymd)}"
        )

    git_dir = git.gen_git_path_ymd(ymd)
    txt_filename = os.path.join(git_dir, report.gen_txt_report_fn_ymd(ymd))

    #git.clear()
    git.add(txt_filename)
    if git.diff():
        git.commit(f"Adding report(s) for {ymd}")
        git.push()
        logging.info(
            f"Changes commited and pushed to git for {ymd}: "
            f"{git.gen_git_url_ymd(ymd)}"
        )
    else:
        logging.info(f"No changes to commit to git for {ymd}.")

def publish_range(end: str = "", start: str = ""):
    """
    A wrapper around the publish function to publish report files for a
    range of days from start to end inclusive.
    """
    if (not end and not start):
        raise ValueError(
            f"Missing required arguments: end={end}, start={start}"
        )

    if (type(end) != str and type(start) != str):
        raise TypeError(
            f"Both end and start must be strings, not: {type(end)} and "
            f"{type(start)}"
        )

    start_day = datetime.datetime.strptime(start, cfg.DAY_FORMAT)
    end_day = datetime.datetime.strptime(end, cfg.DAY_FORMAT)

    if end_day < start_day:
        raise ValueError(
            f"End date {end_day} is before start date {start_day}!"
        )

    diff = end_day - start_day
    logging.info(
        f"Publishing reports for {diff.days + 1} days from {start_day} to "
        f"{end_day}"
    )

    total = diff.days + 1
    for i in range(0, total):
        delta = datetime.timedelta(days=i)
        ymd = datetime.datetime.strftime(start_day + delta, cfg.DAY_FORMAT)
        publish(ymd)
        logging.info(f"Done {i+1}/{total}")

def yesterday():
    """
    A wrapped function to generate the report and publish for yesterday.
    """
    delta = datetime.timedelta(days=1)
    yesterday = datetime.datetime.strftime(
        datetime.datetime.now() - delta, cfg.DAY_FORMAT
    )
    generate(yesterday)
    publish(yesterday)

def main():

    args = parse_args()
    log.setup(
        debug = args["debug"],
        log_src = "report generation and posting script",
        log_path = cfg.LOG_GIT,
    )

    # Ensure the git repo exists, clone if it doesn't:
    check_git()

    if args["generate"]:
        if args["ymd"]:
            generate(args["ymd"])
        elif args["range"]:
            generate_range(args["end"], args["start"])
        else:
            raise ValueError(
                "--range or --ymd must be used with --generate!"
            )

    if args["publish"]:
        if args["ymd"]:
            publish(args["ymd"])
        elif args["range"]:
            publish_range(args["end"], args["start"])
        else:
            raise ValueError(
                "--range or --ymd must be used with --publish!"
            )

    if args["yesterday"]:
        yesterday()

if __name__ == '__main__':
    main()
