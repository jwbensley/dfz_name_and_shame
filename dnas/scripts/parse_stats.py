#!/usr/bin/env python3

import argparse
import json
import logging
import os
import plotly.graph_objects as go
import plotly.offline as po
import sys
from typing import Any

# Accommodate the use of the dnas library, even when the library isn't installed
sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)


from dnas.config import config as cfg
from dnas.log import log


class YearStats:
    stats: dict[int, dict[Any, Any]]
    stat_names = [
        "longest_as_path",
        "longest_comm_set",
        "most_advt_prefixes",
        "most_upd_prefixes",
        "most_withd_prefixes",
        "most_advt_origin_asn",
        "most_advt_peer_asn",
        "most_upd_peer_asn",
        "most_withd_peer_asn",
        "most_origin_asns",
        "most_unknown_attrs",
        "most_unreg_origins",
    ]

    def __init__(self, stats: dict[int, dict[Any, Any]]) -> None:
        self.stats = stats

    def get_stat_names(self) -> list[str]:
        return self.stat_names

    def get_stat_per_day_entire(self, stat_name: str) -> dict[str, int]:
        """
        Return the per-day values for the given stat name, for all years
        """
        result: dict[str, int] = {}

        if not stat_name.endswith("_per_day"):
            logging.debug(f"Appending '_per_day' to stat name '{stat_name}'")
            stat_name += "_per_day"

        for year in self.get_years():
            for day, value in self.stats[year][stat_name].items():
                result[f"{year}_{day}"] = int(value)
        return result

    def get_years(self) -> list[int]:
        return list(self.stats.keys())


def plot_stats(year_stats: YearStats) -> None:

    os.makedirs(cfg.STATS_PLOT_BASE, exist_ok=True)

    for stat_name in year_stats.get_stat_names():
        data = year_stats.get_stat_per_day_entire(stat_name)
        output_file = os.path.join(cfg.STATS_PLOT_BASE, f"{stat_name}.html")

        fig = go.Figure(
            data=[
                go.Bar(
                    y=list(data.values()),
                    x=list(data.keys()),
                    marker={"color": "cornflowerblue"},
                    customdata=[
                        f"{reason}: {count}" for reason, count in data.items()
                    ],
                    hovertemplate="%{customdata}",
                )
            ],
            layout={"yaxis": {"title": "Count"}},
        )
        fig.update_layout(
            barmode="group",
            title_text=(stat_name.replace("_", " ").title()),
            title_x=0.5,
            xaxis_title_text="Date",
            # yaxis_range=[min(data.v4) - int(min(data.v4) * 0.1), max(data.v4)],
            legend=dict(yanchor="top", xanchor="left", x=0.01, y=1.05),
            margin=dict(l=0, r=0, b=0, t=100, pad=0),
        )
        po.plot(
            fig,
            image_width=1920,
            image_height=1080,
            filename=output_file,
            auto_open=False,
        )
        logging.info(f"Written to {output_file}")


def load_stats_files(filenames: list[str]) -> YearStats:
    logging.info(f"Loading stats files: {filenames}")
    stats = YearStats({})

    for filename in filenames:
        year = int(os.path.basename(filename).split(".")[0])
        logging.debug(f"Loading stats for year {year} from file {filename}")
        with open(filename, "r") as f:
            stats.stats[year] = json.load(f)
    logging.debug(f"Loaded stats for years: {stats.get_years()}")
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse stats extracted from DNAS DB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--debug",
        help="Run with debug level logging.",
        default=False,
        action="store_true",
        required=False,
    )

    input = parser.add_argument_group("Input Options")
    input.add_argument(
        "--stats-files",
        help=f"BASH glob pattern to match stats files to parse, e.g., "
        f"{cfg.YEAR_STATS_BASE}*.json",
        type=str,
        required=True,
        nargs="*",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    log.setup(
        debug=args.debug,
        log_src="StatsParser script",
        log_path=cfg.LOG_STATS_PARSER,
    )

    year_stats = load_stats_files(args.stats_files)
    plot_stats(year_stats)


if __name__ == "__main__":
    main()
