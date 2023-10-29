import json
import typing


class twitter_msg:
    """
    This object contains a twitter message, to be tweeted.
    """

    def __init__(
        self: "twitter_msg",
        hdr: str = "",
        hdr_id: int = 0,
        body: str = "",
        body_ids: list[int] = [],
        hidden: bool = True,
    ) -> None:
        """
        The header contains the main message to be tweeted.
        It must be is <= cfg.TWITTER_LEN
        This field is required.
        """
        self.hdr = hdr

        """
        The Tweet ID of the header message.
        """
        self.hdr_id = hdr_id

        """
        The body is an optional field, which contains any subsequent info to be
        tweeted. This will be split across multiple replies to the header tweet.
        """
        self.body = body

        """
        List of Tweet IDs which are the pages replies to the header Tweet.
        """
        self.body_ids = body_ids

        """
        Only tweet the message if False.
        """
        self.hidden = hidden

    def from_json(self: "twitter_msg", json_str: str) -> None:
        """
        Populate this object with data from a JSON string.
        """
        if not json_str:
            raise ValueError(
                f"Missing required arguments: json_str={json_str}"
            )

        if type(json_str) != str:
            raise TypeError(f"json_str is not a string: {type(json_str)}")

        json_data = json.loads(json_str)
        self.hdr = json_data["hdr"]
        self.hdr_id = json_data["hdr_id"]
        self.body = json_data["body"]
        self.body_ids = json_data["body_ids"]
        self.hidden = json_data["hidden"]

    @staticmethod
    def gen_tweeted_q_key(ymd: str) -> str:
        """
        Return the redis key for the tweeted queue, for a specific day.
        """
        if not ymd:
            raise ValueError(f"Missing required arguments: ymd={ymd}")

        if type(ymd) != str:
            raise TypeError(f"ymd is not an str: {type(ymd)}")

        return "TWEETED:" + ymd

    @staticmethod
    def gen_tweet_q_key(ymd: str) -> str:
        """
        Return the redis key for the tweet queue, for a days tweets.
        """
        if not ymd:
            raise ValueError(f"Missing required arguments: ymd={ymd}")

        if type(ymd) != str:
            raise TypeError(f"ymd is not an str: {type(ymd)}")

        return "TWEET_Q:" + ymd

    def to_json(self: "twitter_msg") -> str:
        """
        Return the twitter message serialised as a json string.
        """
        json_data = {
            "hdr": self.hdr,
            "hdr_id": self.hdr_id,
            "body": self.body,
            "body_ids": self.body_ids,
            "hidden": self.hidden,
        }
        return json.dumps(json_data)
