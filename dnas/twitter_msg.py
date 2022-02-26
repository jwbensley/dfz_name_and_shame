import json

class twitter_msg:
    """
    This object contains a twitter message, to be tweeted.
    """

    """
    The header contains the main message to be tweeted.
    It must be is <= cfg.TWITTER_LEN
    This field is required.
    """
    hdr = ""

    """
    The Tweet ID of the header message.
    """
    hdr_id = None

    """
    The body is an optional field, which contains any subsequent info to be
    tweeted. This will be split across multiple replies to the header tweet.
    """
    body = ""

    """
    List of Tweet IDs which are the pages replies to the header Tweet.
    """
    body_ids = []

    """
    Only tweet the message if False.
    """
    hidden = True

    def from_json(self, json_str):
        """
        Populate this object with data from a JSON string.
        """
        if not json_str:
            raise ValueError(
                f"Missing required arguments: json_str={json_str}"
            )

        if type(json_str) != str:
            raise TypeError(
                f"json_str is not a string: {type(json_str)}"
            )

        json_data = json.loads(json_str)
        self.hdr = json_data["hdr"]
        self.body = json_data["body"]
        self.hidden = json_data["hidden"]

    @staticmethod
    def gen_tweeted_q_key(ymd):
        """
        Return the redis key for the tweeted queue, for a specific day.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}"
            )

        if type(ymd) != str:
            raise TypeError(
                f"ymd is not an str: {type(ymd)}"
            )

        return "TWEETED:" + ymd

    @staticmethod
    def gen_tweet_q_key(ymd):
        """
        Return the redis key for the tweet queue, for a days tweets.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}"
            )

        if type(ymd) != str:
            raise TypeError(
                f"ymd is not an str: {type(ymd)}"
            )

        return "TWEET_Q:" + ymd

    def to_json(self):
        """
        Return the twitter message serialised as a json string.
        """
        json_data = {
            "hdr": self.hdr,
            "body": self.body,
            "hidden": self.hidden,
        }
        return json.dumps(json_data)
