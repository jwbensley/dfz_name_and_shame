import datetime
import logging
import os
import subprocess
import typing
import urllib.parse

from dnas.config import config as cfg

class git:
    """
    A class for commiting and pushing files to GitHub.
    """

    @staticmethod
    def add(filename: str = None):
        """
        Add files to the git index, to be commited.
        """
        if not filename:
            raise ValueError(
                f"Missing required arguments: filename={filename}."
            )

        if type(filename) != str:
            raise TypeError(
                f"filename is not a string: {type(filename)}"
            )

        ret = subprocess.run(
            ["git", "add", filename],
            cwd=cfg.GIT_BASE,
            capture_output=True,
        )
        if ret.returncode != 0:
            raise ChildProcessError(
                f"Couldn't add file {filename} to git index:\n"
                f"args: {ret.args}\n"
                f"stdout: {ret.stdout.decode()}\n"
                f"stderr: {ret.stderr.decode()}"
            )
        logging.debug(f"Added {filename} to git index")

    @staticmethod
    def clear():
        """
        Remove all files currently in the git index for commit.
        """
        ret = subprocess.run(
            ["git", "restore", "--staged", "*"],
            cwd=cfg.GIT_BASE,
            capture_output=True,
        )
        if ret.returncode != 0:
            raise ChildProcessError(
                f"Couldn't clear git index of existing staged entries:\n"
                f"args: {ret.args}\n"
                f"stdout: {ret.stdout.decode()}\n"
                f"stderr: {ret.stderr.decode()}"
            )
        logging.debug(f"Cleared git index in {cfg.GIT_BASE}")

    @staticmethod
    def clone():
        """
        Clone the DNS Stats repo.
        """
        os.makedirs(cfg.GIT_BASE, exist_ok = True)

        ret = subprocess.run(
            ["git", "clone", cfg.GIT_STAT_CLONE_URL],
            cwd=cfg.BASE_DIR,
            capture_output=True,
        )
        if ret.returncode != 0:
            raise ChildProcessError(
                f"Couldn't clone git repo {cfg.GIT_STAT_CLONE_URL} to directory "
                f"{cfg.BASE_DIR}:\n"
                f"args: {ret.args}\n"
                f"stdout: {ret.stdout.decode()}\n"
                f"stderr: {ret.stderr.decode()}"
            )
        logging.debug(
            f"Cloned git repo {cfg.GIT_STAT_CLONE_URL} to {cfg.BASE_DIR}"
        )

    @staticmethod        
    def commit(msg: str = None):
        """
        Commit staged changes to git with commit message "msg".
        """
        if not msg:
            raise ValueError(
                f"Missing required arguments: msg={msg}."
            )

        if type(msg) != str:
            raise TypeError(
                f"msg is not a string: {type(msg)}"
            )

        ret = subprocess.run(
            ["git", "commit", "-m", msg],
            cwd=cfg.GIT_BASE,
            capture_output=True,
        )
        if ret.returncode != 0:
            if "Your branch is up to date with" in ret.stdout.decode():
                raise ChildProcessError(
                    "Nothing committed, no changes with remote"
                )
            else:
                raise ChildProcessError(
                    f"Couldn't commit staged changes to git in {cfg.GIT_BASE}\n"
                    f"args: {ret.args}\n"
                    f"stdout: {ret.stdout.decode()}\n"
                    f"stderr: {ret.stderr.decode()}"
                )
        logging.debug(f"Committed to git in {cfg.GIT_BASE}: {msg}")

    @staticmethod
    def diff():
        """
        Return True if there are files in the git index, with uncommitted
        changes, else False.
        """
        ret = subprocess.run(
            ["git", "diff", "--cached"],
            cwd=cfg.GIT_BASE,
            capture_output=True,
        )
        if ret.returncode != 0:
            raise ChildProcessError(
                f"Couldn't check if git has staged changed:\n"
                f"args: {ret.args}\n"
                f"stdout: {ret.stdout.decode()}\n"
                f"stderr: {ret.stderr.decode()}"
            )
        if ret.stdout.decode() == "":
            logging.debug(f"No changes staged in git cache in {cfg.GIT_BASE}")
            return False
        else:
            logging.debug(
                f"Changes are staged git in cache in {cfg.GIT_BASE}")
            return True

    @staticmethod
    def git_exists():
        """
        Return True if DNAS Stats repo exists locally.
        """
        if not os.path.isdir(cfg.GIT_BASE):
            logging.debug(f"Local git directory doesn't exist: {cfg.GIT_BASE}")
            return False

        ret = subprocess.run(
            ["git", "status"],
            cwd=cfg.GIT_BASE,
            capture_output=True,
        )
        if ret.returncode != 0:
            logging.debug(f"No git repo detected in {cfg.GIT_BASE}")
            return False

        logging.debug(f"Git repo found in {cfg.GIT_BASE}")
        return True

    @staticmethod
    def gen_git_path_ymd(ymd: str = None) -> str:
        """
        Generate and return the path to the report files for a specific date.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}."
            )

        if type(ymd) != str:
            raise TypeError(
                f"ymd is not a string: {type(ymd)}"
            )

        day = datetime.datetime.strptime(ymd, cfg.DAY_FORMAT)

        git_dir = os.path.join(cfg.GIT_BASE, str(day.year))
        git_dir = os.path.join(git_dir, f"{day.month:02}")
        git_dir = os.path.join(git_dir, f"{day.day:02}")

        return git_dir

    @staticmethod
    def gen_git_url_ymd(ymd: str = None) -> str:
        """
        Generate and return the URL to the report files for a specific date.
        """
        if not ymd:
            raise ValueError(
                f"Missing required arguments: ymd={ymd}."
            )

        if type(ymd) != str:
            raise TypeError(
                f"ymd is not a string: {type(ymd)}"
            )

        day = datetime.datetime.strptime(ymd, cfg.DAY_FORMAT)

        git_url = os.path.join(str(day.year), f"{day.month:02}")
        git_url = os.path.join(git_url, f"{day.day:02}")
        git_url = urllib.parse.urljoin(cfg.GIT_STAT_BASE_URL, git_url)

        return git_url

    @staticmethod
    def pull():
        """
        Perform a git pull to make sure the local repo is up to date
        """
        ret = subprocess.run(
            ["git", "pull"],
            cwd=cfg.GIT_BASE,
            capture_output=True,
        )
        if ret.returncode != 0:
            raise ChildProcessError(
                f"Couldn't pull from remote branch "
                f"{cfg.GIT_REPORT_BRANCH}\n"
                f"args: {ret.args}\n"
                f"stdout: {ret.stdout.decode()}\n"
                f"stderr: {ret.stderr.decode()}"
            )
        logging.debug(
            f"Git pull succeeded"
        )

    @staticmethod
    def push():
        """
        Push commits to GitHub.
        """
        ret = subprocess.run(
            ["git", "push", "origin", cfg.GIT_REPORT_BRANCH],
            cwd=cfg.GIT_BASE,
            capture_output=True,
        )
        if ret.returncode != 0:
            raise ChildProcessError(
                f"Couldn't push commits to remote branch "
                f"{cfg.GIT_REPORT_BRANCH}\n"
                f"args: {ret.args}\n"
                f"stdout: {ret.stdout.decode()}\n"
                f"stderr: {ret.stderr.decode()}"
            )
        logging.debug(
            f"Pushed commit(s) to remote branch {cfg.GIT_REPORT_BRANCH}"
        )
