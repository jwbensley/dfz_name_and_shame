import os
import pathlib
import re
import shutil
import subprocess
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)
from dnas.config import config
from dnas.git import git

class test_git(unittest.TestCase):

    cfg = config()
    g = git()
    test_filename = "abc123"

    def setUp(self):
        """
        Ensure a fresh local copy of the git repo is available
        """
        try:
            shutil.rmtree(self.cfg.GIT_BASE)
        except FileNotFoundError:
            pass
        os.makedirs(self.cfg.GIT_BASE, exist_ok = False)
        self.g.clone()

    def test_add(self):
        self.assertRaises(ValueError, self.g.add, None)
        self.assertRaises(TypeError, self.g.add, 123)

        # Ensure the test file isn't already in the git index
        self.g.clear()
        try:
            os.remove(os.path.join(self.cfg.GIT_BASE, self.test_filename))
        except FileNotFoundError:
            pass

        # Ensure non-existing file can't be staged
        self.assertRaises(ChildProcessError, self.g.add, self.test_filename)

        # Create the test file and stage it to the git index
        p = pathlib.Path(os.path.join(self.cfg.GIT_BASE, self.test_filename))
        p.touch()
        asserted = False
        try:
            self.g.add(self.test_filename)
        except:
            asserted = True
        self.assertEqual(asserted, False)

        # Check it is a new file in the git status:
        ret = subprocess.run(
            ["git", "status"],
            cwd=self.cfg.GIT_BASE,
            capture_output=True,
        )

        if ret.returncode != 0:
            raise ChildProcessError(
                f"Couldn't check git index status:\n"
                f"args: {ret.args}\n"
                f"stdout: {ret.stdout.decode()}\n"
                f"stderr: {ret.stderr.decode()}"
            )

        self.assertTrue(re.search(f"new file:.*{self.test_filename}", ret.stdout.decode()))

        os.remove(os.path.join(self.cfg.GIT_BASE, self.test_filename))
        self.g.clear()

    def test_clear(self):
        """
        Stage, then de-stage a test file, and then check the git stage is empty
        """

        # Create the test file and stage it to the git index
        try:
            os.remove(os.path.join(self.cfg.GIT_BASE, self.test_filename))
        except FileNotFoundError:
            pass
        p = pathlib.Path(os.path.join(self.cfg.GIT_BASE, self.test_filename))
        p.touch()
        self.g.add(self.test_filename)

        asserted = False
        try:
            self.g.clear()
        except:
            asserted = True
        self.assertEqual(asserted, False)

        ret = subprocess.run(
            ["git", "status"],
            cwd=self.cfg.GIT_BASE,
            capture_output=True,
        )

        if ret.returncode != 0:
            raise ChildProcessError(
                f"Couldn't check git index status:\n"
                f"args: {ret.args}\n"
                f"stdout: {ret.stdout.decode()}\n"
                f"stderr: {ret.stderr.decode()}"
            )

        self.assertTrue(re.search(f"nothing added to commit", ret.stdout.decode()))
        try:
            os.remove(os.path.join(self.cfg.GIT_BASE, self.test_filename))
        except FileNotFoundError:
            pass

    def test_clone(self):
        """
        Git clone should fail if there is an existing directory, that contains a
        different repo. Create the base direcory with an empty .git sub-dir
        (which is invalid for git)
        """
        try:
            shutil.rmtree(self.cfg.GIT_BASE)
        except FileNotFoundError:
            pass
        os.makedirs(self.cfg.GIT_BASE, exist_ok = False)
        os.makedirs(os.path.join(self.cfg.GIT_BASE, ".git"), exist_ok = False)
        self.assertRaises(ChildProcessError, self.g.clone)

        # Git clone should work if there is no existing .git sub-directory
        try:
            shutil.rmtree(self.cfg.GIT_BASE)
        except FileNotFoundError:
            pass

        asserted = False
        try:
            self.g.clone()
        except:
            asserted = True
        self.assertEqual(asserted, False)

    def test_commit(self):
        self.assertRaises(ValueError, self.g.commit, None)
        self.assertRaises(TypeError, self.g.commit, 123)

        # Ensure there are no staged changed to commit
        self.g.clear()

        # If there are no changes to commit, an exception is raised
        self.assertRaises(ChildProcessError, self.g.commit, "unittest")

        # Create the test file
        p = pathlib.Path(os.path.join(self.cfg.GIT_BASE, self.test_filename))
        p.touch()
        self.g.add(self.test_filename)
        # If there are changes to commit, no exception is raised
        asserted = False
        try:
            self.g.commit("unittest")
        except:
            asserted = True
        self.assertEqual(asserted, False)
        os.remove(os.path.join(self.cfg.GIT_BASE, self.test_filename))

    def test_git_diff(self):
        self.assertFalse(self.g.diff())
        f = open(os.path.join(self.cfg.GIT_BASE, "README.md"), "a")
        f.write("abc123")
        f.close()
        self.g.add("README.md")
        self.assertTrue(self.g.diff())
        self.g.clear()

        ret = subprocess.run(
            ["git", "restore", "README.md"],
            cwd=self.cfg.GIT_BASE,
            capture_output=True,
        )

        if ret.returncode != 0:
            raise ChildProcessError(
                f"Couldn't restore README:\n"
                f"args: {ret.args}\n"
                f"stdout: {ret.stdout.decode()}\n"
                f"stderr: {ret.stderr.decode()}"
            )

    def test_git_exists(self):
        # With no directory, exists() should fail
        try:
            shutil.rmtree(self.cfg.GIT_BASE)
        except FileNotFoundError:
            pass
        self.assertFalse(self.g.git_exists())
        # With an empty directory it should fail because it has no .git dir
        os.makedirs(self.cfg.GIT_BASE, exist_ok = False)
        self.assertFalse(self.g.git_exists())
        # Only after cloning a repo should it return True
        self.g.clone()
        self.assertTrue(self.g.git_exists())

    def test_gen_git_path_ymd(self):
        self.assertRaises(ValueError, self.g.gen_git_path_ymd, None)
        self.assertRaises(TypeError, self.g.gen_git_path_ymd, 123)
        self.assertEqual(
            self.g.gen_git_path_ymd("20220401"),
            self.cfg.GIT_BASE + "2022/04/01"
        )

    def test_gen_git_url_ymd(self):
        self.assertRaises(ValueError, self.g.gen_git_url_ymd, None)
        self.assertRaises(TypeError, self.g.gen_git_url_ymd, 123)
        self.assertEqual(
            self.g.gen_git_url_ymd("20220401"),
            self.cfg.GIT_STAT_BASE_URL + "2022/04/01"
        )

    def test_pull(self):
        # Nothing should happen if local is up to date with remote
        self.g.clear()

        asserted = False
        try:
            self.g.pull()
        except:
            asserted = True
        self.assertEqual(asserted, False)


    def test_push(self):
        # With no new commits to push, nothing should happen
        self.g.clear()

        asserted = False
        try:
            self.g.push()
        except:
            asserted = True
        self.assertEqual(asserted, False)

if __name__ == '__main__':
    unittest.main(verbosity=2)