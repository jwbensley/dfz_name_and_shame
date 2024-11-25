import subprocess
ret = subprocess.run(
    ["git", "clone", "git@github.com:DFZ-Name-and-Shame/dnas_stats.git"],
    capture_output=True,
)
print(ret.returncode)
print(ret.stdout.decode())
print(ret.stderr.decode())
