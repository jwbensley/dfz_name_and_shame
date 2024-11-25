import subprocess
subprocess.run(
    ["mkdir", "-p", "/opt/dnas_data/"],
    capture_output=True,
)
ret = subprocess.run(
    ["git", "clone", "git@github.com:DFZ-Name-and-Shame/dnas_stats.git", "/opt/dnas_data/"],
    capture_output=True,
)
print(ret.returncode)
print(ret.stdout.decode())
print(ret.stderr.decode())
ret = subprocess.run(
    ["ls", "-l", "/opt/dnas_data/"],
    capture_output=True,
)
print(ret.returncode)
print(ret.stdout.decode())
print(ret.stderr.decode())
