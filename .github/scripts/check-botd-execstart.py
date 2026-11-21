"""Validate the botd ExecStart commands in the deployment runbooks."""

from pathlib import Path
import re
import shlex


def botd_commands(path):
    contents = re.sub(r"\\\n", " ", path.read_text(encoding="utf-8"))
    for line in contents.splitlines():
        if line.lstrip().startswith("ExecStart="):
            args = shlex.split(line.split("=", 1)[1])
            if args and Path(args[0]).name == "botd":
                yield args


root = Path(__file__).resolve().parents[2]
unit, = botd_commands(root / "deploy/systemd/botd.service")
assert "--token-stdin" in unit, "shipped botd unit omits --token-stdin"
for path in (root / "docs").rglob("*.md"):
    for command in botd_commands(path):
        assert "--token-stdin" in command, f"{path.name}: botd override omits --token-stdin"
for name in ("soak-test.md", "real-separate-verification.md"):
    override, = botd_commands(root / "docs" / name)
    assert override == unit + ["--core-mode", "separate"], f"{name}: botd override drifted"
