import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/version-bump-pr.yml"


class TestVersionBumpWorkflow(unittest.TestCase):
    def test_title_and_bump_type(self):
        steps = yaml.safe_load(WORKFLOW.read_text())["jobs"]["bump"]["steps"]
        title_step = next(step for step in steps if step.get("id") == "prtitle")
        bump_step = next(step for step in steps if step.get("id") == "bumptype")
        self.assertEqual(
            title_step["env"]["HEAD_COMMIT_MESSAGE"],
            "${{ github.event.head_commit.message }}",
        )
        self.assertEqual(bump_step["env"]["MSG"], "${{ steps.prtitle.outputs.title }}")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            gh = path / "gh"
            gh.write_text(
                '#!/bin/bash -e\n'
                '[[ "$3" == "--jq" && "$4" == \'.[0].title // ""\' ]]\n'
                'printf "%s" "$MOCK_PR_TITLE"\n'
            )
            gh.chmod(0o755)
            marker = path / "executed"
            malicious = f'$(touch {marker}) `touch {marker}` "quoted"'
            cases = [
                ("Optionally return the raw output notebook from `nbrun`", "", "push", "", "patch"),
                (f"feat: {malicious}", "", "push", "", "minor"),
                ("", f"fix: {malicious}\n\nBREAKING: body", "push", "", "patch"),
                ("chore: maintenance", "", "push", "", "none"),
                ("feat!: incompatible", "", "push", "", "major"),
                ("fix: ordinary", "", "workflow_dispatch", "major", "major"),
            ]
            for pr_title, commit_message, event, requested_bump, expected in cases:
                with self.subTest(pr_title=pr_title, event=event):
                    output = path / "output"
                    output.write_text("")
                    env = os.environ.copy()
                    env.update({
                        "PATH": f"{path}{os.pathsep}{env['PATH']}",
                        "GITHUB_OUTPUT": str(output),
                        "MOCK_PR_TITLE": pr_title,
                        "HEAD_COMMIT_MESSAGE": commit_message,
                    })
                    title_script = title_step["run"].replace(
                        "${{ github.repository }}", "owner/repo"
                    ).replace("${{ github.sha }}", "commit")
                    subprocess.run(["bash", "-e", "-c", title_script], env=env, check=True)
                    title = (pr_title or commit_message).split("\n", 1)[0]
                    self.assertEqual(output.read_text(), f"title={title}\n")

                    output.write_text("")
                    env["MSG"] = title
                    bump_script = bump_step["run"].replace(
                        "${{ github.event_name }}", event
                    ).replace("${{ github.event.inputs.bump-type }}", requested_bump)
                    subprocess.run(["bash", "-e", "-c", bump_script], env=env, check=True)
                    self.assertEqual(output.read_text(), f"type={expected}\n")
                    self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
