"""An update must not interrupt a claimed job or a shared model engine."""

import io
import os
import unittest
from unittest.mock import patch

os.environ["WORKER_KIND"] = "zeroclaw"

import hive_worker as worker


class WorkerRestartTests(unittest.TestCase):
    def test_lone_worker_can_restart(self):
        own_pid = os.getpid()

        def open_file(path, *args, **kwargs):
            if path == "/proc/self/cgroup":
                return io.StringIO("0::/system.slice/hive-worker.service\n")
            if path.endswith("cgroup.procs"):
                return io.StringIO(f"{own_pid}\n")
            raise AssertionError(path)

        with (
            patch("builtins.open", side_effect=open_file),
            patch.object(worker.os.path, "isdir", return_value=True),
            patch.object(
                worker.os,
                "walk",
                return_value=[
                    (
                        "/sys/fs/cgroup/system.slice/hive-worker.service",
                        [],
                        ["cgroup.procs"],
                    )
                ],
            ),
        ):
            self.assertFalse(worker._cgroup_has_other_processes())

    def test_shared_child_prevents_restart(self):
        own_pid = os.getpid()

        def open_file(path, *args, **kwargs):
            if path == "/proc/self/cgroup":
                return io.StringIO("0::/system.slice/hive-worker.service\n")
            if path.endswith("cgroup.procs"):
                return io.StringIO(f"{own_pid}\n{own_pid + 1}\n")
            raise AssertionError(path)

        with (
            patch("builtins.open", side_effect=open_file),
            patch.object(worker.os.path, "isdir", return_value=True),
            patch.object(
                worker.os,
                "walk",
                return_value=[
                    (
                        "/sys/fs/cgroup/system.slice/hive-worker.service",
                        [],
                        ["cgroup.procs"],
                    )
                ],
            ),
        ):
            self.assertTrue(worker._cgroup_has_other_processes())

    def test_missing_cgroup_process_listing_fails_closed(self):
        with (
            patch(
                "builtins.open",
                return_value=io.StringIO("0::/system.slice/hive-worker.service\n"),
            ),
            patch.object(worker.os.path, "isdir", return_value=True),
            patch.object(worker.os, "walk", return_value=[]),
        ):
            self.assertTrue(worker._cgroup_has_other_processes())

    def test_relaunch_waits_for_idle_source_change(self):
        new_mtime = worker.SOURCE_MTIME_NS + 1
        with (
            patch.object(worker, "AUTO_RESTART", True),
            patch.object(worker.os, "stat") as stat,
            patch.object(worker, "_cgroup_has_other_processes", return_value=True),
        ):
            stat.return_value.st_mtime_ns = new_mtime
            worker.restart_after_source_update()
        with (
            patch.object(worker, "AUTO_RESTART", True),
            patch.object(worker.os, "stat") as stat,
            patch.object(worker, "_cgroup_has_other_processes", return_value=False),
        ):
            stat.return_value.st_mtime_ns = new_mtime
            with self.assertRaisesRegex(SystemExit, "75"):
                worker.restart_after_source_update()


if __name__ == "__main__":
    unittest.main()
