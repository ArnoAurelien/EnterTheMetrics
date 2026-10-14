import json
import io
import tarfile
import unittest
from types import SimpleNamespace
import zstandard

from agent_history import parse_transcript, scan_container


class TranscriptParserTests(unittest.TestCase):
    def test_parses_codex_messages_and_only_reasoning_summary(self):
        records = [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "type": "session_meta",
                "payload": {"meta": {"id": "thread-1", "cwd": "/repo"}},
            },
            {
                "timestamp": "2026-01-01T00:00:01Z",
                "type": "response_item",
                "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Fix tests"}]},
            },
            {
                "timestamp": "2026-01-01T00:00:02Z",
                "type": "response_item",
                "payload": {"type": "reasoning", "summary": [{"type": "summary_text", "text": "Inspecting the failure."}], "content": "private reasoning"},
            },
            {
                "timestamp": "2026-01-01T00:00:03Z",
                "type": "response_item",
                "payload": {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Fixed."}]},
            },
        ]

        transcript = parse_transcript("codex", "rollout.jsonl", "\n".join(map(json.dumps, records)))

        self.assertEqual(transcript.thread_id, "thread-1")
        self.assertEqual(transcript.cwd, "/repo")
        self.assertEqual(transcript.title, "Fix tests")
        self.assertEqual([item.kind for item in transcript.items], ["message", "reasoning_summary", "message"])
        self.assertNotIn("private reasoning", " ".join(item.content for item in transcript.items))
        self.assertEqual(transcript.started_at, 1767225601)

    def test_parses_claude_messages_and_tool_calls_but_skips_thinking(self):
        records = [
            {
                "type": "user",
                "uuid": "user-1",
                "timestamp": "2026-01-01T00:00:00Z",
                "message": {"role": "user", "content": [{"type": "text", "text": "Run tests"}]},
            },
            {
                "type": "assistant",
                "uuid": "assistant-1",
                "timestamp": "2026-01-01T00:00:01Z",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "thinking", "thinking": "private reasoning"},
                        {"type": "tool_use", "name": "Bash", "input": {"command": "pytest"}},
                        {"type": "text", "text": "Tests passed."},
                    ],
                },
            },
        ]

        transcript = parse_transcript("claude-code", "/home/user/.claude/projects/repo/thread.jsonl", "\n".join(map(json.dumps, records)))

        self.assertEqual(transcript.provider, "claude-code")
        self.assertEqual(transcript.thread_id, "thread")
        self.assertEqual([item.role for item in transcript.items], ["user", "assistant", "assistant"])
        self.assertEqual(transcript.items[-1].kind, "tool_use")
        self.assertEqual(transcript.items[-1].metadata["name"], "Bash")
        self.assertNotIn("private reasoning", " ".join(item.content for item in transcript.items))

    def test_parses_generic_cli_and_vscode_shapes_and_skips_invalid_lines(self):
        content = "\n".join(
            [
                "not json",
                json.dumps({"id": "u1", "timestamp": 1767225600000, "role": "user", "text": "Hello"}),
                json.dumps({"id": "a1", "timestamp": 1767225601, "message": {"role": "assistant", "content": "Hi", "reasoning_summary": "Short rationale"}}),
            ]
        )

        transcript = parse_transcript("copilot-cli", "/home/user/.copilot/session-state/thread/events.jsonl", content)

        self.assertEqual(transcript.thread_id, "thread")
        self.assertEqual(transcript.title, "Hello")
        self.assertEqual([item.kind for item in transcript.items], ["message", "message", "reasoning_summary"])
        self.assertEqual(transcript.items[0].timestamp, 1767225600)

    def test_unknown_provider_and_empty_file_are_ignored(self):
        self.assertIsNone(parse_transcript("unknown", "thread.jsonl", "{}"))
        self.assertIsNone(parse_transcript("codex", "thread.jsonl", ""))

    def test_container_scan_imports_known_state_without_mutating_container(self):
        transcript = "\n".join(
            [
                json.dumps({"type": "session_meta", "payload": {"meta": {"id": "thread-1"}}}),
                json.dumps({"type": "response_item", "payload": {"type": "message", "role": "user", "content": "Hello"}}),
            ]
        )
        file_stream = io.BytesIO()
        with tarfile.open(fileobj=file_stream, mode="w") as archive:
            data = transcript.encode()
            info = tarfile.TarInfo("thread.jsonl")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))

        class FakeContainer:
            def __init__(self):
                self.commands = []

            def exec_run(self, command, **kwargs):
                self.commands.append(command)
                output = (
                    "binary\tcodex\t/usr/local/bin/codex\n"
                    "root\tcodex\t/root/.codex/sessions\n"
                    "file\tcodex\t/root/.codex/sessions/thread.jsonl\n"
                ).encode()
                return SimpleNamespace(exit_code=0, output=output)

            def get_archive(self, path):
                return iter([file_stream.getvalue()]), {"size": len(transcript)}

        container = FakeContainer()
        result = scan_container(container)

        self.assertEqual(len(container.commands), 1)
        self.assertEqual(result.sources[0].provider, "codex")
        self.assertEqual(result.sources[0].status, "history_found")
        self.assertEqual(result.transcripts[0].thread_id, "thread-1")

    def test_container_scan_decodes_compressed_codex_rollout(self):
        transcript = "\n".join(
            [
                json.dumps({"type": "session_meta", "payload": {"meta": {"id": "compressed-thread"}}}),
                json.dumps({"type": "response_item", "payload": {"type": "message", "role": "user", "content": "Read compressed history"}}),
            ]
        ).encode()
        compressed = zstandard.ZstdCompressor().compress(transcript)
        archive_stream = io.BytesIO()
        with tarfile.open(fileobj=archive_stream, mode="w") as archive:
            info = tarfile.TarInfo("rollout.jsonl.zst")
            info.size = len(compressed)
            archive.addfile(info, io.BytesIO(compressed))

        class FakeContainer:
            def exec_run(self, command, **kwargs):
                return SimpleNamespace(
                    exit_code=0,
                    output=(
                        "root\tcodex\t/root/.codex/sessions\n"
                        "file\tcodex\t/root/.codex/sessions/rollout.jsonl.zst\n"
                    ).encode(),
                )

            def get_archive(self, path):
                return iter([archive_stream.getvalue()]), {"size": len(compressed)}

        result = scan_container(FakeContainer())

        self.assertEqual(result.transcripts[0].thread_id, "compressed-thread")

    def test_container_probe_failure_is_reported_without_raising(self):
        class FakeContainer:
            def exec_run(self, command, **kwargs):
                raise RuntimeError("container stopped")

        result = scan_container(FakeContainer())

        self.assertEqual(len(result.sources), 4)
        self.assertTrue(all(source.status == "probe_unavailable" for source in result.sources))

    def test_successful_empty_probe_reports_known_providers_as_not_detected(self):
        class FakeContainer:
            def exec_run(self, command, **kwargs):
                return SimpleNamespace(exit_code=0, output=b"")

        result = scan_container(FakeContainer())

        self.assertEqual({source.provider for source in result.sources}, {"codex", "claude-code", "copilot-cli", "vscode-copilot"})
        self.assertTrue(all(source.status == "not_detected" for source in result.sources))

    def test_stopped_container_fallback_reads_archive_without_exec(self):
        def make_archive(name, data):
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode="w") as archive:
                info = tarfile.TarInfo(name)
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
            return output.getvalue()

        transcript = "\n".join(
            [
                json.dumps({"type": "session_meta", "payload": {"meta": {"id": "stopped-thread"}}}),
                json.dumps({"type": "response_item", "payload": {"type": "message", "role": "user", "content": "Inspect old run"}}),
            ]
        ).encode()

        class FakeContainer:
            def exec_run(self, command, **kwargs):
                raise RuntimeError("container is stopped")

            def get_archive(self, path):
                if path == "/etc/passwd":
                    data = b"root:x:0:0:root:/root:/bin/sh\n"
                    return iter([make_archive("passwd", data)]), {"size": len(data)}
                if path == "/root/.codex/sessions":
                    return iter([make_archive("thread.jsonl", transcript)]), {"size": len(transcript)}
                raise FileNotFoundError(path)

        result = scan_container(FakeContainer())

        self.assertEqual(result.transcripts[0].thread_id, "stopped-thread")
        self.assertEqual(result.sources[0].status, "history_found")


if __name__ == "__main__":
    unittest.main()