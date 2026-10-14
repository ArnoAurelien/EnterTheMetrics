from __future__ import annotations

import json
import io
import posixpath
import tarfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class TranscriptItem:
    external_id: str
    timestamp: float | None
    role: str
    kind: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AgentTranscript:
    provider: str
    thread_id: str
    title: str
    source_path: str
    cwd: str | None
    started_at: float | None
    updated_at: float | None
    items: tuple[TranscriptItem, ...]


@dataclass(frozen=True)
class AgentSource:
    provider: str
    status: str
    evidence: tuple[str, ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class AgentScan:
    sources: tuple[AgentSource, ...]
    transcripts: tuple[AgentTranscript, ...]
    skipped_files: int = 0


MAX_FILES_PER_SCAN = 200
MAX_FILES_PER_PROVIDER = 50
MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_BYTES_PER_SCAN = 10 * 1024 * 1024


class _ChunkReader:
    def __init__(self, chunks: Iterable[bytes], limit: int):
        self._chunks = iter(chunks)
        self._limit = limit
        self._read = 0
        self._buffer = bytearray()
        self._finished = False
        self.truncated = False

    @property
    def bytes_read(self) -> int:
        return self._read

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = self._limit
        while len(self._buffer) < size and not self._finished:
            try:
                chunk = next(self._chunks)
            except StopIteration:
                self._finished = True
                break
            if not chunk:
                self._finished = True
                break
            available = self._limit - self._read
            if available <= 0:
                self._finished = True
                self.truncated = True
                break
            self._buffer.extend(chunk[:available])
            self._read += min(len(chunk), available)
            if len(chunk) > available:
                self._finished = True
                self.truncated = True
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    def readinto(self, buffer: bytearray) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


def _probe_script() -> str:
    return r'''scan_root() {
  provider="$1"
  root="$2"
  [ -d "$root" ] || return 0
  printf 'root\t%s\t%s\n' "$provider" "$root"
    if [ "$provider" = "vscode-copilot" ]; then
    find "$root" -type f -path '*/GitHub.copilot-chat/transcripts/*.jsonl' -print 2>/dev/null | head -n 200 | while IFS= read -r file; do printf 'file\t%s\t%s\n' "$provider" "$file"; done
    elif [ "$provider" = "codex" ]; then
        find "$root" -type f \( -name '*.jsonl' -o -name '*.jsonl.zst' \) -print 2>/dev/null | head -n 200 | while IFS= read -r file; do printf 'file\t%s\t%s\n' "$provider" "$file"; done
  else
    find "$root" -type f -name '*.jsonl' -print 2>/dev/null | head -n 200 | while IFS= read -r file; do printf 'file\t%s\t%s\n' "$provider" "$file"; done
  fi
}
for tool in codex claude copilot; do
  location=$(command -v "$tool" 2>/dev/null) || location=
  [ -n "$location" ] && printf 'binary\t%s\t%s\n' "$tool" "$location"
done
scan_home() {
    home="$1"
    [ -d "$home" ] || return 0
  scan_root codex "$home/.codex/sessions"
  scan_root codex "$home/.codex/archived_sessions"
  scan_root claude-code "$home/.claude/projects"
  scan_root copilot-cli "$home/.copilot/session-state"
  scan_root vscode-copilot "$home/.vscode-server/data/User/workspaceStorage"
  scan_root vscode-copilot "$home/.vscode-server-insiders/data/User/workspaceStorage"
}
scan_home /root
for home in /home/*; do
    [ -d "$home" ] && scan_home "$home"
done
while IFS=: read -r _ _ _ _ _ home _; do
    case "$home" in /*) scan_home "$home" ;; esac
done < /etc/passwd
'''


def _archive_file(container: Any, path: str, remaining_bytes: int) -> bytes | None:
    archive, stat = container.get_archive(path)
    if isinstance(stat, dict) and stat.get("size", 0) > min(MAX_FILE_BYTES, remaining_bytes):
        return None
    chunks = []
    size = 0
    for chunk in archive:
        size += len(chunk)
        if size > min(MAX_FILE_BYTES + 10240, remaining_bytes + 10240):
            return None
        chunks.append(chunk)
    with tarfile.open(fileobj=io.BytesIO(b"".join(chunks)), mode="r:*") as tar:
        member = next((entry for entry in tar if entry.isfile()), None)
        if member is None or member.size > min(MAX_FILE_BYTES, remaining_bytes):
            return None
        stream = tar.extractfile(member)
        return stream.read(MAX_FILE_BYTES + 1) if stream else None


def _decode_transcript(path: str, data: bytes) -> str | None:
    if path.endswith(".jsonl.zst"):
        try:
            import zstandard
        except ImportError:
            return None
        try:
            with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as reader:
                data = reader.read(MAX_FILE_BYTES + 1)
        except (OSError, zstandard.ZstdError):
            return None
        if len(data) > MAX_FILE_BYTES:
            return None
    return data.decode("utf-8", errors="replace")


def _archive_fallback(container: Any) -> AgentScan:
    providers = ("codex", "claude-code", "copilot-cli", "vscode-copilot")
    homes = {"/root"}
    try:
        passwd = _archive_file(container, "/etc/passwd", MAX_FILE_BYTES)
    except Exception:
        passwd = None
    if passwd:
        for line in passwd.decode("utf-8", errors="replace").splitlines():
            fields = line.split(":")
            if len(fields) >= 6 and fields[5].startswith("/"):
                homes.add(fields[5])

    sources: dict[str, set[str]] = {}
    transcripts: list[AgentTranscript] = []
    file_counts: dict[str, int] = {}
    file_seen: set[str] = set()
    skipped_by_provider: dict[str, int] = {}
    remaining_bytes = MAX_BYTES_PER_SCAN
    skipped = 0
    roots = []
    for home in sorted(homes):
        roots.extend(
            [
                ("codex", posixpath.join(home, ".codex/sessions")),
                ("codex", posixpath.join(home, ".codex/archived_sessions")),
                ("claude-code", posixpath.join(home, ".claude/projects")),
                ("copilot-cli", posixpath.join(home, ".copilot/session-state")),
                ("vscode-copilot", posixpath.join(home, ".vscode-server/data/User/workspaceStorage")),
                ("vscode-copilot", posixpath.join(home, ".vscode-server-insiders/data/User/workspaceStorage")),
            ]
        )
    for provider, root in roots:
        if remaining_bytes <= 0 or len(transcripts) >= MAX_FILES_PER_SCAN:
            break
        if file_counts.get(provider, 0) >= MAX_FILES_PER_PROVIDER:
            skipped += 1
            skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
            continue
        try:
            chunks, _ = container.get_archive(root)
        except Exception:
            continue
        sources.setdefault(provider, set()).add(f"state:{root}")
        reader = _ChunkReader(chunks, remaining_bytes)
        try:
            with tarfile.open(fileobj=reader, mode="r|*") as archive:
                while True:
                    try:
                        member = archive.next()
                    except (EOFError, tarfile.ReadError):
                        break
                    if member is None:
                        break
                    if not member.isfile() or not member.name.endswith((".jsonl", ".jsonl.zst")):
                        continue
                    file_seen.add(provider)
                    if file_counts.get(provider, 0) >= MAX_FILES_PER_PROVIDER:
                        skipped += 1
                        skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
                        break
                    if len(transcripts) >= MAX_FILES_PER_SCAN:
                        skipped += 1
                        skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
                        break
                    if member.size > min(MAX_FILE_BYTES, remaining_bytes):
                        skipped += 1
                        skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
                        continue
                    stream = archive.extractfile(member)
                    if stream is None:
                        skipped += 1
                        skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
                        continue
                    data = stream.read(min(MAX_FILE_BYTES, remaining_bytes) + 1)
                    if len(data) > min(MAX_FILE_BYTES, remaining_bytes):
                        skipped += 1
                        skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
                        continue
                    file_counts[provider] = file_counts.get(provider, 0) + 1
                    source_path = posixpath.join(root, member.name)
                    decoded = _decode_transcript(source_path, data)
                    transcript = parse_transcript(provider, source_path, decoded) if decoded is not None else None
                    if transcript is not None:
                        transcripts.append(transcript)
        except (tarfile.ReadError, EOFError, OSError):
            skipped += 1
            skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
        finally:
            remaining_bytes = max(0, remaining_bytes - reader.bytes_read)
            if reader.truncated:
                skipped += 1
                skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1

    if not sources:
        status = "not_detected" if passwd else "probe_unavailable"
        detail = "No known agent CLI or state directory found." if passwd else "Container probe unavailable."
        return AgentScan(
            tuple(AgentSource(provider, status, detail=detail) for provider in providers),
            (),
            skipped,
        )
    source_rows = []
    for provider, evidence in sorted(sources.items()):
        found = any(item.provider == provider for item in transcripts)
        provider_skipped = skipped_by_provider.get(provider, 0)
        status = "history_partial" if found and provider_skipped else "history_found" if found else "history_unsupported" if provider in file_seen else "history_unavailable"
        detail = "Transcript files discovered." if found else "State directory found; no supported local transcript found."
        if provider_skipped:
            detail += f" {provider_skipped} file(s) were skipped by scan limits or could not be read."
        source_rows.append(
            AgentSource(
                provider,
                status,
                tuple(sorted(evidence)),
                detail,
            )
        )
    return AgentScan(tuple(source_rows), tuple(transcripts), skipped)


def scan_container(container: Any) -> AgentScan:
    """Inspect common agent state locations using read-only Docker operations."""
    try:
        result = container.exec_run(["/bin/sh", "-c", _probe_script()], stdout=True, stderr=False)
        exit_code = getattr(result, "exit_code", 0)
        output = getattr(result, "output", b"")
        if exit_code != 0:
            raise RuntimeError("container probe failed")
        if isinstance(output, tuple):
            output = b"".join(part for part in output if part)
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
    except Exception:
        return _archive_fallback(container)

    evidence: dict[str, list[str]] = {}
    file_paths: list[tuple[str, str]] = []
    for line in output.splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        kind, provider, value = parts
        if kind == "binary":
            provider = {"codex": "codex", "claude": "claude-code", "copilot": "copilot-cli"}.get(provider, provider)
            evidence.setdefault(provider, []).append(f"cli:{value}")
        elif kind == "root":
            evidence.setdefault(provider, []).append(f"state:{value}")
        elif kind == "file":
            file_paths.append((provider, value))
            evidence.setdefault(provider, [])

    transcripts: list[AgentTranscript] = []
    skipped = 0
    skipped_by_provider: dict[str, int] = {}
    remaining_bytes = MAX_BYTES_PER_SCAN
    unique_paths = []
    provider_counts: dict[str, int] = {}
    all_paths = list(dict.fromkeys(file_paths))
    for index, (provider, path) in enumerate(all_paths):
        if len(unique_paths) >= MAX_FILES_PER_SCAN:
            for skipped_provider, _ in all_paths[index:]:
                skipped_by_provider[skipped_provider] = skipped_by_provider.get(skipped_provider, 0) + 1
            break
        if provider_counts.get(provider, 0) >= MAX_FILES_PER_PROVIDER:
            skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
            continue
        unique_paths.append((provider, path))
        provider_counts[provider] = provider_counts.get(provider, 0) + 1
    skipped += sum(skipped_by_provider.values())
    for provider, path in unique_paths:
        try:
            archived = _archive_file(container, path, remaining_bytes)
        except Exception:
            skipped += 1
            skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
            continue
        if archived is None:
            skipped += 1
            skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
            continue
        remaining_bytes -= len(archived)
        content = _decode_transcript(path, archived)
        if content is None:
            skipped += 1
            skipped_by_provider[provider] = skipped_by_provider.get(provider, 0) + 1
            continue
        transcript = parse_transcript(provider, path, content)
        if transcript is not None:
            transcripts.append(transcript)

    sources = []
    for provider, provider_evidence in sorted(evidence.items()):
        has_transcript = any(transcript.provider == provider for transcript in transcripts)
        has_file = any(provider == candidate for candidate, _ in file_paths)
        status = "history_found" if has_transcript else "history_unavailable"
        if has_file and not has_transcript:
            status = "history_unsupported"
        provider_skipped = skipped_by_provider.get(provider, 0)
        if has_transcript and provider_skipped:
            status = "history_partial"
        detail = "Transcript files discovered; scan is bounded." if has_transcript else "Agent detected; no supported local transcript found."
        if provider_skipped:
            detail += f" {provider_skipped} file(s) were skipped by scan limits or could not be read."
        sources.append(AgentSource(provider, status, tuple(provider_evidence), detail))
    if not sources:
        sources = [
            AgentSource(provider, "not_detected", detail="No known agent CLI or state directory found.")
            for provider in ("codex", "claude-code", "copilot-cli", "vscode-copilot")
        ]
    return AgentScan(tuple(sources), tuple(transcripts), skipped)


def _timestamp(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric / 1000 if numeric > 100_000_000_000 else numeric
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _first(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        return ""
    fragments = []
    for part in value:
        if not isinstance(part, dict):
            continue
        if part.get("type") in {"thinking", "reasoning", "image", "input_image"}:
            continue
        text = _first(part, "text", "output_text", "input_text")
        if isinstance(text, str) and text:
            fragments.append(text)
    return "\n".join(fragments)


def _artifact_metadata(value: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for key in ("name", "command", "cwd", "status", "tool", "toolName", "toolCallId", "arguments", "input", "exitCode", "success"):
        item = value.get(key)
        if isinstance(item, (str, int, float, bool, list, dict)):
            metadata[key] = item
    return metadata


def _add_item(
    items: list[TranscriptItem],
    *,
    record: dict[str, Any],
    line_number: int,
    role: str,
    kind: str,
    content: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    if not content and not metadata:
        return
    item_id = _first(record, "id", "uuid", "messageId", "message_id")
    stable_id = str(item_id) if item_id is not None else f"line-{line_number}-{len(items)}"
    timestamp = _timestamp(_first(record, "timestamp", "createdAt", "created_at", "ts"))
    items.append(
        TranscriptItem(
            external_id=stable_id,
            timestamp=timestamp,
            role=role,
            kind=kind,
            content=content,
            metadata=metadata or {},
        )
    )


def _codex_record(record: dict[str, Any], line_number: int, items: list[TranscriptItem]) -> tuple[str | None, str | None]:
    event_type = record.get("type")
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return None, None
    if event_type == "session_meta":
        meta = payload.get("meta")
        meta = meta if isinstance(meta, dict) else payload
        thread_id = _first(meta, "id", "session_id", "sessionId")
        cwd = _first(meta, "cwd", "working_directory", "workingDirectory")
        return (str(thread_id) if thread_id is not None else None, str(cwd) if cwd else None)

    item_type = payload.get("type")
    if event_type == "response_item" and item_type == "message":
        role = payload.get("role")
        if role in {"user", "assistant"}:
            _add_item(items, record=record, line_number=line_number, role=role, kind="message", content=_text(payload.get("content")))
    elif event_type == "response_item" and item_type == "reasoning":
        summary = _text(payload.get("summary"))
        if summary:
            _add_item(items, record=record, line_number=line_number, role="assistant", kind="reasoning_summary", content=summary)
    elif event_type == "response_item" and item_type in {"function_call", "function_call_output"}:
        _add_item(
            items,
            record=record,
            line_number=line_number,
            role="assistant" if item_type == "function_call" else "tool",
            kind="tool_call" if item_type == "function_call" else "tool_result",
            content="",
            metadata=_artifact_metadata(payload),
        )
    elif event_type == "event_msg" and payload.get("type") in {"user_message", "agent_message"}:
        role = "user" if payload["type"] == "user_message" else "assistant"
        _add_item(items, record=record, line_number=line_number, role=role, kind="message", content=_text(payload.get("message")))
    elif event_type == "event_msg" and payload.get("type", "").startswith("exec_command"):
        _add_item(items, record=record, line_number=line_number, role="assistant", kind="tool_activity", content="", metadata=_artifact_metadata(payload))
    return None, None


def _claude_record(record: dict[str, Any], line_number: int, items: list[TranscriptItem]) -> None:
    role = record.get("type")
    message = record.get("message")
    if not isinstance(message, dict):
        message = record
    content = message.get("content")
    if role in {"user", "assistant"}:
        text = _text(content)
        if text:
            _add_item(items, record=record, line_number=line_number, role=role, kind="message", content=text)
        if isinstance(content, list):
            for index, part in enumerate(content):
                if not isinstance(part, dict) or part.get("type") not in {"tool_use", "tool_result"}:
                    continue
                artifact = _artifact_metadata(part)
                if part.get("type") == "tool_use":
                    artifact.setdefault("name", part.get("name"))
                    artifact.setdefault("input", part.get("input"))
                _add_item(
                    items,
                    record={**record, "id": f"{record.get('uuid', line_number)}-{index}"},
                    line_number=line_number,
                    role="assistant" if part["type"] == "tool_use" else "tool",
                    kind=part["type"],
                    content="",
                    metadata=artifact,
                )


def _generic_record(record: dict[str, Any], line_number: int, items: list[TranscriptItem]) -> None:
    message = record.get("message")
    message = message if isinstance(message, dict) else record
    role = _first(message, "role", "speaker")
    if role not in {"user", "assistant", "tool"}:
        return
    content = _first(message, "content", "text", "message")
    text = _text(content)
    if text:
        _add_item(items, record=record, line_number=line_number, role=role, kind="message", content=text)
    for key, kind in (("reasoning_summary", "reasoning_summary"), ("summary", "reasoning_summary")):
        summary = message.get(key)
        if isinstance(summary, str) and summary:
            _add_item(items, record=record, line_number=line_number, role="assistant", kind=kind, content=summary)
            break
    tool = _first(message, "toolCall", "tool_call", "toolUse", "tool_use")
    if isinstance(tool, dict):
        _add_item(items, record=record, line_number=line_number, role="assistant", kind="tool_call", content="", metadata=_artifact_metadata(tool))


def _vscode_copilot_record(
    record: dict[str, Any],
    line_number: int,
    items: list[TranscriptItem],
) -> tuple[str | None, str | None]:
    event_type = record.get("type")
    data = record.get("data")
    if not isinstance(data, dict):
        return None, None
    if event_type == "session.start":
        session_id = _first(data, "sessionId", "session_id")
        cwd = _first(data, "cwd", "workingDirectory", "working_directory")
        return (str(session_id) if session_id is not None else None, str(cwd) if isinstance(cwd, str) else None)
    if event_type == "user.message":
        _add_item(items, record=record, line_number=line_number, role="user", kind="message", content=_text(data.get("content")))
    elif event_type == "assistant.message":
        _add_item(items, record=record, line_number=line_number, role="assistant", kind="message", content=_text(data.get("content")))
        summary = _text(_first(data, "reasoningSummary", "reasoning_summary"))
        if summary:
            _add_item(items, record={**record, "id": f"{record.get('id', line_number)}-summary"}, line_number=line_number, role="assistant", kind="reasoning_summary", content=summary)
        tool_requests = data.get("toolRequests")
        if isinstance(tool_requests, list):
            for index, request in enumerate(tool_requests):
                if not isinstance(request, dict):
                    continue
                _add_item(
                    items,
                    record={**record, "id": f"{record.get('id', line_number)}-tool-{index}"},
                    line_number=line_number,
                    role="assistant",
                    kind="tool_call",
                    content="",
                    metadata=_artifact_metadata(request),
                )
    elif event_type in {"tool.execution_start", "tool.execution_complete"}:
        _add_item(
            items,
            record=record,
            line_number=line_number,
            role="tool",
            kind="tool_activity" if event_type == "tool.execution_start" else "tool_result",
            content="",
            metadata=_artifact_metadata(data),
        )
    return None, None


def parse_transcript(provider: str, source_path: str | Path, content: str) -> AgentTranscript | None:
    """Normalize known JSONL transcript shapes without retaining raw reasoning blocks."""
    normalized_provider = provider.casefold().replace("_", "-")
    items: list[TranscriptItem] = []
    thread_id: str | None = None
    cwd: str | None = None
    for line_number, line in enumerate(content.splitlines(), start=1):
        try:
            record = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(record, dict):
            continue
        record_id = _first(record, "threadId", "thread_id", "sessionId", "session_id")
        if record_id is not None and thread_id is None:
            thread_id = str(record_id)
        record_cwd = _first(record, "cwd", "workingDirectory", "working_directory")
        if isinstance(record_cwd, str) and cwd is None:
            cwd = record_cwd
        if normalized_provider == "codex":
            meta_id, meta_cwd = _codex_record(record, line_number, items)
            thread_id = meta_id or thread_id
            cwd = meta_cwd or cwd
        elif normalized_provider in {"claude", "claude-code"}:
            _claude_record(record, line_number, items)
        elif normalized_provider in {"copilot", "copilot-cli", "github-copilot"}:
            _generic_record(record, line_number, items)
        elif normalized_provider in {"vscode", "vscode-copilot", "github-copilot-chat"}:
            session_id, session_cwd = _vscode_copilot_record(record, line_number, items)
            thread_id = session_id or thread_id
            cwd = session_cwd or cwd
        else:
            return None

    if not items:
        return None
    source = Path(source_path)
    if thread_id is None:
        generic_names = {"events", "messages", "session", "transcript"}
        thread_id = source.parent.name if source.stem.casefold() in generic_names else source.stem
    title = ""
    for item in items:
        if item.role == "user" and item.kind == "message" and item.content.strip():
            title = item.content.strip().splitlines()[0][:160]
            break
    timestamps = [item.timestamp for item in items if item.timestamp is not None]
    return AgentTranscript(
        provider=normalized_provider,
        thread_id=thread_id,
        title=title or source.stem,
        source_path=str(source),
        cwd=cwd,
        started_at=min(timestamps) if timestamps else None,
        updated_at=max(timestamps) if timestamps else None,
        items=tuple(items),
    )


def parse_jsonl_files(provider: str, files: Iterable[tuple[str, str]]) -> list[AgentTranscript]:
    transcripts = []
    for path, content in files:
        transcript = parse_transcript(provider, path, content)
        if transcript is not None:
            transcripts.append(transcript)
    return transcripts