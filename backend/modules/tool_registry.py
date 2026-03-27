from __future__ import annotations

import ast
import asyncio
import importlib.util
import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ToolRegistryError(RuntimeError):
    def __init__(self, error_type: str, message: str, status_code: int) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.message = message
        self.status_code = status_code


class ToolNotFoundError(ToolRegistryError):
    def __init__(self, tool: str) -> None:
        super().__init__("not_found", f"Tool '{tool}' not found", 404)


class ToolExecutionError(ToolRegistryError):
    pass


@dataclass(frozen=True)
class ToolDescriptor:
    name: str
    description: str
    is_async: bool
    parameters: list[dict[str, Any]]
    source: str
    fn: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "is_async": self.is_async,
            "parameters": self.parameters,
            "source": self.source,
        }


def _is_tool_decorator(node: ast.expr) -> bool:
    target = node.func if isinstance(node, ast.Call) else node
    return (
        isinstance(target, ast.Attribute)
        and target.attr == "tool"
        and isinstance(target.value, ast.Name)
        and target.value.id in {"mcp", "server"}
    )


def _discover_tool_names(module_path: Path) -> list[str]:
    tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
    names: list[str] = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if any(_is_tool_decorator(decorator) for decorator in node.decorator_list):
            names.append(node.name)
    return names


def _load_module(module_path: Path):
    spec = importlib.util.spec_from_file_location("saturn_mcp_server", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _signature_params(fn: Any) -> list[dict[str, Any]]:
    params: list[dict[str, Any]] = []
    for param in inspect.signature(fn).parameters.values():
        annotation = ""
        if param.annotation is not inspect._empty:
            annotation = getattr(param.annotation, "__name__", str(param.annotation))
        default = None if param.default is inspect._empty else param.default
        params.append(
            {
                "name": param.name,
                "kind": str(param.kind).replace("Parameter.", "").lower(),
                "annotation": annotation,
                "required": param.default is inspect._empty,
                "default": default,
            }
        )
    return params


def _normalize_result_payload(result: Any) -> tuple[Any, Any]:
    if result is None:
        empty = {"status": "error", "reason": "empty_result"}
        return empty, empty

    if isinstance(result, (dict, list)):
        return result, result

    if isinstance(result, str):
        text = result.strip()
        if text.startswith("{") or text.startswith("["):
            try:
                parsed = json.loads(text)
                return parsed, parsed
            except Exception:
                pass
        wrapped = {"status": "ok", "result": result}
        return wrapped, wrapped

    wrapped = {"status": "ok", "result": result}
    return wrapped, wrapped


class ToolRegistry:
    def __init__(self, module_path: str | Path) -> None:
        self.module_path = Path(module_path).expanduser().resolve()
        self._module = None
        self._tools: dict[str, ToolDescriptor] = {}

    def _refresh(self) -> None:
        if self._module is not None and self._tools:
            return
        if not self.module_path.exists():
            raise FileNotFoundError(f"MCP server not found: {self.module_path}")
        module = _load_module(self.module_path)
        tool_names = _discover_tool_names(self.module_path)
        tools: dict[str, ToolDescriptor] = {}
        for name in tool_names:
            fn = getattr(module, name, None)
            if not callable(fn):
                continue
            tools[name] = ToolDescriptor(
                name=name,
                description=(inspect.getdoc(fn) or "").strip(),
                is_async=inspect.iscoroutinefunction(fn),
                parameters=_signature_params(fn),
                source=str(self.module_path),
                fn=fn,
            )
        self._module = module
        self._tools = tools

    def list_tools(self) -> list[dict[str, Any]]:
        self._refresh()
        return [self._tools[name].to_dict() for name in sorted(self._tools)]

    def execute(self, tool: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        self._refresh()
        descriptor = self._tools.get(tool)
        if descriptor is None:
            raise ToolNotFoundError(tool)
        payload = dict(args or {})
        try:
            if descriptor.is_async:
                result = asyncio.run(descriptor.fn(**payload))
            else:
                result = descriptor.fn(**payload)
        except TypeError as exc:
            raise ToolExecutionError("invalid_args", str(exc)[:300], 400) from exc
        except Exception as exc:
            raise ToolExecutionError("execution_error", str(exc)[:300], 500) from exc

        normalized, parsed = _normalize_result_payload(result)

        return {
            "ok": True,
            "tool": tool,
            "result": normalized,
            "raw_result": result,
            "result_json": parsed,
            "result_type": type(normalized).__name__,
        }
