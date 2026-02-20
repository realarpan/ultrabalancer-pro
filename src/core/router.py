"""
router.py - Advanced pluggable request router for Ultrabalancer Pro

This module implements an asynchronous, extensible router with:
- Trie-based path matching with params
- Method-aware routes and automatic HEAD/OPTIONS
- Middleware pipeline (pre, post) with short-circuiting
- Plugin architecture for custom matchers, middleware, and route providers
- Typed handlers and lifecycle hooks

Best practices followed:
- Clear separation of concerns (routing, middleware, plugins)
- Type hints and docstrings for maintainability
- Minimal dependencies; pure Python
- Defensive coding and explicit errors
- Small, focused classes
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Protocol, Tuple
import asyncio

# Types
Scope = Mapping[str, Any]
Headers = Mapping[str, str]
Params = Dict[str, str]
Handler = Callable[[Scope, Headers, Params], Awaitable[Any]]
Middleware = Callable[[Scope, Headers, Params, Handler], Awaitable[Any]]


class Plugin(Protocol):
    """Base plugin protocol.

    Implement any of:
    - contribute_routes(router): to register routes
    - contribute_middleware(router): to register middleware
    - contribute_matchers(router): to register custom matchers
    - on_startup(router): lifecycle hook
    - on_shutdown(router): lifecycle hook
    """

    def contribute_routes(self, router: "Router") -> None: ...  # pragma: no cover
    def contribute_middleware(self, router: "Router") -> None: ...  # pragma: no cover
    def contribute_matchers(self, router: "Router") -> None: ...  # pragma: no cover
    async def on_startup(self, router: "Router") -> None: ...  # pragma: no cover
    async def on_shutdown(self, router: "Router") -> None: ...  # pragma: no cover


@dataclass
class Route:
    method: str
    pattern: str
    handler: Handler
    name: Optional[str] = None
    # compiled path template into trie segments
    _segments: Tuple[Tuple[str, bool], ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.pattern.startswith("/"):
            raise ValueError(f"Route pattern must start with '/': {self.pattern}")
        self.method = self.method.upper()
        self._segments = tuple(self._compile(self.pattern))

    @staticmethod
    def _compile(pattern: str) -> Iterable[Tuple[str, bool]]:
        """Compile pattern into (segment, is_param) tuples.
        e.g., /users/:id -> [("users", False), ("id", True)]
        """
        parts = [p for p in pattern.strip("/").split("/") if p]
        for part in parts:
            if part.startswith(":"):
                yield (part[1:], True)
            elif part == "*":  # simple wildcard consumes the rest
                yield ("*", True)
            else:
                yield (part, False)

    def match(self, path: str) -> Optional[Params]:
        parts = [p for p in path.strip("/").split("/") if p]
        params: Params = {}
        si = 0
        for si, (seg, is_param) in enumerate(self._segments):
            if seg == "*":
                # wildcard matches the rest
                remainder = parts[si:]
                params["wildcard"] = "/".join(remainder)
                return params
            if si >= len(parts):
                return None
            if is_param:
                params[seg] = parts[si]
            else:
                if parts[si] != seg:
                    return None
        # all route segments consumed; allow trailing path parts only if last seg was '*'
        if len(parts) != len([s for s in self._segments if s[0] != "*"]):
            return None
        return params


class TrieNode:
    def __init__(self) -> None:
        self.literals: Dict[str, TrieNode] = {}
        self.param: Optional[Tuple[str, TrieNode]] = None
        self.wildcard: Optional[TrieNode] = None
        self.handlers: Dict[str, Handler] = {}


class RouteTrie:
    """Method-aware path trie with params and wildcard support."""

    def __init__(self) -> None:
        self.root = TrieNode()

    def add(self, route: Route) -> None:
        node = self.root
        for seg, is_param in route._segments:
            if seg == "*":
                node.wildcard = node.wildcard or TrieNode()
                node = node.wildcard
                break
            if is_param:
                # single param child per depth
                if node.param is None:
                    child = TrieNode()
                    node.param = (seg, child)
                else:
                    name, child = node.param
                    if name != seg:
                        # Allow different names at same position
                        child = node.param[1]
                node = child
            else:
                node.literals.setdefault(seg, TrieNode())
                node = node.literals[seg]
        if route.method in node.handlers:
            raise ValueError(f"Duplicate route for {route.method} {route.pattern}")
        node.handlers[route.method] = route.handler

    def find(self, method: str, path: str) -> Optional[Tuple[Handler, Params]]:
        parts = [p for p in path.strip("/").split("/") if p]
        params: Params = {}

        def dfs(i: int, node: TrieNode) -> Optional[Tuple[Handler, Params]]:
            if i == len(parts):
                # terminal: exact path matched; prefer method, then HEAD->GET fallback
                if method in node.handlers:
                    return node.handlers[method], dict(params)
                if method == "HEAD" and "GET" in node.handlers:
                    return node.handlers["GET"], dict(params)
                if method == "OPTIONS":
                    # synthesize OPTIONS from available methods
                    allow = ", ".join(sorted(node.handlers.keys() | {"OPTIONS"}))
                    async def _options_handler(_s: Scope, _h: Headers, _p: Params) -> Any:
                        return {"status": 204, "headers": {"Allow": allow}}
                    return _options_handler, dict(params)
                return None
            seg = parts[i]
            # literal first
            if seg in node.literals:
                res = dfs(i + 1, node.literals[seg])
                if res:
                    return res
            # param match
            if node.param is not None:
                name, child = node.param
                params[name] = seg
                res = dfs(i + 1, child)
                if res:
                    return res
                params.pop(name, None)
            # wildcard consumes the rest
            if node.wildcard is not None:
                params["wildcard"] = "/".join(parts[i:])
                # resolve at wildcard node terminal
                if method in node.wildcard.handlers:
                    return node.wildcard.handlers[method], dict(params)
                if method == "HEAD" and "GET" in node.wildcard.handlers:
                    return node.wildcard.handlers["GET"], dict(params)
            return None

        return dfs(0, self.root)


class Router:
    """Advanced, pluggable async router.

    Usage:
        router = Router()
        router.add("GET", "/health", handler)
        response = await router.dispatch(scope, headers, method, path)
    """

    def __init__(self) -> None:
        self._trie = RouteTrie()
        self._middleware_pre: List[Middleware] = []
        self._middleware_post: List[Middleware] = []
        self._plugins: List[Plugin] = []
        self._started: bool = False
        # custom matchers by name, if extended
        self._matchers: Dict[str, Callable[[str], bool]] = {}

    # Route registration
    def add(self, method: str, pattern: str, handler: Handler, name: Optional[str] = None) -> None:
        self._trie.add(Route(method=method, pattern=pattern, handler=handler, name=name))

    def get(self, pattern: str, handler: Handler, name: Optional[str] = None) -> None:
        self.add("GET", pattern, handler, name)

    def post(self, pattern: str, handler: Handler, name: Optional[str] = None) -> None:
        self.add("POST", pattern, handler, name)

    def put(self, pattern: str, handler: Handler, name: Optional[str] = None) -> None:
        self.add("PUT", pattern, handler, name)

    def delete(self, pattern: str, handler: Handler, name: Optional[str] = None) -> None:
        self.add("DELETE", pattern, handler, name)

    def head(self, pattern: str, handler: Handler, name: Optional[str] = None) -> None:
        self.add("HEAD", pattern, handler, name)

    def options(self, pattern: str, handler: Handler, name: Optional[str] = None) -> None:
        self.add("OPTIONS", pattern, handler, name)

    # Middleware pipeline
    def use(self, middleware: Middleware, *, stage: str = "pre") -> None:
        if stage not in ("pre", "post"):
            raise ValueError("stage must be 'pre' or 'post'")
        if stage == "pre":
            self._middleware_pre.append(middleware)
        else:
            self._middleware_post.append(middleware)

    # Plugins
    def register_plugin(self, plugin: Plugin) -> None:
        self._plugins.append(plugin)
        # allow immediate contribution upon registration
        if hasattr(plugin, "contribute_routes"):
            plugin.contribute_routes(self)
        if hasattr(plugin, "contribute_middleware"):
            plugin.contribute_middleware(self)
        if hasattr(plugin, "contribute_matchers"):
            plugin.contribute_matchers(self)

    async def startup(self) -> None:
        if self._started:
            return
        self._started = True
        await asyncio.gather(*(p.on_startup(self) for p in self._plugins if hasattr(p, "on_startup")))

    async def shutdown(self) -> None:
        if not self._started:
            return
        await asyncio.gather(*(p.on_shutdown(self) for p in self._plugins if hasattr(p, "on_shutdown")))
        self._started = False

    async def dispatch(self, scope: Scope, headers: Headers, method: str, path: str) -> Any:
        method = method.upper()
        found = self._trie.find(method, path)
        if not found:
            return {"status": 404, "body": f"No route for {method} {path}"}
        handler, params = found

        # Compose middleware around the handler
        async def call_handler(s: Scope, h: Headers, p: Params) -> Any:
            return await handler(s, h, p)

        # post middleware wraps outward-in
        wrapped: Handler = call_handler
        for mw in reversed(self._middleware_post):
            current = wrapped
            async def make_post(m: Middleware, nxt: Handler) -> Handler:  # type: ignore[override]
                async def _wrapped(s: Scope, h: Headers, p: Params) -> Any:
                    return await m(s, h, p, nxt)
                return _wrapped
            wrapped = await make_post(mw, current)  # type: ignore[assignment]

        # pre middleware wraps inward-out
        for mw in self._middleware_pre[::-1]:
            current = wrapped
            async def make_pre(m: Middleware, nxt: Handler) -> Handler:  # type: ignore[override]
                async def _wrapped(s: Scope, h: Headers, p: Params) -> Any:
                    return await m(s, h, p, nxt)
                return _wrapped
            wrapped = await make_pre(mw, current)  # type: ignore[assignment]

        return await wrapped(scope, headers, params)

    # Matcher extension (placeholder to illustrate plugin extensibility)
    def add_matcher(self, name: str, fn: Callable[[str], bool]) -> None:
        if name in self._matchers:
            raise ValueError(f"Matcher '{name}' already exists")
        self._matchers[name] = fn

    def match_with(self, name: str, value: str) -> bool:
        fn = self._matchers.get(name)
        if not fn:
            raise KeyError(f"Unknown matcher: {name}")
        return fn(value)


# Example middleware and usage notes (kept for reference and tests)
async def logging_middleware(scope: Scope, headers: Headers, params: Params, next_handler: Handler) -> Any:
    # Pre-processing
    path = scope.get("path", "")
    method = scope.get("method", "")
    # In production, replace prints with structured logs
    print(f"Request {method} {path} -> params={params}")
    try:
        result = await next_handler(scope, headers, params)
        print(f"Response {method} {path} -> {getattr(result, 'status', getattr(result, 'code', 'ok'))}")
        return result
    except Exception as exc:  # defensive: never let middleware swallow exceptions silently
        print(f"Error while handling {method} {path}: {exc}")
        raise


# Simple factory for default router with health route
async def _health(_s: Scope, _h: Headers, _p: Params) -> Any:
    return {"status": 200, "body": "ok"}


def create_default_router() -> Router:
    router = Router()
    router.get("/health", _health, name="health")
    router.use(logging_middleware, stage="pre")
    return router
