from __future__ import annotations

from wsgiref.simple_server import make_server
import json

from authoritative_pnl.api.routes import dispatch
from authoritative_pnl.settings import Settings
from authoritative_pnl.store.sqlite_store import SqliteStore


def create_app(store: SqliteStore):
    def app(environ, start_response):
        status_code, payload = dispatch(
            store,
            path=environ.get("PATH_INFO", "/"),
            query_string=environ.get("QUERY_STRING", ""),
        )
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        reason = "OK" if status_code < 400 else "ERROR"
        start_response(
            f"{status_code} {reason}",
            [("Content-Type", "application/json"), ("Content-Length", str(len(body)))],
        )
        return [body]

    return app


def serve(settings: Settings, store: SqliteStore) -> None:
    app = create_app(store)
    with make_server(settings.api.host, settings.api.port, app) as server:
        server.serve_forever()
