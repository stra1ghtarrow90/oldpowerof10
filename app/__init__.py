from __future__ import annotations


def __getattr__(name: str):
    if name == "app":
        from .web import app

        return app
    raise AttributeError(name)
