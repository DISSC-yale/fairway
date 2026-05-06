try:
    from importlib.metadata import version, PackageNotFoundError
    __version__ = version("fairway")
except PackageNotFoundError:
    __version__ = "unknown"
