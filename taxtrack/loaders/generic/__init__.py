# taxtrack/loaders/generic/__init__.py

"""
Generic Loader Package

Dieses Package liefert den generischen CSV/Text-Loader, 
der von load_auto für alle unbekannten Formate verwendet wird.
"""

from .generic_loader import load_generic

__all__ = ["load_generic"]
