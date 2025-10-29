# api/__init__.py
from flask import Flask
import sys
from pathlib import Path

# Fix path
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Import your app from root (now it's a module)
from app import app
app = app  # Optional, but clear
application = app
# Vercel looks for `application` or `app`
# We expose it here