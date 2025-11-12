# api/__init__.py
from flask import Flask
import sys
from pathlib import Path
from flask import Flask, request

from flask import Flask, render_template
# Fix path
sys.path.append(str(Path(__file__).resolve().parent.parent))
app = Flask(__name__, template_folder='../templates')  # This is correct

# Import your app from root (now it's a module)
from app import app
@app.route('/disconnect')
def disconnect():
        return render_template('disconnect.html')
@app.route('/eula')
def eula():
    return render_template('eula.html')
@app.route('/callback')
def callback():
    code = request.args.get('code')
    realm_id = request.args.get('realmId')
    if code:
        return f"<h1>SUCCESS!</h1><p>Code: {code}</p><p>Realm ID: {realm_id}</p>"
    return "Error: No code", 400
@app.route('/privacy')
def privacy():
    return render_template('privacy.html')
app = app  # Optional, but clear
application = app
# Vercel looks for `application` or `app`
# We expose it here