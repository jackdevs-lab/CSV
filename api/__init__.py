# api/__init__.py
from app import app
from flask import render_template

@app.route('/disconnect')
def disconnect():
    return render_template('disconnect.html')

@app.route('/eula')
def eula():
    return render_template('eula.html')

@app.route('/privacy')
def privacy():
    return render_template('privacy.html')

# Vercel entry point
application = app