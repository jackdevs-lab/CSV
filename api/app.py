# api/app.py
from flask import Flask, render_template

app = Flask(__name__, template_folder="../templates", static_folder="../static")

@app.route("/")
def index():
    return render_template("index.html")

# Add this so Vercel knows it's a serverless function
def handler(event, context):
    from werkzeug.serving import run_simple
    return run_simple('localhost', 3000, app)