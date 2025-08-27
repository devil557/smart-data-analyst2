import os
import io
import re
import uuid
import sqlite3
import datetime
from typing import Dict, List, Optional
from flask import Flask, render_template, request, jsonify, session, send_file
from werkzeug.utils import secure_filename
import pandas as pd
from pandasai import SmartDataframe
from pandasai_openai import OpenAI
from config import OPENAI_API_KEY
from visualization import generate_chart
from pdf_export import export_analysis_to_pdf
import plotly.io as pio
from data_handler import load_any_file, analyze_data
import re
import numpy as np
from flask import send_from_directory
from flask import url_for


# 🔄 Preload AI modules to avoid first-call crash
print("🔄 Preloading AI modules...")
try:
    dummy_df = pd.DataFrame({"col": [1, 2, 3]})
    _ = analyze_data(dummy_df, "test query to warm up")
    print("✅ AI warmup complete")
except Exception as e:
    print(f"⚠️ Warmup failed: {e}")
#==========================================


# ============================================================
# MySQL → SQLite converter for SQL uploads
# ============================================================
def mysql_to_sqlite(sql_text: str) -> str:
    sql_text = re.sub(r'\bAUTO_INCREMENT\b', '', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'ENGINE\s*=\s*\w+\s*', '', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'DEFAULT CHARSET=\w+', '', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'CHARACTER SET\s+\w+', '', sql_text, flags=re.IGNORECASE)
    sql_text = sql_text.replace('`', '"')
    sql_text = re.sub(r'\bint\(\d+\)', 'INTEGER', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'UNSIGNED', '', sql_text, flags=re.IGNORECASE)
    return sql_text

def load_sql_file(file_bytes: bytes) -> dict:
    df_list = {}
    sql_text = file_bytes.decode("utf-8", errors="ignore")
    sql_text = mysql_to_sqlite(sql_text)
    conn = sqlite3.connect(":memory:")
    conn.executescript(sql_text)
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
    for table in tables["name"].tolist():
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        df_list[table] = df
    return df_list



# ============================================================
# Flask setup
# ============================================================
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key")

SESS_DATAFRAMES: Dict[str, Dict[str, pd.DataFrame]] = {}
SESS_ACTIVE_DATASET: Dict[str, str] = {}
SESS_CHAT_HISTORY: Dict[str, List[Dict[str, str]]] = {}

ALLOWED_EXTS = {
    ".csv", ".xlsx", ".xls", ".txt", ".sql"
}


@app.route('/exports/<path:filename>')
def serve_exports(filename):
    return send_from_directory('exports', filename)
# ============================================================
# Helpers
# ============================================================
def _get_sid() -> str:
    if "sid" not in session:
        session["sid"] = str(uuid.uuid4())
    return session["sid"]

def _ensure_session_structs(sid: str):
    SESS_DATAFRAMES.setdefault(sid, {})
    SESS_CHAT_HISTORY.setdefault(sid, [])

def _ext_ok(filename: str) -> bool:
    return any(filename.lower().endswith(ext) for ext in ALLOWED_EXTS)

# ============================================================
# Routes
# ============================================================
@app.route("/", methods=["GET"])
def index():
    sid = _get_sid()
    _ensure_session_structs(sid)
    datasets = list(SESS_DATAFRAMES[sid].keys())
    active = SESS_ACTIVE_DATASET.get(sid)
    history = SESS_CHAT_HISTORY.get(sid, [])
    return render_template("index.html", datasets=datasets, active_dataset=active, history=history)


from werkzeug.utils import secure_filename

@app.route("/upload", methods=["POST"])
def upload():
    sid = _get_sid()
    _ensure_session_structs(sid)

    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file part"}), 400
    f = request.files["file"]
    if not f or f.filename == "":
        return jsonify({"ok": False, "error": "No file selected"}), 400
    if not _ext_ok(f.filename):
        return jsonify({"ok": False, "error": "Unsupported file type"}), 400

    try:
        display_name = secure_filename(f.filename)

        # Handle SQL uploads separately
        if display_name.lower().endswith(".sql"):
            df_dict = load_sql_file(f.read())
            if not df_dict:
                return jsonify({"ok": False, "error": "No tables found in SQL"}), 400
            for tname, df in df_dict.items():
                SESS_DATAFRAMES[sid][tname] = df
            first_table = list(df_dict.keys())[0]
            SESS_ACTIVE_DATASET[sid] = first_table
            df = df_dict[first_table]

        # Handle all other supported file types
        else:
            df = load_any_file(f)
            if df is None or df.empty:
                return jsonify({"ok": False, "error": "Failed to load DataFrame"}), 400
            SESS_DATAFRAMES[sid][display_name] = df
            SESS_ACTIVE_DATASET[sid] = display_name

        # Replace NaN/inf/-inf with None for safe JSON serialization
        preview_df = df.head(20).copy()
        preview_df = preview_df.replace({np.nan: None, np.inf: None, -np.inf: None})
        preview = preview_df.to_dict(orient="records")

        return jsonify({
            "ok": True,
            "dataset": SESS_ACTIVE_DATASET[sid],
            "columns": list(df.columns),
            "preview": preview,
            "datasets": list(SESS_DATAFRAMES[sid].keys())
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/set-dataset", methods=["POST"])
def set_dataset():
    sid = _get_sid()
    _ensure_session_structs(sid)
    data = request.get_json(force=True)
    name = data.get("dataset")
    if not name or name not in SESS_DATAFRAMES[sid]:
        return jsonify({"ok": False, "error": "Dataset not found"}), 404
    SESS_ACTIVE_DATASET[sid] = name
    return jsonify({"ok": True, "active": name})






@app.route("/ask", methods=["POST"])
def ask():
    sid = _get_sid()
    _ensure_session_structs(sid)
    data = request.get_json(force=True)
    question = (data.get("question") or "").strip()

    active_name = SESS_ACTIVE_DATASET.get(sid)
    if not active_name:
        return jsonify({"ok": False, "error": "No active dataset"}), 400
    df = SESS_DATAFRAMES[sid][active_name]

    try:
        result = analyze_data(df, question)
    except Exception as e:
        return jsonify({"ok": False, "error": f"AI error: {e}"}), 500

    chart_json = None
    if isinstance(result, pd.DataFrame) and not result.empty:
        # ✅ Reset index cleanly
        result = result.reset_index(drop=True)
        result.index = result.index + 1

        # ✅ Always generate real HTML table (no custom "name/email" logic)
        html_answer = result.to_html(
            classes="chat-table",
            index=True,
            border=1,
            justify="center",
            escape=False
        )

        # ✅ Clean plain text (no "[n rows x m cols]")
        with pd.option_context("display.max_colwidth", None, "display.width", 10000):
            plain_text = result.to_string(index=False)
        plain_text = re.sub(r"\n*\[\d+\s+rows\s+x\s+\d+\s+columns\]\s*$", "", plain_text)

        # ✅ Try chart generation
        fig = generate_chart(result)
        if fig:
            chart_json = pio.to_json(fig)

    else:
        plain_text = str(result) if result is not None else "No result"
        html_answer = f"<p>{plain_text}</p>"

        # ✅ Append any image files if found
        png_files = re.findall(r'(https?://\S+\.png|\S+\.png)', plain_text)
        for png in png_files:
            img_url = png if png.startswith("http") else f"/{png}"
            html_answer += f'<br><img src="{img_url}" alt="Chart" style="max-width:100%; height:auto;">'

    # ✅ Save chat history
    SESS_CHAT_HISTORY[sid].append({"q": question, "a": plain_text, "a_html": html_answer})

    return jsonify({
        "ok": True,
        "answer": html_answer,
        "chart": chart_json,
        "history": SESS_CHAT_HISTORY[sid]
    })


@app.route("/preview", methods=["GET"])
def preview():
    sid = _get_sid()
    active_name = SESS_ACTIVE_DATASET.get(sid)
    if not active_name:
        return jsonify({"ok": False, "error": "No active dataset"}), 404
    df = SESS_DATAFRAMES[sid][active_name]
    return jsonify({"ok": True, "dataset": active_name, "columns": list(df.columns), "preview": df.head(100).to_dict(orient="records")})

@app.route("/export-pdf", methods=["POST"])
def export_pdf():
    sid = _get_sid()
    history = SESS_CHAT_HISTORY.get(sid, [])
    if not history:
        return jsonify({"ok": False, "error": "No Q&A history"}), 400
    
    # ✅ Only export the last answer
    last_entry = history[-1]  # Get the most recent Q&A
    transcript = f"Query: {last_entry['q']}\n\n Output:\n  {last_entry['a']}"
    
    success, buffer_or_err = export_analysis_to_pdf(transcript)
    if not success:
        return jsonify({"ok": False, "error": buffer_or_err}), 500
    
    memfile = io.BytesIO(buffer_or_err.getvalue())
    memfile.seek(0)
    return send_file(
        memfile,
        mimetype="application/pdf",
        as_attachment=True,
        download_name="ai_analysis_report.pdf"
    )

@app.route("/download/<filename>")
def download_file(filename):
    return send_file(f"static/{filename}", as_attachment=True)



# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    # Run normally (for local dev / Render)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)

