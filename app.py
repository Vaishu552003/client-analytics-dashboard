from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import os
import uuid

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = "../data/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.route("/")
def index():
    return render_template("index.html")


# UPLOAD FILE
@app.route("/api/upload", methods=["POST"])
def upload():

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    filename = f"{uuid.uuid4().hex}_{file.filename}"
    filepath = os.path.join(UPLOAD_FOLDER, filename)

    file.save(filepath)

    return jsonify({
        "success": True,
        "filepath": filepath
    })


@app.route("/api/dashboard", methods=["POST"])
def dashboard():

    data = request.json
    filepath = data.get("filepath")

    if not filepath:
        return jsonify({"error": "Filepath missing"}), 400

    # Support CSV & Excel
    if filepath.endswith(".csv"):
        df = pd.read_csv(filepath)
    else:
        df = pd.read_excel(filepath)

    df["date"] = pd.to_datetime(df["date"])

    # KPIs
    total_revenue = df["revenue"].sum()
    total_profit = df["profit"].sum()
    total_units = df["units_sold"].sum()
    deal_close_rate = round((df["deal_closed"].sum() / len(df)) * 100, 2)

    df["month"] = df["date"].dt.to_period("M").astype(str)

   
    monthly = df.groupby("month")["revenue"].sum().reset_index()
    region_data = df.groupby("region")["revenue"].sum().reset_index()
    product_data = df.groupby("product")["profit"].sum().reset_index()

    
    sales_rep_data = df.groupby("sales_rep")["revenue"].sum().reset_index()

    
    channel_data = df.groupby("channel")["revenue"].sum().reset_index()

    return jsonify({
        "kpis": {
            "total_revenue": float(total_revenue),
            "total_profit": float(total_profit),
            "total_units": int(total_units),
            "deal_close_rate": deal_close_rate
        },
        "monthly_revenue": {
            "months": monthly["month"].tolist(),
            "revenue": monthly["revenue"].tolist()
        },
        "region_revenue": {
            "regions": region_data["region"].tolist(),
            "revenue": region_data["revenue"].tolist()
        },
        "product_profit": {
            "products": product_data["product"].tolist(),
            "profit": product_data["profit"].tolist()
        },
        
        "sales_rep_performance": {
            "reps": sales_rep_data["sales_rep"].tolist(),
            "revenue": sales_rep_data["revenue"].tolist()
        },
        "channel_performance": {
            "channels": channel_data["channel"].tolist(),
            "revenue": channel_data["revenue"].tolist()
        }
    })

if __name__ == "__main__":
    app.run(debug=True, port=5000)