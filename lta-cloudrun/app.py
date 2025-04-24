# app.py
from flask import Flask, request, jsonify
import numpy as np
from tensorflow.keras.models import load_model
import joblib
import os
import pandas as pd

app = Flask(__name__)

# Load model from GCS mounted bucket or local file system
MODEL_PATH = "gs://yourfolder/lstm_model_x1.keras"
model = load_model(MODEL_PATH)

# Predict route
@app.route("/predict", methods=["POST"])
def predict():
    try:
        data = request.get_json()
        sequences = data.get("sequences", [])  # Expect list of lists with 10 elements each
        ids = data.get("carpark_ids", [])

        predictions = []
        for seq, carpark_id in zip(sequences, ids):
            input_array = np.array(seq, dtype=np.float32).reshape(1, -1, 1)
            predicted = model.predict(input_array)[0][0]

            # Apply constraints
            predicted = max(0, round(predicted))

            predictions.append({
                "carpark_id": carpark_id,
                "predicted_lots": predicted
            })

        return jsonify(predictions)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))  # ✅ Cloud Run expects 8080
    app.run(host="0.0.0.0", port=port)

