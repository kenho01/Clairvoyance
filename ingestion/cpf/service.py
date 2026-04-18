from __future__ import annotations

import os
import tempfile
from pathlib import Path

from flask import Flask, jsonify, request

from ingestion.cpf.pipeline import run

app = Flask(__name__)


@app.route("/", methods=["POST"])
def handle_event():
    envelope = request.get_json(silent=True) or {}

    # Eventarc wraps GCS event data under a "data" key
    data   = envelope.get("data", envelope)
    bucket = data.get("bucket", "")
    name   = data.get("name", "")

    if not bucket or not name:
        return jsonify({"error": "Missing bucket or name in event"}), 400

    if not name.lower().endswith(".pdf"):
        return jsonify({"skipped": f"{name} is not a PDF"}), 200

    print(f"Processing gs://{bucket}/{name}")

    from google.cloud import storage
    gcs_client = storage.Client(project=os.environ.get("GCP_PROJECT_ID"))
    blob = gcs_client.bucket(bucket).blob(name)

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        blob.download_to_filename(tmp.name)
        pdf_path = Path(tmp.name)

    try:
        run(
            pdf_path=pdf_path,
            bucket_name=os.environ.get("GCS_BUCKET_BANK", ""),
            project_id=os.environ.get("GCP_PROJECT_ID", ""),
        )
    finally:
        pdf_path.unlink(missing_ok=True)

    return jsonify({"status": "ok", "processed": name}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
