import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse, urlunparse

from flask import Flask, render_template, request

from crawler.crawler import VERIFY_SSL as DEFAULT_VERIFY_SSL, crawl_and_download
from elasticsearch_index.es_index import create_index, index_multiple, search_pdfs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "change-me")
app.config["DOWNLOAD_DIR"] = Path(os.getenv("DOWNLOAD_DIR", "./downloaded_pdfs")).resolve()
app.config["VERIFY_SSL_DEFAULT"] = DEFAULT_VERIFY_SSL

ES_AVAILABLE = False
try:
    create_index()
except RuntimeError as exc:
    logger.warning("Elasticsearch unavailable at startup: %s", exc)
else:
    ES_AVAILABLE = True


def _initial_context() -> Dict[str, Any]:
    return {
        "message": None,
        "documents": [],
        "error": None,
        "indexing_error": None,
        "form_values": {
            "url": "",
            "max_pages": "",
            "max_pdfs": "",
            "ignore_ssl": not app.config["VERIFY_SSL_DEFAULT"],
        },
    }



ES_AVAILABLE = False
try:
    create_index()
except RuntimeError as exc:
    logger.warning("Elasticsearch unavailable at startup: %s", exc)
else:
    ES_AVAILABLE = True


def _initial_context() -> Dict[str, Any]:
    return {
        "message": None,
        "documents": [],
        "error": None,
        "indexing_error": None,
    }



def _store_context(context: Dict[str, Any]) -> Dict[str, Any]:
    app.config["LAST_CONTEXT"] = context
    return context


@app.route("/")
def index():
    context = _initial_context()
    context.update(app.config.get("LAST_CONTEXT", {}))
    return render_template("index.html", **context)


def _normalize_start_url(raw_url: str) -> str:
    """Return a fully qualified URL for crawling."""

    parsed = urlparse(raw_url)
    if not parsed.scheme:
        # Default to HTTP for intranet/offline sites unless a scheme is provided.
        parsed = parsed._replace(scheme="http")
    if not parsed.netloc:
        # In case the user passed only a hostname without scheme.
        parsed = urlparse(f"{parsed.scheme}://{parsed.path}")
    if not parsed.netloc:
        raise ValueError("A valid hostname is required to start crawling.")
    return urlunparse(parsed)


def _parse_limit(value: str, field_name: str) -> Optional[int]:
    """Convert a form value into an optional positive integer."""

    value = value.strip()
    if not value:
        return None

    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer") from exc

    if parsed <= 0:
        raise ValueError(f"{field_name} must be greater than 0")

    return parsed



def _parse_checkbox(value: Optional[str]) -> bool:
    if value is None:
        return False

    normalized = value.strip().lower()
    return normalized in {"1", "true", "on", "yes"}



def _format_downloaded_documents(documents: list) -> list:
    formatted = []
    for doc in documents:
        path = Path(doc["path"])
        size_kb = None
        if path.exists():
            size_kb = round(path.stat().st_size / 1024, 2)

        downloaded_at = doc.get("downloaded_at")
        readable_timestamp = downloaded_at
        if downloaded_at:
            try:
                parsed = datetime.fromisoformat(downloaded_at.replace("Z", "+00:00"))
                readable_timestamp = parsed.strftime("%Y-%m-%d %H:%M:%S %Z") or downloaded_at
            except ValueError:
                readable_timestamp = downloaded_at

        formatted.append(
            {
                **doc,
                "filename": path.name if path.name else doc.get("filename"),
                "size_kb": size_kb,
                "downloaded_display": readable_timestamp,
            }
        )

    return formatted


@app.post("/start_scraping")
def start_scraping():
    global ES_AVAILABLE

    website_url = request.form.get("url", "").strip()
    max_pages_raw = request.form.get("max_pages", "").strip()
    max_pdfs_raw = request.form.get("max_pdfs", "").strip()
    ignore_ssl = _parse_checkbox(request.form.get("ignore_ssl"))
    form_values = {
        "url": website_url,
        "max_pages": max_pages_raw,
        "max_pdfs": max_pdfs_raw,
        "ignore_ssl": ignore_ssl,
    }

    if not website_url:

        context = _initial_context()
        context["error"] = "A website URL is required."
        context["form_values"].update(form_values)
        context = _store_context(context)

        context = _store_context(
            {
                **_initial_context(),
                "error": "A website URL is required.",
            }
        )

        return render_template("index.html", **context), 400

    try:
        start_url = _normalize_start_url(website_url)

        max_pages = _parse_limit(max_pages_raw, "Maximum pages")
        max_pdfs = _parse_limit(max_pdfs_raw, "Maximum PDFs")
    except ValueError as exc:
        context = _initial_context()
        context["error"] = str(exc)
        context["form_values"].update(form_values)
        context = _store_context(context)

        max_pages = _parse_limit(request.form.get("max_pages", ""), "Maximum pages")
        max_pdfs = _parse_limit(request.form.get("max_pdfs", ""), "Maximum PDFs")
    except ValueError as exc:
        context = _store_context({**_initial_context(), "error": str(exc)})

        return render_template("index.html", **context), 400

    download_folder: Path = app.config["DOWNLOAD_DIR"]
    download_folder.mkdir(parents=True, exist_ok=True)

    allowed_hosts = {urlparse(start_url).netloc}
    verify_ssl = not ignore_ssl

    downloaded_documents = crawl_and_download(
        start_url,
        download_folder,
        allowed_hosts=allowed_hosts,
        max_pages=max_pages,
        max_pdfs=max_pdfs,

        verify_ssl=verify_ssl,


    )
    documents = _format_downloaded_documents(downloaded_documents)

    indexing_error = None
    indexed_count = 0

    if documents:
        if not ES_AVAILABLE:
            try:
                create_index()
            except RuntimeError as exc:
                indexing_error = f"Indexing skipped: {exc}"
            else:
                ES_AVAILABLE = True

        if ES_AVAILABLE and indexing_error is None:
            try:
                indexed_count = index_multiple(downloaded_documents)
            except RuntimeError as exc:
                indexing_error = str(exc)
                ES_AVAILABLE = False

    message = {
        "website_url": start_url,
        "downloaded": len(downloaded_documents),
        "indexed": indexed_count,
        "max_pages": max_pages,
        "max_pdfs": max_pdfs,

        "verify_ssl": verify_ssl,
    }

    form_values.update({"url": start_url})

    context = _initial_context()
    context.update(

    }

    context = _store_context(

        {
            "message": message,
            "documents": documents,
            "error": None,
            "indexing_error": indexing_error,
        }
    )

    context["form_values"].update(form_values)
    context = _store_context(context)


    return render_template("index.html", **context)


@app.route("/search", methods=["GET", "POST"])
def search():
    global ES_AVAILABLE
    query = request.values.get("query", "").strip()
    results = []
    error = None

    if query:
        if not ES_AVAILABLE:
            try:
                create_index()
            except RuntimeError as exc:
                error = f"Search unavailable: {exc}"
            else:
                ES_AVAILABLE = True

        if error is None:
            try:
                results = search_pdfs(query)
            except RuntimeError as exc:
                error = f"Search unavailable: {exc}"
                ES_AVAILABLE = False

    return render_template("search_results.html", query=query, results=results, error=error)


if __name__ == "__main__":
    app.run(debug=True)
