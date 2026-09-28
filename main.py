import asyncio
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from PySide6.QtCore import QThread, Signal, Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication, QComboBox, QGroupBox, QHBoxLayout, QLabel, QLineEdit,
    QMainWindow, QMessageBox, QPlainTextEdit, QPushButton, QSplitter,
    QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget
)
from playwright.async_api import async_playwright


BASE_DIR = Path(__file__).resolve().parent
TRACE_FILE = BASE_DIR / "pega_api_trace.jsonl"


def redact_headers(headers):
    sensitive = {
        "authorization", "proxy-authorization", "cookie", "set-cookie",
        "x-api-key", "api-key"
    }
    return {
        str(k): "[REDACTED]" if str(k).lower() in sensitive else str(v)
        for k, v in headers.items()
    }


def safe_body(value, limit=2_000_000):
    if value is None:
        return ""

    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            try:
                value = value.decode("latin-1")
            except Exception:
                return "[binary body omitted]"

    value = str(value)

    if "\x00" in value:
        return "[binary body omitted]"

    if len(value) > limit:
        return value[:limit] + "\n[body truncated]"

    return value


def classify_call(url, req_headers, res_headers, resource_type):
    u = url.lower()
    request_text = json.dumps(req_headers).lower()
    response_text = json.dumps(res_headers).lower()
    combined = f"{u} {request_text} {response_text}"

    if (
        "soap" in combined
        or "xml" in combined
        or "wsdl" in u
        or resource_type in ("xhr", "fetch") and (
            "text/xml" in combined or "application/xml" in combined
        )
    ):
        return "SOAP/XML"

    if (
        "json" in combined
        or "prrestservice" in u
        or "/prweb/api/" in u
        or resource_type in ("xhr", "fetch")
    ):
        return "REST/JSON"

    return "HTTP"


def service_from_url(url):
    parsed = urlparse(url)
    path = parsed.path or "/"

    patterns = [
        r"(PRRestService/[^/?#]+)",
        r"(PRServlet/[^/?#]+)",
        r"([^/?#]*Service[^/?#]*)",
    ]

    for pattern in patterns:
        match = re.search(pattern, path, re.IGNORECASE)
        if match:
            return match.group(1)

    return path.strip("/") or parsed.netloc


class BrowserWorker(QThread):
    record = Signal(dict)
    status = Signal(str)
    error = Signal(str)

    def __init__(self, start_url=""):
        super().__init__()
        self.start_url = start_url
        self.loop = None
        self.stop_event = None
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.pending = {}
        self.started = False

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.stop_event = asyncio.Event()

        try:
            self.loop.run_until_complete(self.async_main())
        except Exception as exc:
            self.error.emit(f"{type(exc).__name__}: {exc}")
        finally:
            try:
                self.loop.close()
            except Exception:
                pass

    async def async_main(self):
        self.status.emit("Starting Playwright...")

        self.playwright = await async_playwright().start()

        self.browser = await self.playwright.chromium.launch(
            headless=False,
            args=[
                "--disable-popup-blocking",
            ],
        )

        self.context = await self.browser.new_context(
            ignore_https_errors=False
        )

        # Context-level listeners catch requests from all pages/iframes
        # belonging to this browser context.
        self.context.on("request", self.on_request)
        self.context.on("response", self.on_response)
        self.context.on("requestfailed", self.on_request_failed)

        self.page = await self.context.new_page()

        self.page.on("request", self.on_request)
        self.page.on("response", self.on_response)
        self.page.on("requestfailed", self.on_request_failed)

        self.started = True
        self.status.emit("Browser started - tracing network calls")

        if self.start_url:
            try:
                await self.page.goto(
                    self.start_url,
                    wait_until="domcontentloaded",
                    timeout=60000
                )
            except Exception as exc:
                self.status.emit(
                    f"Browser started; navigation warning: {exc}"
                )

        await self.stop_event.wait()

        await self.cleanup()

    def on_request(self, request):
        if not self.started:
            return

        try:
            headers = redact_headers(request.headers)

            self.pending[request] = {
                "started": time.perf_counter(),
                "method": request.method,
                "url": request.url,
                "resource_type": request.resource_type,
                "request_headers": headers,
                "request_body": safe_body(request.post_data),
                "frame_url": self.safe_frame_url(request),
            }
        except Exception:
            pass

    async def on_response(self, response):
        if not self.started:
            return

        request = response.request
        data = self.pending.pop(request, None)

        if data is None:
            data = {
                "started": time.perf_counter(),
                "method": request.method,
                "url": request.url,
                "resource_type": request.resource_type,
                "request_headers": redact_headers(request.headers),
                "request_body": safe_body(request.post_data),
                "frame_url": self.safe_frame_url(request),
            }

        try:
            response_headers = redact_headers(response.headers)
        except Exception:
            response_headers = {}

        duration = (time.perf_counter() - data["started"]) * 1000

        response_body = ""
        body_error = ""

        # API traffic normally appears as XHR/fetch.
        # We also attempt bodies for document responses when practical.
        if request.resource_type in {"xhr", "fetch"}:
            try:
                response_body = safe_body(await response.body())
            except Exception as exc:
                body_error = str(exc)

        record = {
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "method": data["method"],
            "url": data["url"],
            "host": urlparse(data["url"]).netloc,
            "service": service_from_url(data["url"]),
            "type": classify_call(
                data["url"],
                data["request_headers"],
                response_headers,
                data["resource_type"],
            ),
            "status": response.status,
            "status_text": response.status_text,
            "duration_ms": round(duration, 1),
            "resource_type": data["resource_type"],
            "frame_url": data["frame_url"],
            "request_headers": data["request_headers"],
            "response_headers": response_headers,
            "request_body": data["request_body"],
            "response_body": response_body,
            "response_body_error": body_error,
        }

        self.record.emit(record)

    def on_request_failed(self, request):
        if not self.started:
            return

        data = self.pending.pop(request, None)

        if data is None:
            data = {
                "started": time.perf_counter(),
                "method": request.method,
                "url": request.url,
                "resource_type": request.resource_type,
                "request_headers": redact_headers(request.headers),
                "request_body": safe_body(request.post_data),
                "frame_url": self.safe_frame_url(request),
            }

        failure = request.failure or "request failed"

        record = {
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "method": data["method"],
            "url": data["url"],
            "host": urlparse(data["url"]).netloc,
            "service": service_from_url(data["url"]),
            "type": "ERROR",
            "status": 0,
            "status_text": "",
            "duration_ms": round(
                (time.perf_counter() - data["started"]) * 1000, 1
            ),
            "resource_type": data["resource_type"],
            "frame_url": data["frame_url"],
            "request_headers": data["request_headers"],
            "response_headers": {},
            "request_body": data["request_body"],
            "response_body": "",
            "response_body_error": str(failure),
        }

        self.record.emit(record)

    @staticmethod
    def safe_frame_url(request):
        try:
            frame = request.frame
            return frame.url if frame else ""
        except Exception:
            return ""

    def request_stop(self):
        if self.loop and self.stop_event:
            self.loop.call_soon_threadsafe(self.stop_event.set)

    async def cleanup(self):
        self.status.emit("Stopping browser...")

        try:
            if self.browser:
                await self.browser.close()
        except Exception:
            pass

        try:
            if self.playwright:
                await self.playwright.stop()
        except Exception:
            pass

        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None
        self.started = False

        self.status.emit("Stopped")


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Pega API Tracer - Playwright")
        self.resize(1650, 950)

        self.records = []
        self.worker = None
        self.filtered_records = []

        self.build_ui()

    def build_ui(self):
        root = QWidget()
        main_layout = QVBoxLayout(root)

        # Top controls
        controls = QHBoxLayout()

        controls.addWidget(QLabel("Pega URL:"))

        self.url_edit = QLineEdit()
        self.url_edit.setPlaceholderText(
            "https://your-pega-server/prweb/..."
        )
        controls.addWidget(self.url_edit, 1)

        self.start_button = QPushButton("▶ Start Browser + Trace")
        self.start_button.clicked.connect(self.start_trace)
        controls.addWidget(self.start_button)

        self.stop_button = QPushButton("■ Stop")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_trace)
        controls.addWidget(self.stop_button)

        self.clear_button = QPushButton("Clear")
        self.clear_button.clicked.connect(self.clear_trace)
        controls.addWidget(self.clear_button)

        self.export_button = QPushButton("Export JSON")
        self.export_button.clicked.connect(self.export_trace)
        controls.addWidget(self.export_button)

        main_layout.addLayout(controls)

        # Filters
        filters = QHBoxLayout()

        filters.addWidget(QLabel("Search:"))

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText(
            "URL, service, host, PRRestService, SOAP, request body..."
        )
        self.search_edit.textChanged.connect(self.refresh_table)
        filters.addWidget(self.search_edit, 1)

        filters.addWidget(QLabel("Type:"))

        self.type_combo = QComboBox()
        self.type_combo.addItems([
            "All",
            "REST/JSON",
            "SOAP/XML",
            "HTTP",
            "ERROR",
        ])
        self.type_combo.currentTextChanged.connect(self.refresh_table)
        filters.addWidget(self.type_combo)

        filters.addWidget(QLabel("Resource:"))

        self.resource_combo = QComboBox()
        self.resource_combo.addItems([
            "All",
            "xhr",
            "fetch",
            "document",
            "script",
            "stylesheet",
            "image",
            "font",
            "other",
        ])
        self.resource_combo.currentTextChanged.connect(self.refresh_table)
        filters.addWidget(self.resource_combo)

        self.status_label = QLabel("● Stopped")
        filters.addWidget(self.status_label)

        main_layout.addLayout(filters)

        # Network table
        self.table = QTableWidget(0, 10)
        self.table.setHorizontalHeaderLabels([
            "#",
            "Time",
            "Type",
            "Method",
            "Resource",
            "Host",
            "Service",
            "Status",
            "Duration",
            "URL",
        ])

        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.itemSelectionChanged.connect(self.show_selected)

        # Details
        self.request_box = self.make_editor("REQUEST")
        self.response_box = self.make_editor("RESPONSE")

        details = QSplitter(Qt.Horizontal)
        details.addWidget(self.request_box)
        details.addWidget(self.response_box)

        vertical = QSplitter(Qt.Vertical)
        vertical.addWidget(self.table)
        vertical.addWidget(details)
        vertical.setSizes([520, 400])

        main_layout.addWidget(vertical, 1)

        self.setCentralWidget(root)

    def make_editor(self, title):
        group = QGroupBox(title)
        layout = QVBoxLayout(group)

        editor = QPlainTextEdit()
        editor.setReadOnly(True)
        editor.setFont(QFont("Consolas", 10))

        layout.addWidget(editor)
        group.editor = editor

        return group

    def start_trace(self):
        if self.worker and self.worker.isRunning():
            return

        url = self.url_edit.text().strip()

        if not url:
            QMessageBox.warning(
                self,
                "Pega URL",
                "Enter the Pega application URL first."
            )
            return

        self.clear_trace()

        self.worker = BrowserWorker(url)
        self.worker.record.connect(self.add_record)
        self.worker.status.connect(self.set_status)
        self.worker.error.connect(self.worker_error)
        self.worker.finished.connect(self.worker_finished)

        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)

        self.worker.start()

    def stop_trace(self):
        if self.worker:
            self.worker.request_stop()

        self.stop_button.setEnabled(False)

    def worker_finished(self):
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)

    def worker_error(self, message):
        self.set_status("Error")
        QMessageBox.critical(self, "Playwright error", message)
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)

    def set_status(self, text):
        self.status_label.setText("● " + text)

    def add_record(self, record):
        self.records.append(record)

        try:
            with TRACE_FILE.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )
        except Exception:
            pass

        self.refresh_table()

    def clear_trace(self):
        self.records.clear()
        self.filtered_records.clear()

        try:
            TRACE_FILE.write_text("", encoding="utf-8")
        except Exception:
            pass

        self.table.setRowCount(0)
        self.request_box.editor.clear()
        self.response_box.editor.clear()

    def refresh_table(self):
        search = self.search_edit.text().strip().lower()
        type_filter = self.type_combo.currentText()
        resource_filter = self.resource_combo.currentText()

        self.filtered_records = []

        for record in self.records:
            if type_filter != "All":
                if record.get("type") != type_filter:
                    continue

            if resource_filter != "All":
                if record.get("resource_type") != resource_filter:
                    continue

            if search:
                searchable = " ".join([
                    str(record.get("url", "")),
                    str(record.get("host", "")),
                    str(record.get("service", "")),
                    str(record.get("method", "")),
                    str(record.get("status", "")),
                    str(record.get("resource_type", "")),
                    str(record.get("frame_url", "")),
                    str(record.get("request_body", "")),
                    str(record.get("response_body", "")),
                ]).lower()

                if search not in searchable:
                    continue

            self.filtered_records.append(record)

        self.table.setRowCount(0)

        for index, record in enumerate(self.filtered_records, 1):
            row = self.table.rowCount()
            self.table.insertRow(row)

            values = [
                index,
                record.get("time", ""),
                record.get("type", ""),
                record.get("method", ""),
                record.get("resource_type", ""),
                record.get("host", ""),
                record.get("service", ""),
                record.get("status", ""),
                f'{record.get("duration_ms", 0):.0f} ms',
                record.get("url", ""),
            ]

            for col, value in enumerate(values):
                self.table.setItem(
                    row,
                    col,
                    QTableWidgetItem(str(value))
                )

        if self.table.rowCount():
            self.table.scrollToBottom()

    def show_selected(self):
        selected = self.table.selectionModel().selectedRows()

        if not selected:
            return

        row = selected[0].row()

        if row >= len(self.filtered_records):
            return

        record = self.filtered_records[row]

        request_text = [
            f'{record.get("method", "")} {record.get("url", "")}',
            "",
            f'TYPE: {record.get("type", "")}',
            f'RESOURCE: {record.get("resource_type", "")}',
            f'HOST: {record.get("host", "")}',
            f'SERVICE: {record.get("service", "")}',
            f'FRAME: {record.get("frame_url", "")}',
            "",
            "REQUEST HEADERS",
            "==============================",
            self.format_headers(record.get("request_headers", {})),
            "",
            "REQUEST BODY",
            "==============================",
            record.get("request_body", ""),
        ]

        response_text = [
            f'HTTP {record.get("status", "")} '
            f'{record.get("status_text", "")}',
            "",
            f'DURATION: {record.get("duration_ms", 0)} ms',
            "",
            "RESPONSE HEADERS",
            "==============================",
            self.format_headers(record.get("response_headers", {})),
            "",
            "RESPONSE BODY",
            "==============================",
            record.get("response_body", ""),
            "",
            "BODY ERROR",
            "==============================",
            record.get("response_body_error", ""),
        ]

        self.request_box.editor.setPlainText(
            "\n".join(request_text)
        )
        self.response_box.editor.setPlainText(
            "\n".join(response_text)
        )

    @staticmethod
    def format_headers(headers):
        if not headers:
            return "(none)"

        return "\n".join(
            f"{key}: {value}"
            for key, value in headers.items()
        )

    def export_trace(self):
        if not self.records:
            QMessageBox.information(
                self,
                "Export",
                "No network calls have been captured."
            )
            return

        output = BASE_DIR / "pega_api_trace_export.json"

        output.write_text(
            json.dumps(
                self.records,
                indent=2,
                ensure_ascii=False
            ),
            encoding="utf-8"
        )

        QMessageBox.information(
            self,
            "Export complete",
            f"Saved:\n{output}"
        )

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            self.worker.request_stop()
            self.worker.wait(10000)

        event.accept()


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
