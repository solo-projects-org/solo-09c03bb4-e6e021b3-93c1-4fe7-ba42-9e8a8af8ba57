"""FastAPI application and HTML UI."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, Request, UploadFile
from fastapi.responses import (
    FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse,
)
from fastapi.templating import Jinja2Templates

from .config import Settings
from .csvparse import BatchRejected, ParseError
from .service import AppService, ConflictError, format_utc
from .worker import ReportWorker

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def create_app(settings: Settings | None = None, start_worker: bool = True) -> FastAPI:
    settings = settings or Settings.from_env()
    service = AppService(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        worker = None
        if start_worker:
            worker = ReportWorker(service, settings.worker_poll_seconds)
            worker.start()
            app.state.worker = worker
        try:
            yield
        finally:
            if worker is not None:
                worker.stop()
                worker.join(timeout=10)

    app = FastAPI(title="交易修订审核时点特征", docs_url="/api/docs",
                  redoc_url=None, lifespan=lifespan)
    app.state.settings = settings
    app.state.service = service
    app.state.worker = None

    TEMPLATES.env.filters["ts"] = format_utc

    @app.exception_handler(BatchRejected)
    async def _batch_rejected(_: Request, exc: BatchRejected):
        return JSONResponse(status_code=422, content=exc.payload())

    @app.exception_handler(ParseError)
    async def _parse_error(_: Request, exc: ParseError):
        return JSONResponse(status_code=400, content=exc.payload())

    # ------------------------------------------------------------------ #
    # HTML pages
    # ------------------------------------------------------------------ #
    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        summary = service.batch_summary()
        return TEMPLATES.TemplateResponse(request, "index.html", {
            "request": request,
            "batches": summary["batches"],
            "reports": service.list_reports(),
            "error": request.query_params.get("error"),
            "notice": request.query_params.get("notice"),
            "tz": "UTC",
        })

    @app.get("/reports/{public_id}", response_class=HTMLResponse)
    def report_page(request: Request, public_id: str):
        preview = service.preview_report(public_id, settings.preview_max_rows)
        if preview is None:
            return HTMLResponse("报告不存在", status_code=404)
        return TEMPLATES.TemplateResponse(request, "report.html", {
            "request": request,
            "r": preview["report"],
            "headers": preview["headers"],
            "rows": preview["rows"],
            "total": preview["total"],
            "truncated": preview["total"] > len(preview["rows"]),
        })

    @app.get("/reports/{public_id}/download")
    def download(public_id: str):
        found = service.report_file(public_id)
        if found is None:
            return PlainTextResponse(
                "报告尚未就绪或文件校验失败，不能下载。", status_code=409)
        path, _digest = found
        return FileResponse(
            path, media_type="text/csv; charset=utf-8",
            filename=f"training_features_{public_id[:12]}.csv",
            headers={"Cache-Control": "no-store",
                     "X-Content-SHA256": _digest},
        )

    # ------------------------------------------------------------------ #
    # Form actions (browser)
    # ------------------------------------------------------------------ #
    @app.post("/upload-transactions")
    async def upload_transactions_form(file: UploadFile):
        try:
            data = await _read_limited(file, settings.max_upload_bytes)
            result = service.upload_transactions(file.filename or "transactions.csv", data)
            return RedirectResponse(
                f"/?notice={quote('交易批次 #' + str(result['batch_id']) + ' 已入库，共 ' + str(result['rows']) + ' 行')}",
                status_code=303)
        except ParseError as exc:
            return RedirectResponse(
                f"/?error={quote(exc.message)}", status_code=303)

    @app.post("/upload-samples")
    async def upload_samples_form(file: UploadFile):
        try:
            data = await _read_limited(file, settings.max_upload_bytes)
            result = service.upload_samples(file.filename or "samples.csv", data)
            return RedirectResponse(
                f"/?notice={quote('样本批次 #' + str(result['batch_id']) + ' 已入库，共 ' + str(result['rows']) + ' 行')}",
                status_code=303)
        except ParseError as exc:
            return RedirectResponse(
                f"/?error={quote(exc.message)}", status_code=303)

    @app.post("/reports")
    def create_report_form():
        try:
            result = service.create_report()
            return RedirectResponse(f"/reports/{result['report_id']}", status_code=303)
        except ConflictError as exc:
            return RedirectResponse(f"/?error={quote(exc.message)}", status_code=303)

    @app.post("/reports/{public_id}/regenerate")
    def regenerate_form(public_id: str):
        try:
            result = service.regenerate_report(public_id)
            return RedirectResponse(f"/reports/{result['report_id']}", status_code=303)
        except LookupError:
            return RedirectResponse("/?error=报告不存在", status_code=303)
        except ConflictError as exc:
            return RedirectResponse(f"/?error={quote(exc.message)}", status_code=303)

    # ------------------------------------------------------------------ #
    # JSON API
    # ------------------------------------------------------------------ #
    @app.post("/api/uploads/transactions")
    async def api_upload_transactions(file: UploadFile):
        data = await _read_limited(file, settings.max_upload_bytes)
        return service.upload_transactions(file.filename or "transactions.csv", data)

    @app.post("/api/uploads/samples")
    async def api_upload_samples(file: UploadFile):
        data = await _read_limited(file, settings.max_upload_bytes)
        return service.upload_samples(file.filename or "samples.csv", data)

    @app.get("/api/batches")
    def api_batches():
        return service.batch_summary()

    @app.post("/api/reports")
    def api_create_report():
        try:
            return service.create_report()
        except ConflictError as exc:
            return JSONResponse(status_code=409,
                                content={"error": "not_ready", "message": exc.message})

    @app.get("/api/reports")
    def api_reports():
        return {"reports": service.list_reports()}

    @app.get("/api/reports/{public_id}")
    def api_report(public_id: str):
        report = service.get_report(public_id)
        if report is None:
            return JSONResponse(status_code=404,
                                content={"error": "not_found"})
        return report

    @app.get("/api/reports/{public_id}/download")
    def api_download(public_id: str):
        return download(public_id)

    @app.post("/api/reports/{public_id}/regenerate")
    def api_regenerate(public_id: str):
        try:
            return service.regenerate_report(public_id)
        except LookupError:
            return JSONResponse(status_code=404, content={"error": "not_found"})
        except ConflictError as exc:
            return JSONResponse(status_code=409,
                                content={"error": "conflict", "message": exc.message})

    @app.get("/healthz", response_class=PlainTextResponse)
    def healthz():
        return "ok"

    return app


async def _read_limited(file: UploadFile, limit: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await file.read(1 << 20)
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            raise BatchRejected(
                "上传文件超过大小限制。",
                [{"row": 0, "field": "", "code": "too_large",
                  "message": f"上限 {limit} 字节。", "value": ""}])
        chunks.append(chunk)
    if total == 0:
        raise BatchRejected(
            "上传文件为空。",
            [{"row": 0, "field": "", "code": "empty", "message": "0 字节。",
              "value": ""}])
    return b"".join(chunks)

# The container uses the factory form: uvicorn app.main:create_app --factory
