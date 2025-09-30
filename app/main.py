import logging
import os
from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .sf import (
    get_salesforce_client,
    list_report_folders,
    list_dashboard_folders,
    list_dashboards_in_folder,
    list_reports_in_folder,
    copy_report_folder,
    copy_dashboard_with_reports,
    prepare_report_copy,
    prepare_selected_reports_copy,
    prepare_dashboard_copy,
    start_deploy,
    get_deploy_status,
)


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

app = FastAPI(title="SF Reports & Dashboards Copier")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/")
def index(request: Request):
    try:
        report_folders = list_report_folders()
        dashboard_folders = list_dashboard_folders()
        error_message = None
    except Exception as exc:  # minimal UX: show error banner
        report_folders = []
        dashboard_folders = []
        error_message = str(exc)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "report_folders": report_folders,
            "dashboard_folders": dashboard_folders,
            "dashboards": [],
            "error_message": error_message,
        },
    )


@app.get("/dashboards")
def dashboards_by_folder(request: Request, folder_id: str):
    report_folders = list_report_folders()
    dashboard_folders = list_dashboard_folders()
    dashboards = list_dashboards_in_folder(folder_id)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "report_folders": report_folders,
            "dashboard_folders": dashboard_folders,
            "dashboards": dashboards,
            "selected_dashboard_folder_id": folder_id,
        },
    )


# JSON API endpoints for browser tables
@app.get("/api/reports")
def api_list_reports(folder_id: str):
    try:
        items = list_reports_in_folder(folder_id)
        return JSONResponse(items)
    except Exception as exc:
        logging.exception("Failed to list reports for folder %s", folder_id)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/dashboards")
def api_list_dashboards(folder_id: str):
    try:
        items = list_dashboards_in_folder(folder_id)
        return JSONResponse(items)
    except Exception as exc:
        logging.exception("Failed to list dashboards for folder %s", folder_id)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/folders")
def api_list_folders(kind: str):
    try:
        if kind == "report":
            items = list_report_folders()
        elif kind == "dashboard":
            items = list_dashboard_folders()
        else:
            return JSONResponse({"error": "invalid_kind"}, status_code=400)
        # Normalize shape
        out = [{"Id": f.get("Id"), "Name": f.get("Name"), "DeveloperName": f.get("DeveloperName")} for f in items or []]
        return JSONResponse(out)
    except Exception as exc:
        logging.exception("Failed to list folders for kind %s", kind)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.post("/copy/report-folder")
def post_copy_report_folder(source_folder_id: str = Form(...), target_folder_name: str = Form(...)):
    copy_report_folder(source_folder_id=source_folder_id, target_folder_name=target_folder_name)
    return RedirectResponse(url="/", status_code=303)


@app.post("/copy/dashboard")
def post_copy_dashboard(
    dashboard_folder_id: str = Form(...),
    dashboard_developer_name: str = Form(...),
    target_dashboard_folder_name: str = Form(...),
    target_report_folder_name: str = Form(...),
):
    copy_dashboard_with_reports(
        source_dashboard_folder_id=dashboard_folder_id,
        source_dashboard_developer_name=dashboard_developer_name,
        target_dashboard_folder_name=target_dashboard_folder_name,
        target_report_folder_name=target_report_folder_name,
    )
    return RedirectResponse(url="/", status_code=303)


# New prepare endpoints
@app.post("/prepare/report-folder")
def prepare_report_folder(request: Request, source_folder_id: str = Form(...), target_folder_name: str = Form(...)):
    data = prepare_report_copy(source_folder_id=source_folder_id, target_folder_name=target_folder_name)
    report_folders = list_report_folders()
    dashboard_folders = list_dashboard_folders()
    return templates.TemplateResponse(
        "review.html",
        {
            "request": request,
            "kind": "report",
            "members": data.get("members", []),
            "package_xml": data.get("package_xml", ""),
            "zip_path": data.get("zip_path", ""),
            "target_folder_devname": data.get("target_folder_devname", ""),
            "report_folders": report_folders,
            "dashboard_folders": dashboard_folders,
        },
    )


@app.post("/prepare/reports-selected")
def prepare_reports_selected(
    request: Request,
    source_folder_id: str = Form(...),
    target_folder_name: str = Form(...),
    report_ids: list[str] = Form(default=[]),
):
    data = prepare_selected_reports_copy(
        source_folder_id=source_folder_id,
        selected_report_ids=report_ids,
        target_folder_name=target_folder_name,
    )
    report_folders = list_report_folders()
    dashboard_folders = list_dashboard_folders()
    return templates.TemplateResponse(
        "review.html",
        {
            "request": request,
            "kind": "report",
            "members": data.get("members", []),
            "package_xml": data.get("package_xml", ""),
            "zip_path": data.get("zip_path", ""),
            "target_folder_devname": data.get("target_folder_devname", ""),
            "report_folders": report_folders,
            "dashboard_folders": dashboard_folders,
        },
    )


@app.post("/prepare/dashboard")
def prepare_dashboard(request: Request,
    dashboard_folder_id: str = Form(...),
    dashboard_developer_name: str = Form(...),
    target_dashboard_folder_name: str = Form(...),
    target_report_folder_name: str = Form(...),
):
    data = prepare_dashboard_copy(
        source_dashboard_folder_id=dashboard_folder_id,
        source_dashboard_developer_name=dashboard_developer_name,
        target_dashboard_folder_name=target_dashboard_folder_name,
        target_report_folder_name=target_report_folder_name,
    )
    report_folders = list_report_folders()
    dashboard_folders = list_dashboard_folders()
    return templates.TemplateResponse(
        "review.html",
        {
            "request": request,
            "kind": "dashboard",
            "members_reports": data.get("members_reports", []),
            "member_dashboard": data.get("member_dashboard", ""),
            "package_xml": data.get("package_xml", ""),
            "zip_path": data.get("zip_path", ""),
            "target_dashboard_devname": data.get("target_dashboard_devname", ""),
            "target_report_devname": data.get("target_report_devname", ""),
            "report_folders": report_folders,
            "dashboard_folders": dashboard_folders,
        },
    )


# Deploy endpoints
@app.post("/deploy/start")
def deploy_start(request: Request, zip_path: str = Form(...)):
    job_id = start_deploy(zip_path)
    return templates.TemplateResponse(
        "deploy_progress.html",
        {"request": request, "job_id": job_id},
    )


@app.get("/deploy/status")
def deploy_status(job_id: str):
    return get_deploy_status(job_id)


