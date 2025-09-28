import logging
import os
from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .sf import (
    get_salesforce_client,
    list_report_folders,
    list_dashboard_folders,
    list_dashboards_in_folder,
    copy_report_folder,
    copy_dashboard_with_reports,
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


@app.post("/copy/report-folder")
def post_copy_report_folder(source_folder_id: str = Form(...), target_folder_name: str = Form(...)):
    copy_report_folder(source_folder_id=source_folder_id, target_folder_name=target_folder_name)
    return RedirectResponse(url="/", status_code=303)


@app.post("/copy/dashboard")
def post_copy_dashboard(
    dashboard_folder_id: str = Form(...),
    dashboard_developer_name: str = Form(...),
    target_folder_name: str = Form(...),
):
    copy_dashboard_with_reports(
        source_dashboard_folder_id=dashboard_folder_id,
        source_dashboard_developer_name=dashboard_developer_name,
        target_folder_name=target_folder_name,
    )
    return RedirectResponse(url="/", status_code=303)


