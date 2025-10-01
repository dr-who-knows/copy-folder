import os
import logging
import uuid
import tempfile
import zipfile
from io import BytesIO
from typing import Dict, List, Tuple, Set

from dotenv import load_dotenv

# salesforce_api provides both data APIs and metadata deploy/retrieve
from salesforce_api import Salesforce
from salesforce_api.models.shared import Type as SfType


logger = logging.getLogger(__name__)

_SF_CLIENT: Salesforce | None = None
_DEPLOY_JOBS: Dict[str, object] = {}


def get_salesforce_client() -> Salesforce:
    global _SF_CLIENT
    if _SF_CLIENT is not None:
        return _SF_CLIENT

    load_dotenv()

    username = os.getenv("SF_USERNAME")
    password = os.getenv("SF_PASSWORD")
    token = os.getenv("SF_SECURITY_TOKEN")
    is_sandbox = os.getenv("SF_IS_SANDBOX", "false").lower() == "true"
    domain = os.getenv("SF_DOMAIN")
    api_version = os.getenv("SF_API_VERSION")

    if not username or not password or not token:
        raise RuntimeError("Missing Salesforce creds in environment (.env)")

    client_kwargs: Dict[str, object] = {
        "username": username,
        "password": password,
        "security_token": token,
    }
    if domain:
        client_kwargs["domain"] = domain
    else:
        client_kwargs["is_sandbox"] = is_sandbox
    if api_version:
        client_kwargs["api_version"] = api_version

    logger.debug(
        "Initializing Salesforce client (domain=%s, is_sandbox=%s, api_version=%s)",
        client_kwargs.get("domain"),
        client_kwargs.get("is_sandbox"),
        client_kwargs.get("api_version"),
    )
    _SF_CLIENT = Salesforce(**client_kwargs)
    return _SF_CLIENT


def list_report_folders() -> List[Dict[str, str]]:
    sf = get_salesforce_client()
    soql = "SELECT Id, Name, DeveloperName FROM Folder WHERE Type = 'Report' ORDER BY Name"
    res = sf.sobjects.query(soql)
    # Filter out folders with null or literal "(null)" names
    filtered = []
    for r in res or []:
        name = r.get("Name")
        dev = r.get("DeveloperName")
        if not name:
            continue
        if isinstance(name, str) and name.strip().lower() == "(null)":
            continue
        # Also skip if DeveloperName is missing (not actionable)
        if not dev:
            continue
        filtered.append(r)
    return filtered  # library returns list-like per docs


def list_dashboard_folders() -> List[Dict[str, str]]:
    sf = get_salesforce_client()
    soql = "SELECT Id, Name, DeveloperName FROM Folder WHERE Type = 'Dashboard' ORDER BY Name"
    res = sf.sobjects.query(soql)
    return res


def list_dashboards_in_folder(folder_id: str) -> List[Dict[str, str]]:
    # Use SOQL to list dashboards by Folder Name
    _, folder_name = _get_folder_devname_by_id(folder_id)
    sf = get_salesforce_client()
    safe_name = folder_name.replace("'", "\\'")
    soql = (
        "SELECT Id, DeveloperName, Title FROM Dashboard WHERE FolderName = '"
        + safe_name
        + "' ORDER BY Title"
    )
    rows = sf.sobjects.query(soql) or []
    dashboards: List[Dict[str, str]] = []
    for r in rows:
        dashboards.append({
            "Id": r.get("Id", ""),
            "DeveloperName": r.get("DeveloperName", ""),
            "Title": r.get("Title", r.get("DeveloperName", "")),
        })
    return dashboards


def list_reports_in_folder(folder_id: str) -> List[Dict[str, str]]:
    """Public wrapper to list reports in a given folder by Id.

    Returns items with keys: Id, name, developerName
    """
    return _list_folder_items(folder_id, "Report")


def _get_folder_devname_by_id(folder_id: str) -> Tuple[str, str]:
    sf = get_salesforce_client()
    soql = f"SELECT Id, Name, DeveloperName FROM Folder WHERE Id = '{folder_id}'"
    res = sf.sobjects.query(soql)
    if not res:
        raise RuntimeError("Folder not found")
    row = res[0]
    logger.info(
        "Resolved Folder by Id '%s' to DeveloperName='%s', Name='%s'",
        folder_id,
        row["DeveloperName"],
        row["Name"],
    )
    return row["DeveloperName"], row["Name"]


def _ensure_report_folder_exists(target_folder_name: str) -> str:
    sf = get_salesforce_client()
    logger.info("Ensuring Report folder exists: Name='%s'", target_folder_name)
    # Try to find by Name; if not exist, create Folder record
    soql = (
        "SELECT Id, Name, DeveloperName FROM Folder WHERE Type = 'Report' "
        f"AND Name = '{target_folder_name}'"
    )
    res = sf.sobjects.query(soql)
    if res:
        dev = res[0]["DeveloperName"]
        logger.info("Found existing Report folder: DeveloperName='%s'", dev)
        return dev

    # Deduplicate DeveloperName across all report folders in org
    existing_devnames_res = sf.sobjects.query(
        "SELECT DeveloperName FROM Folder WHERE Type = 'Report'"
    )
    existing_devnames: Set[str] = (
        {r["DeveloperName"] for r in existing_devnames_res} if existing_devnames_res else set()
    )
    base = _to_devname(target_folder_name)
    unique_devname = _dedupe_developer_name(base, existing_devnames)

    create_body = {
        "Name": target_folder_name,
        "DeveloperName": unique_devname,
        "Type": "Report",
        "AccessType": "Public",
    }
    logger.info(
        "Creating Report folder: Name='%s', DeveloperName='%s'",
        target_folder_name,
        unique_devname,
    )
    sf.sobjects.Folder.insert(create_body)
    return unique_devname


def _ensure_dashboard_folder_exists(target_folder_name: str) -> str:
    sf = get_salesforce_client()
    logger.info("Ensuring Dashboard folder exists: Name='%s'", target_folder_name)
    soql = (
        "SELECT Id, Name, DeveloperName FROM Folder WHERE Type = 'Dashboard' "
        f"AND Name = '{target_folder_name}'"
    )
    res = sf.sobjects.query(soql)
    if res:
        dev = res[0]["DeveloperName"]
        logger.info("Found existing Dashboard folder: DeveloperName='%s'", dev)
        return dev

    existing_devnames_res = sf.sobjects.query(
        "SELECT DeveloperName FROM Folder WHERE Type = 'Dashboard'"
    )
    existing_devnames: Set[str] = (
        {r["DeveloperName"] for r in existing_devnames_res} if existing_devnames_res else set()
    )
    base = _to_devname(target_folder_name)
    unique_devname = _dedupe_developer_name(base, existing_devnames)

    create_body = {
        "Name": target_folder_name,
        "DeveloperName": unique_devname,
        "Type": "Dashboard",
        "AccessType": "Public",
    }
    logger.info(
        "Creating Dashboard folder: Name='%s', DeveloperName='%s'",
        target_folder_name,
        unique_devname,
    )
    sf.sobjects.Folder.insert(create_body)
    return unique_devname


def _get_folder_id_by_devname(folder_type: str, folder_devname: str) -> str:
    sf = get_salesforce_client()
    soql = (
        "SELECT Id FROM Folder WHERE Type = '"
        + folder_type
        + "' AND DeveloperName = '"
        + folder_devname
        + "'"
    )
    res = sf.sobjects.query(soql)
    if not res:
        raise RuntimeError("Folder id not found")
    return res[0]["Id"]


def _list_reports_in_folder_via_soql(folder_name: str) -> List[Dict[str, str]]:
    """List reports by Folder Name using SOQL to avoid Analytics 'recent' pollution.

    Returns items with keys: Id, Name, DeveloperName.
    """
    sf = get_salesforce_client()
    # Query Report by FolderName to avoid relationship/field differences across API versions
    safe_name = folder_name.replace("'", "\\'")
    soql = (
        "SELECT Id, Name, DeveloperName FROM Report WHERE FolderName = '"
        + safe_name
        + "' ORDER BY Name"
    )
    rows = sf.sobjects.query(soql) or []
    return rows


def _dedupe_developer_name(base: str, existing: Set[str]) -> str:
    if base not in existing:
        existing.add(base)
        return base
    suffix = "_copy"
    n = 2
    candidate = base + suffix
    while candidate in existing:
        candidate = f"{base}{suffix}_{n}"
        n += 1
    existing.add(candidate)
    return candidate


def _force_new_developer_name(base: str, existing: Set[str]) -> str:
    """Always return a different name than base, updating existing set."""
    suffix = "_copy"
    n = 1
    candidate = f"{base}{suffix}"
    while candidate in existing or candidate == base:
        n += 1
        candidate = f"{base}{suffix}_{n}"
    existing.add(candidate)
    return candidate


def copy_report_folder(source_folder_id: str, target_folder_name: str) -> None:
    sf = get_salesforce_client()

    src_folder_devname, src_folder_name = _get_folder_devname_by_id(source_folder_id)
    tgt_folder_devname = _ensure_report_folder_exists(target_folder_name)

    # Determine reports within the source folder via Analytics REST, then retrieve exactly those
    logger.info(
        "Starting copy of Report folder: source_devname='%s' -> target_name='%s' (target_devname='%s')",
        src_folder_devname,
        target_folder_name,
        tgt_folder_devname,
    )
    # Prefer SOQL listing by FolderName to avoid Analytics API returning recent reports
    items = _list_reports_in_folder_via_soql(src_folder_name)
    logger.info("Listed reports via SOQL in folder '%s': %d", src_folder_name, len(items))
    if not items:
        logger.warning(
            "Source report folder '%s' has no items; nothing to copy.",
            src_folder_devname,
        )
        return
    # Build fullnames from DeveloperName
    fullnames = [f"{src_folder_devname}/" + it.get("DeveloperName", "") for it in items if it.get("DeveloperName")]
    # Deduplicate while preserving order
    fullnames = list(dict.fromkeys(fullnames))
    logger.info("Reports to retrieve (%d): %s", len(fullnames), ", ".join(fullnames) if fullnames else "<none>")
    retrieve_types = [SfType("Report", fullnames)]
    retr = sf.retrieve.retrieve(retrieve_types)
    logger.info("Waiting for reports retrieve job...")
    retr.wait()
    source_zip = retr.get_zip_file()  # BytesIO
    try:
        raw_zip = source_zip.getvalue()
        with zipfile.ZipFile(BytesIO(raw_zip), "r") as zsrc:
            logger.info("Retrieved zip entries: %s", ", ".join(zsrc.namelist()))
    except Exception as exc:
        logger.debug("Could not inspect retrieved zip: %s", exc)
        raw_zip = source_zip.getvalue()

    # Build new zip with files moved to target folder (dedupe collisions) and package.xml regenerated
    tgt_folder_id = _get_folder_id_by_devname("Report", tgt_folder_devname)
    existing_items = _list_folder_items(tgt_folder_id, "Report")
    existing: Set[str] = {row.get("developerName", "") for row in existing_items if row.get("developerName")}
    new_zip_bytes = _repack_reports_zip(
        BytesIO(raw_zip), src_folder_devname, tgt_folder_devname, existing, force_rename=True
    )
    _deploy_zip(new_zip_bytes)


def prepare_report_copy(source_folder_id: str, target_folder_name: str) -> Dict[str, object]:
    """Build deployable zip for copying a report folder, but do not deploy.

    Returns dictionary with: members (List[str]), package_xml (str), zip_path (str), target_folder_devname (str)
    """
    sf = get_salesforce_client()

    src_folder_devname, src_folder_name = _get_folder_devname_by_id(source_folder_id)
    tgt_folder_devname = _ensure_report_folder_exists(target_folder_name)

    logger.info(
        "Preparing package for Report folder copy: source_devname='%s' -> target_name='%s' (target_devname='%s')",
        src_folder_devname,
        target_folder_name,
        tgt_folder_devname,
    )
    items = _list_reports_in_folder_via_soql(src_folder_name)
    logger.info("Listed reports via SOQL in folder '%s': %d", src_folder_name, len(items))
    if not items:
        return {"members": [], "package_xml": "", "zip_path": "", "target_folder_devname": tgt_folder_devname}
    fullnames = [f"{src_folder_devname}/" + it.get("DeveloperName", "") for it in items if it.get("DeveloperName")]
    fullnames = list(dict.fromkeys(fullnames))
    retrieve_types = [SfType("Report", fullnames)]
    retr = sf.retrieve.retrieve(retrieve_types)
    logger.info("Waiting for reports retrieve job (prepare)...")
    retr.wait()
    source_zip = retr.get_zip_file()
    raw_zip = source_zip.getvalue()

    tgt_folder_id = _get_folder_id_by_devname("Report", tgt_folder_devname)
    existing_items = _list_folder_items(tgt_folder_id, "Report")
    existing: Set[str] = {row.get("developerName", "") for row in existing_items if row.get("developerName")}
    new_zip_bytes = _repack_reports_zip(BytesIO(raw_zip), src_folder_devname, tgt_folder_devname, existing, force_rename=True)

    # Inspect prepared zip: members and package.xml
    members: List[str] = []
    package_xml = ""
    with zipfile.ZipFile(new_zip_bytes, "r") as zf:
        for n in zf.namelist():
            if n.startswith(f"reports/{tgt_folder_devname}/") and n.endswith(".report"):
                dev = n.split("/")[-1].removesuffix(".report")
                members.append(f"{tgt_folder_devname}/{dev}")
        try:
            package_xml = zf.read("package.xml").decode("utf-8", errors="replace")
        except Exception:
            package_xml = ""

    # Persist zip to temp file
    new_zip_bytes.seek(0)
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp.write(new_zip_bytes.read())
        tmp.flush()
        zip_path = tmp.name
    logger.info("Prepared zip stored at %s with %d member(s)", zip_path, len(members))
    return {
        "members": members,
        "package_xml": package_xml,
        "zip_path": zip_path,
        "target_folder_devname": tgt_folder_devname,
    }


def prepare_selected_reports_copy(
    source_folder_id: str,
    selected_report_ids: List[str],
    target_folder_name: str,
) -> Dict[str, object]:
    """Build deployable zip for copying only selected reports from a folder.

    Returns dictionary with: members (List[str]), package_xml (str), zip_path (str), target_folder_devname (str)
    """
    sf = get_salesforce_client()

    src_folder_devname, _ = _get_folder_devname_by_id(source_folder_id)
    tgt_folder_devname = _ensure_report_folder_exists(target_folder_name)

    if not selected_report_ids:
        return {"members": [], "package_xml": "", "zip_path": "", "target_folder_devname": tgt_folder_devname}

    # Resolve Id -> DeveloperName for selected reports
    id_to_dev = _resolve_report_developernames(selected_report_ids)
    fullnames = [f"{src_folder_devname}/{dev}" for dev in id_to_dev.values() if dev]
    # Deduplicate while preserving order
    fullnames = list(dict.fromkeys(fullnames))
    if not fullnames:
        return {"members": [], "package_xml": "", "zip_path": "", "target_folder_devname": tgt_folder_devname}

    # Retrieve just the selected reports
    retr = sf.retrieve.retrieve([SfType("Report", fullnames)])
    retr.wait()
    source_zip = retr.get_zip_file()
    raw_zip = source_zip.getvalue()

    # Prepare existing names in the target folder to dedupe
    tgt_folder_id = _get_folder_id_by_devname("Report", tgt_folder_devname)
    existing_items = _list_folder_items(tgt_folder_id, "Report")
    existing: Set[str] = {row.get("developerName", "") for row in existing_items if row.get("developerName")}

    # Build new zip with files moved to target folder
    new_zip_bytes = _repack_reports_zip(BytesIO(raw_zip), src_folder_devname, tgt_folder_devname, existing, force_rename=True)

    # Inspect prepared zip: members and package.xml
    members: List[str] = []
    package_xml = ""
    with zipfile.ZipFile(new_zip_bytes, "r") as zf:
        for n in zf.namelist():
            if n.startswith(f"reports/{tgt_folder_devname}/") and n.endswith(".report"):
                dev = n.split("/")[-1].removesuffix(".report")
                members.append(f"{tgt_folder_devname}/{dev}")
        try:
            package_xml = zf.read("package.xml").decode("utf-8", errors="replace")
        except Exception:
            package_xml = ""

    # Persist zip to temp file
    new_zip_bytes.seek(0)
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp.write(new_zip_bytes.read())
        tmp.flush()
        zip_path = tmp.name

    logger.info("Prepared selective reports zip stored at %s with %d member(s)", zip_path, len(members))
    return {
        "members": members,
        "package_xml": package_xml,
        "zip_path": zip_path,
        "target_folder_devname": tgt_folder_devname,
    }


def _resolve_report_developernames(report_ids: List[str]) -> Dict[str, str]:
    """Resolve Report.Id -> Report.DeveloperName via SOQL in batches.

    Uses chunking to respect SOQL limits. Returns mapping of Id to DeveloperName.
    """
    sf = get_salesforce_client()
    mapping: Dict[str, str] = {}
    if not report_ids:
        return mapping
    chunk_size = 200
    for i in range(0, len(report_ids), chunk_size):
        chunk = [rid for rid in report_ids[i:i + chunk_size] if rid]
        if not chunk:
            continue
        ids_clause = ",".join(f"'{rid}'" for rid in chunk)
        soql = f"SELECT Id, DeveloperName FROM Report WHERE Id IN ({ids_clause})"
        try:
            res = sf.sobjects.query(soql)
            for row in res or []:
                rid = row.get("Id")
                dev = row.get("DeveloperName")
                if rid and dev:
                    mapping[rid] = dev
        except Exception as exc:
            logger.warning(
                "Failed to resolve DeveloperName for %d report ids: %s",
                len(chunk), exc,
            )
    logger.info("Resolved DeveloperName for %d/%d reports", len(mapping), len(report_ids))
    return mapping


def copy_dashboard_with_reports(
    source_dashboard_folder_id: str,
    source_dashboard_developer_name: str,
    target_dashboard_folder_name: str,
    target_report_folder_name: str,
) -> None:
    sf = get_salesforce_client()

    src_folder_devname, _ = _get_folder_devname_by_id(source_dashboard_folder_id)
    tgt_dash_folder_devname = _ensure_dashboard_folder_exists(target_dashboard_folder_name)
    # Ensure report folder exists for copied reports
    tgt_report_folder_devname = _ensure_report_folder_exists(target_report_folder_name)

    dashboard_fullname = f"{src_folder_devname}/{source_dashboard_developer_name}"
    logger.info(
        "Starting copy of Dashboard '%s' to target folders (dashboard='%s', reports='%s')",
        dashboard_fullname,
        target_dashboard_folder_name,
        target_report_folder_name,
    )

    # Retrieve dashboard first
    retr = sf.retrieve.retrieve([SfType("Dashboard", [dashboard_fullname])])
    logger.info("Waiting for dashboard retrieve job...")
    retr.wait()
    dash_zip = retr.get_zip_file()

    # Extract dashboard XML and find referenced reports
    dash_xml_path = f"dashboards/{src_folder_devname}/{source_dashboard_developer_name}.dashboard"
    with zipfile.ZipFile(dash_zip, "r") as zf:
        xml_bytes = zf.read(dash_xml_path)
    referenced_reports = _extract_report_fullnames_from_dashboard_xml(
        xml_bytes.decode("utf-8")
    )
    logger.info(
        "Referenced reports in dashboard (%d): %s",
        len(referenced_reports),
        ", ".join(referenced_reports) if referenced_reports else "<none>",
    )

    # Retrieve all referenced reports
    if referenced_reports:
        retr2 = sf.retrieve.retrieve([SfType("Report", referenced_reports)])
        logger.info("Waiting for referenced reports retrieve job...")
        retr2.wait()
        reports_zip = retr2.get_zip_file()
    else:
        reports_zip = BytesIO()
        with zipfile.ZipFile(reports_zip, "w"):
            pass
        reports_zip.seek(0)

    # Prepare existing names in target folders for dedupe
    tgt_report_folder_id = _get_folder_id_by_devname("Report", tgt_report_folder_devname)
    existing_reports_items = _list_folder_items(tgt_report_folder_id, "Report")
    report_existing: Set[str] = {r.get("developerName", "") for r in existing_reports_items if r.get("developerName")}

    tgt_dash_folder_id = _get_folder_id_by_devname("Dashboard", tgt_dash_folder_devname)
    existing_dash_items = _list_folder_items(tgt_dash_folder_id, "Dashboard")
    dashboard_existing: Set[str] = {d.get("developerName", "") for d in existing_dash_items if d.get("developerName")}

    # Build deployable zip: moved reports and rewritten dashboard
    deploy_zip = _repack_dashboard_and_reports_zip(
        dash_zip,
        reports_zip,
        src_folder_devname,
        tgt_dash_folder_devname,
        tgt_report_folder_devname,
        source_dashboard_developer_name,
        report_existing,
        dashboard_existing,
    )
    _deploy_zip(deploy_zip)


def prepare_dashboard_copy(
    source_dashboard_folder_id: str,
    source_dashboard_developer_name: str,
    target_dashboard_folder_name: str,
    target_report_folder_name: str,
) -> Dict[str, object]:
    """Build deployable zip for copying a dashboard (and its reports), but do not deploy.

    Returns dictionary with: members_reports (List[str]), member_dashboard (str), package_xml (str), zip_path (str), target_folder_devname (str)
    """
    sf = get_salesforce_client()

    src_folder_devname, _ = _get_folder_devname_by_id(source_dashboard_folder_id)
    tgt_dash_folder_devname = _ensure_dashboard_folder_exists(target_dashboard_folder_name)
    tgt_report_folder_devname = _ensure_report_folder_exists(target_report_folder_name)

    dashboard_fullname = f"{src_folder_devname}/{source_dashboard_developer_name}"
    logger.info(
        "Preparing package for Dashboard '%s' to target folders (dashboard='%s', reports='%s')",
        dashboard_fullname,
        target_dashboard_folder_name,
        target_report_folder_name,
    )
    retr = sf.retrieve.retrieve([SfType("Dashboard", [dashboard_fullname])])
    retr.wait()
    dash_zip = retr.get_zip_file()

    dash_xml_path = f"dashboards/{src_folder_devname}/{source_dashboard_developer_name}.dashboard"
    with zipfile.ZipFile(dash_zip, "r") as zf:
        xml_bytes = zf.read(dash_xml_path)
    referenced_reports = _extract_report_fullnames_from_dashboard_xml(xml_bytes.decode("utf-8"))

    if referenced_reports:
        retr2 = sf.retrieve.retrieve([SfType("Report", referenced_reports)])
        retr2.wait()
        reports_zip = retr2.get_zip_file()
    else:
        reports_zip = BytesIO()
        with zipfile.ZipFile(reports_zip, "w"):
            pass
        reports_zip.seek(0)

    tgt_report_folder_id = _get_folder_id_by_devname("Report", tgt_report_folder_devname)
    existing_reports_items = _list_folder_items(tgt_report_folder_id, "Report")
    report_existing: Set[str] = {r.get("developerName", "") for r in existing_reports_items if r.get("developerName")}

    tgt_dash_folder_id = _get_folder_id_by_devname("Dashboard", tgt_dash_folder_devname)
    existing_dash_items = _list_folder_items(tgt_dash_folder_id, "Dashboard")
    dashboard_existing: Set[str] = {d.get("developerName", "") for d in existing_dash_items if d.get("developerName")}

    deploy_zip = _repack_dashboard_and_reports_zip(
        dash_zip,
        reports_zip,
        src_folder_devname,
        tgt_dash_folder_devname,
        tgt_report_folder_devname,
        source_dashboard_developer_name,
        report_existing,
        dashboard_existing,
    )

    members_reports: List[str] = []
    member_dashboard: str = ""
    package_xml = ""
    with zipfile.ZipFile(deploy_zip, "r") as zf:
        for n in zf.namelist():
            if n.startswith(f"reports/{tgt_report_folder_devname}/") and n.endswith(".report"):
                dev = n.split("/")[-1].removesuffix(".report")
                members_reports.append(f"{tgt_report_folder_devname}/{dev}")
            if n.startswith(f"dashboards/{tgt_dash_folder_devname}/") and n.endswith(".dashboard"):
                dev = n.split("/")[-1].removesuffix(".dashboard")
                member_dashboard = f"{tgt_dash_folder_devname}/{dev}"
        try:
            package_xml = zf.read("package.xml").decode("utf-8", errors="replace")
        except Exception:
            package_xml = ""

    deploy_zip.seek(0)
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp.write(deploy_zip.read())
        tmp.flush()
        zip_path = tmp.name
    logger.info(
        "Prepared dashboard zip stored at %s with %d report(s) and dashboard '%s'",
        zip_path,
        len(members_reports),
        member_dashboard,
    )
    return {
        "members_reports": members_reports,
        "member_dashboard": member_dashboard,
        "package_xml": package_xml,
        "zip_path": zip_path,
        "target_dashboard_devname": tgt_dash_folder_devname,
        "target_report_devname": tgt_report_folder_devname,
    }


def start_deploy(zip_path: str) -> str:
    sf = get_salesforce_client()
    from salesforce_api.models.deploy import Options
    job_id = str(uuid.uuid4())
    logger.info("Starting deployment job %s for zip %s", job_id, zip_path)
    deployment = sf.deploy.deploy(zip_path, Options(checkOnly=False))
    _DEPLOY_JOBS[job_id] = deployment
    return job_id


def get_deploy_status(job_id: str) -> Dict[str, object]:
    deployment = _DEPLOY_JOBS.get(job_id)
    if deployment is None:
        return {"error": "job_not_found", "job_id": job_id}
    status = deployment.get_status()
    # Serialize common fields if present
    out: Dict[str, object] = {"job_id": job_id}
    for key in ["id", "status", "done", "success"]:
        try:
            out[key] = getattr(status, key)
        except Exception:
            pass

    # Components progress (library exposes DeployDetails at status.components)
    try:
        comp = getattr(status, "components", None)
        if comp is not None:
            total = getattr(comp, "total_count", None)
            completed = getattr(comp, "completed_count", None)
            failed = getattr(comp, "failed_count", None)
            out["numberComponentsTotal"] = total
            out["numberComponentsDeployed"] = completed
            out["numberComponentErrors"] = failed
            out["componentsProgressPercent"] = (completed * 100 // total) if total else 0
            # Include component failures if any
            try:
                failures = getattr(comp, "failures", []) or []
                out["componentFailures"] = [
                    {
                        "component_type": getattr(f, "component_type", None),
                        "file": getattr(f, "file", None),
                        "status": getattr(f, "status", None),
                        "message": getattr(f, "message", None),
                    }
                    for f in failures
                ]
            except Exception:
                pass
    except Exception:
        pass

    # Tests progress (library exposes DeployDetails at status.tests)
    try:
        tests = getattr(status, "tests", None)
        if tests is not None:
            t_total = getattr(tests, "total_count", None)
            t_completed = getattr(tests, "completed_count", None)
            t_failed = getattr(tests, "failed_count", None)
            out["numberTestsTotal"] = t_total
            out["numberTestsCompleted"] = t_completed
            out["numberTestErrors"] = t_failed
            out["testsProgressPercent"] = (t_completed * 100 // t_total) if t_total else 0
            # Include test failures if any
            try:
                t_failures = getattr(tests, "failures", []) or []
                out["testFailures"] = [
                    {
                        "class_name": getattr(f, "class_name", None),
                        "method": getattr(f, "method", None),
                        "message": getattr(f, "message", None),
                        "stack_trace": getattr(f, "stack_trace", None),
                    }
                    for f in t_failures
                ]
            except Exception:
                pass
    except Exception:
        pass

    # Failures summary if available
    try:
        details = getattr(status, "details", None)
        if details:
            out["details"] = str(details)
    except Exception:
        pass
    return out


def _repack_reports_zip(
    source_zip: BytesIO,
    src_folder: str,
    tgt_folder: str,
    existing_devnames: Set[str],
    force_rename: bool = False,
) -> BytesIO:
    src = zipfile.ZipFile(source_zip, "r")
    out_bytes = BytesIO()
    with zipfile.ZipFile(out_bytes, "w", zipfile.ZIP_DEFLATED) as out:
        new_fullnames: List[str] = []
        for name in src.namelist():
            # Include all reports from the retrieved zip, regardless of original folder
            if not name.startswith("reports/") or not name.endswith(".report"):
                # skip non-report files from retrieve (e.g., original package.xml)
                continue
            content = src.read(name)
            # derive developer name
            base_devname = name.split("/")[-1].removesuffix(".report")
            # If force_rename, ensure a different name so deploy copies instead of moves
            if force_rename:
                new_devname = _force_new_developer_name(base_devname, existing_devnames)
            else:
                new_devname = _dedupe_developer_name(base_devname, existing_devnames)
            new_name = f"reports/{tgt_folder}/{new_devname}.report"
            out.writestr(new_name, content)
            new_fullnames.append(f"{tgt_folder}/{new_devname}")

        # package.xml
        pkg = _render_package_xml({"Report": new_fullnames})
        out.writestr("package.xml", pkg)

    logger.info(
        "Prepared deploy package for Reports: %d members -> %s",
        len(new_fullnames),
        ", ".join(new_fullnames) if new_fullnames else "<none>",
    )
    logger.info("package.xml contents (Reports):\n%s", pkg)

    out_bytes.seek(0)
    return out_bytes


def _repack_dashboard_and_reports_zip(
    dash_zip: BytesIO,
    reports_zip: BytesIO,
    src_folder: str,
    tgt_dash_folder: str,
    tgt_report_folder: str,
    dashboard_devname: str,
    report_existing: Set[str],
    dashboard_existing: Set[str],
) -> BytesIO:
    dash_src = zipfile.ZipFile(dash_zip, "r")
    rep_src = zipfile.ZipFile(reports_zip, "r")

    out_bytes = BytesIO()
    with zipfile.ZipFile(out_bytes, "w", zipfile.ZIP_DEFLATED) as out:
        report_fullnames: List[str] = []
        rename_map: Dict[str, str] = {}

        # write reports moved
        for name in rep_src.namelist():
            # Include all reports returned in retrieve; they may come from various source folders
            if not name.startswith("reports/") or not name.endswith(".report"):
                continue
            content = rep_src.read(name)
            base_devname = name.split("/")[-1].removesuffix(".report")
            new_devname = _force_new_developer_name(base_devname, report_existing)
            new_name = f"reports/{tgt_report_folder}/{new_devname}.report"
            out.writestr(new_name, content)
            report_fullnames.append(f"{tgt_report_folder}/{new_devname}")
            # Build rename map for any possible original folder
            try:
                _, original_folder, _ = name.split("/", 2)
            except ValueError:
                original_folder = src_folder
            rename_map[f"{original_folder}/{base_devname}"] = f"{tgt_report_folder}/{new_devname}"

        # dashboard rewritten
        old_dash_path = f"dashboards/{src_folder}/{dashboard_devname}.dashboard"
        xml_s = dash_src.read(old_dash_path).decode("utf-8")
        new_xml_s = _rewrite_dashboard_report_refs(xml_s, rename_map)
        # Always force rename dashboard to ensure copy-not-move semantics
        new_dash_devname = _force_new_developer_name(dashboard_devname, dashboard_existing)
        new_dash_path = f"dashboards/{tgt_dash_folder}/{new_dash_devname}.dashboard"
        out.writestr(new_dash_path, new_xml_s.encode("utf-8"))

        # package.xml with both types
        pkg = _render_package_xml({
            "Report": report_fullnames,
            "Dashboard": [f"{tgt_dash_folder}/{new_dash_devname}"],
        })
        out.writestr("package.xml", pkg)

    logger.info(
        "Prepared deploy package for Dashboard+Reports: dashboard='%s', reports=%d",
        f"{tgt_dash_folder}/{new_dash_devname}",
        len(report_fullnames),
    )
    logger.info("Rename map entries: %d", len(rename_map))
    if rename_map:
        # Log a compact view of the map
        preview = ", ".join(f"{k} -> {v}" for k, v in list(rename_map.items())[:20])
        if len(rename_map) > 20:
            preview += ", ..."
        logger.info("Report rename preview: %s", preview)
    logger.info("package.xml contents (Dashboard+Reports):\n%s", pkg)

    out_bytes.seek(0)
    return out_bytes


def _deploy_zip(zip_bytes: BytesIO) -> None:
    sf = get_salesforce_client()
    # Inspect and log zip contents and package.xml before deploy
    try:
        zip_bytes.seek(0)
        raw = zip_bytes.read()
        with zipfile.ZipFile(BytesIO(raw), "r") as zf:
            names = zf.namelist()
            logger.info("Deploying zip with %d entries: %s", len(names), ", ".join(names))
            if "package.xml" in names:
                pkg_s = zf.read("package.xml").decode("utf-8", errors="replace")
                logger.info("Deploy package.xml contents before deploy:\n%s", pkg_s)
                # Skip deploy if package.xml has no <types> members
                has_members = "<types>" in pkg_s and "<members>" in pkg_s
                if not has_members:
                    logger.warning("package.xml has no members; skipping deployment.")
                    return
            else:
                logger.warning("Deploy zip missing package.xml")
                # Attempt to detect metadata files
                meta_entries = [n for n in names if n.endswith((".report", ".dashboard"))]
                if not meta_entries:
                    logger.warning("No metadata entries detected; skipping deployment.")
                    return
    except Exception as exc:
        logger.warning("Failed to inspect deploy zip: %s", exc)
        raw = raw if 'raw' in locals() else zip_bytes.getvalue()

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp.write(raw)
        tmp.flush()
        from salesforce_api.models.deploy import Options

        logger.info("Starting metadata deployment (checkOnly=False)...")
        deployment = sf.deploy.deploy(tmp.name, Options(checkOnly=False))

        # Poll for status with logging
        import time
        max_wait_seconds = 1800
        poll_interval = 2
        waited = 0
        final_status = None
        while waited <= max_wait_seconds:
            status = deployment.get_status()
            state = getattr(status, "status", None) or getattr(status, "State", None)
            done = getattr(status, "done", None)
            success = getattr(status, "success", None)
            logger.info("Deploy status: status=%s done=%s success=%s", state, done, success)
            if bool(done) or (isinstance(state, str) and state.lower() in ("succeeded", "failed", "canceled", "completed")):
                final_status = status
                break
            time.sleep(poll_interval)
            waited += poll_interval

        if final_status is None:
            # Fallback: ensure we have the latest
            final_status = deployment.get_status()

        # Summarize final status
        try:
            detail_dict = getattr(final_status, "__dict__", {})
            logger.info("Final deploy status summary: %s", detail_dict)
        except Exception:
            logger.info("Final deploy status (raw): %s", str(final_status))

        if getattr(final_status, "success", True) is False:
            raise RuntimeError("Deployment failed")


def _render_package_xml(type_to_members: Dict[str, List[str]]) -> str:
    version = os.getenv("SF_API_VERSION") or "58.0"
    types_xml_parts: List[str] = []
    for t, members in type_to_members.items():
        if not members:
            continue
        member_tags = "".join(f"<members>{m}</members>" for m in sorted(set(members)))
        types_xml_parts.append(f"<types>{member_tags}<name>{t}</name></types>")
    types_xml = "".join(types_xml_parts)
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Package xmlns=\"http://soap.sforce.com/2006/04/metadata\">"
        f"{types_xml}"
        f"<version>{version}</version>"
        "</Package>"
    )


def _extract_report_fullnames_from_dashboard_xml(xml_s: str) -> List[str]:
    # Simple and fast parse: look for <report>Folder/Name</report>
    out: List[str] = []
    start_tag = "<report>"
    end_tag = "</report>"
    i = 0
    while True:
        i = xml_s.find(start_tag, i)
        if i == -1:
            break
        j = xml_s.find(end_tag, i)
        if j == -1:
            break
        val = xml_s[i + len(start_tag) : j].strip()
        if "/" in val:
            out.append(val)
        i = j + len(end_tag)
    return sorted(set(out))


def _rewrite_dashboard_report_refs(xml_s: str, rename_map: Dict[str, str]) -> str:
    # Replace <report>oldFullName</report> with new fullName using the rename_map
    for old_full, new_full in rename_map.items():
        xml_s = xml_s.replace(f"<report>{old_full}</report>", f"<report>{new_full}</report>")
    return xml_s


def _to_devname(label: str) -> str:
    # Simple normalization for DeveloperName
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in label)
    cleaned = cleaned.strip("_")
    return cleaned[:80] if len(cleaned) > 80 else cleaned


def _list_folder_items(folder_id: str, item_type: str) -> List[Dict[str, str]]:
    # SOQL-based listing by folder name to avoid Analytics 'recent' behavior
    sf = get_salesforce_client()
    _, folder_name = _get_folder_devname_by_id(folder_id)
    safe_name = folder_name.replace("'", "\\'")
    if item_type == "Report":
        soql = (
            "SELECT Id, Name, DeveloperName FROM Report WHERE FolderName = '"
            + safe_name
            + "' ORDER BY Name"
        )
        rows = sf.sobjects.query(soql) or []
        out: List[Dict[str, str]] = []
        for r in rows:
            out.append({
                "Id": r.get("Id", ""),
                "name": r.get("Name", ""),
                "developerName": r.get("DeveloperName", ""),
            })
        logger.info("Found %d Report item(s) in folder '%s' via SOQL", len(out), folder_id)
        return out
    else:
        soql = (
            "SELECT Id, DeveloperName, Title FROM Dashboard WHERE FolderName = '"
            + safe_name
            + "' ORDER BY Title"
        )
        rows = sf.sobjects.query(soql) or []
        out: List[Dict[str, str]] = []
        for r in rows:
            out.append({
                "Id": r.get("Id", ""),
                "name": r.get("Title", r.get("DeveloperName", "")),
                "developerName": r.get("DeveloperName", ""),
            })
        logger.info("Found %d Dashboard item(s) in folder '%s' via SOQL", len(out), folder_id)
        return out


