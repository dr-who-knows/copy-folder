import os
import tempfile
import zipfile
from io import BytesIO
from typing import Dict, List, Tuple, Set

from dotenv import load_dotenv

# salesforce_api provides both data APIs and metadata deploy/retrieve
from salesforce_api import Salesforce
from salesforce_api.models.shared import Type as SfType


_SF_CLIENT: Salesforce | None = None


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

    _SF_CLIENT = Salesforce(**client_kwargs)
    return _SF_CLIENT


def list_report_folders() -> List[Dict[str, str]]:
    sf = get_salesforce_client()
    soql = "SELECT Id, Name, DeveloperName FROM Folder WHERE Type = 'Report' ORDER BY Name"
    res = sf.sobjects.query(soql)
    return res  # library returns list-like per docs


def list_dashboard_folders() -> List[Dict[str, str]]:
    sf = get_salesforce_client()
    soql = "SELECT Id, Name, DeveloperName FROM Folder WHERE Type = 'Dashboard' ORDER BY Name"
    res = sf.sobjects.query(soql)
    return res


def list_dashboards_in_folder(folder_id: str) -> List[Dict[str, str]]:
    return _list_folder_items(folder_id, "Dashboard")


def _get_folder_devname_by_id(folder_id: str) -> Tuple[str, str]:
    sf = get_salesforce_client()
    soql = f"SELECT Id, Name, DeveloperName FROM Folder WHERE Id = '{folder_id}'"
    res = sf.sobjects.query(soql)
    if not res:
        raise RuntimeError("Folder not found")
    row = res[0]
    return row["DeveloperName"], row["Name"]


def _ensure_report_folder_exists(target_folder_name: str) -> str:
    sf = get_salesforce_client()
    # Try to find by Name; if not exist, create Folder record
    soql = (
        "SELECT Id, Name, DeveloperName FROM Folder WHERE Type = 'Report' "
        f"AND Name = '{target_folder_name}'"
    )
    res = sf.sobjects.query(soql)
    if res:
        return res[0]["DeveloperName"]

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
    sf.sobjects.Folder.insert(create_body)
    return unique_devname


def _ensure_dashboard_folder_exists(target_folder_name: str) -> str:
    sf = get_salesforce_client()
    soql = (
        "SELECT Id, Name, DeveloperName FROM Folder WHERE Type = 'Dashboard' "
        f"AND Name = '{target_folder_name}'"
    )
    res = sf.sobjects.query(soql)
    if res:
        return res[0]["DeveloperName"]

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


def copy_report_folder(source_folder_id: str, target_folder_name: str) -> None:
    sf = get_salesforce_client()

    src_folder_devname, _ = _get_folder_devname_by_id(source_folder_id)
    tgt_folder_devname = _ensure_report_folder_exists(target_folder_name)

    # Determine reports within the source folder via Analytics REST, then retrieve exactly those
    items = _list_folder_items(source_folder_id, "Report")
    if not items:
        return
    fullnames = [f"{src_folder_devname}/" + it.get("developerName", "") for it in items if it.get("developerName")]
    retrieve_types = [SfType("Report", fullnames)]
    retr = sf.retrieve.retrieve(retrieve_types)
    retr.wait()
    source_zip = retr.get_zip_file()  # BytesIO

    # Build new zip with files moved to target folder (dedupe collisions) and package.xml regenerated
    tgt_folder_id = _get_folder_id_by_devname("Report", tgt_folder_devname)
    existing_items = _list_folder_items(tgt_folder_id, "Report")
    existing: Set[str] = {row.get("developerName", "") for row in existing_items if row.get("developerName")}
    new_zip_bytes = _repack_reports_zip(
        source_zip, src_folder_devname, tgt_folder_devname, existing
    )
    _deploy_zip(new_zip_bytes)


def copy_dashboard_with_reports(
    source_dashboard_folder_id: str,
    source_dashboard_developer_name: str,
    target_folder_name: str,
) -> None:
    sf = get_salesforce_client()

    src_folder_devname, _ = _get_folder_devname_by_id(source_dashboard_folder_id)
    tgt_folder_devname = _ensure_dashboard_folder_exists(target_folder_name)
    # Also ensure report folder exists with the same name for copied reports
    _ensure_report_folder_exists(target_folder_name)

    dashboard_fullname = f"{src_folder_devname}/{source_dashboard_developer_name}"

    # Retrieve dashboard first
    retr = sf.retrieve.retrieve([SfType("Dashboard", [dashboard_fullname])])
    retr.wait()
    dash_zip = retr.get_zip_file()

    # Extract dashboard XML and find referenced reports
    dash_xml_path = f"dashboards/{src_folder_devname}/{source_dashboard_developer_name}.dashboard"
    with zipfile.ZipFile(dash_zip, "r") as zf:
        xml_bytes = zf.read(dash_xml_path)
    referenced_reports = _extract_report_fullnames_from_dashboard_xml(
        xml_bytes.decode("utf-8")
    )

    # Retrieve all referenced reports
    if referenced_reports:
        retr2 = sf.retrieve.retrieve([SfType("Report", referenced_reports)])
        retr2.wait()
        reports_zip = retr2.get_zip_file()
    else:
        reports_zip = BytesIO()
        with zipfile.ZipFile(reports_zip, "w"):
            pass
        reports_zip.seek(0)

    # Prepare existing names in target folders for dedupe
    tgt_report_folder_id = _get_folder_id_by_devname("Report", tgt_folder_devname)
    existing_reports_items = _list_folder_items(tgt_report_folder_id, "Report")
    report_existing: Set[str] = {r.get("developerName", "") for r in existing_reports_items if r.get("developerName")}

    tgt_dash_folder_id = _get_folder_id_by_devname("Dashboard", tgt_folder_devname)
    existing_dash_items = _list_folder_items(tgt_dash_folder_id, "Dashboard")
    dashboard_existing: Set[str] = {d.get("developerName", "") for d in existing_dash_items if d.get("developerName")}

    # Build deployable zip: moved reports and rewritten dashboard
    deploy_zip = _repack_dashboard_and_reports_zip(
        dash_zip,
        reports_zip,
        src_folder_devname,
        tgt_folder_devname,
        source_dashboard_developer_name,
        report_existing,
        dashboard_existing,
    )
    _deploy_zip(deploy_zip)


def _repack_reports_zip(
    source_zip: BytesIO,
    src_folder: str,
    tgt_folder: str,
    existing_devnames: Set[str],
) -> BytesIO:
    src = zipfile.ZipFile(source_zip, "r")
    out_bytes = BytesIO()
    with zipfile.ZipFile(out_bytes, "w", zipfile.ZIP_DEFLATED) as out:
        new_fullnames: List[str] = []
        for name in src.namelist():
            if not name.startswith(f"reports/{src_folder}/") or not name.endswith(".report"):
                # skip non-report files from retrieve (e.g., original package.xml)
                continue
            content = src.read(name)
            # derive developer name and dedupe
            base_devname = name.split("/")[-1].removesuffix(".report")
            new_devname = _dedupe_developer_name(base_devname, existing_devnames)
            new_name = f"reports/{tgt_folder}/{new_devname}.report"
            out.writestr(new_name, content)
            new_fullnames.append(f"{tgt_folder}/{new_devname}")

        # package.xml
        pkg = _render_package_xml({"Report": new_fullnames})
        out.writestr("package.xml", pkg)

    out_bytes.seek(0)
    return out_bytes


def _repack_dashboard_and_reports_zip(
    dash_zip: BytesIO,
    reports_zip: BytesIO,
    src_folder: str,
    tgt_folder: str,
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
            if not name.startswith(f"reports/{src_folder}/") or not name.endswith(".report"):
                continue
            content = rep_src.read(name)
            base_devname = name.split("/")[-1].removesuffix(".report")
            new_devname = _dedupe_developer_name(base_devname, report_existing)
            new_name = f"reports/{tgt_folder}/{new_devname}.report"
            out.writestr(new_name, content)
            report_fullnames.append(f"{tgt_folder}/{new_devname}")
            rename_map[f"{src_folder}/{base_devname}"] = f"{tgt_folder}/{new_devname}"

        # dashboard rewritten
        old_dash_path = f"dashboards/{src_folder}/{dashboard_devname}.dashboard"
        xml_s = dash_src.read(old_dash_path).decode("utf-8")
        new_xml_s = _rewrite_dashboard_report_refs(xml_s, rename_map)
        # dedupe dashboard name if collisions
        new_dash_devname = _dedupe_developer_name(dashboard_devname, dashboard_existing)
        new_dash_path = f"dashboards/{tgt_folder}/{new_dash_devname}.dashboard"
        out.writestr(new_dash_path, new_xml_s.encode("utf-8"))

        # package.xml with both types
        pkg = _render_package_xml({
            "Report": report_fullnames,
            "Dashboard": [f"{tgt_folder}/{new_dash_devname}"],
        })
        out.writestr("package.xml", pkg)

    out_bytes.seek(0)
    return out_bytes


def _deploy_zip(zip_bytes: BytesIO) -> None:
    sf = get_salesforce_client()
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp.write(zip_bytes.read())
        tmp.flush()
        from salesforce_api.models.deploy import Options

        deployment = sf.deploy.deploy(tmp.name, Options(checkOnly=False))
        deployment.wait()
        status = deployment.get_status()
        if getattr(status, "success", True) is False:
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
    # Uses Analytics REST API
    sf = get_salesforce_client()
    version = os.getenv("SF_API_VERSION") or "58.0"
    base = getattr(sf, "connection").instance_url.rstrip('/')  # type: ignore[attr-defined]
    if item_type == "Report":
        url = f"{base}/services/data/v{version}/analytics/reports?folderId={folder_id}"
    else:
        url = f"{base}/services/data/v{version}/analytics/dashboards?folderId={folder_id}"
    resp = getattr(sf, "connection").session.get(url)  # type: ignore[attr-defined]
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        items = data
    else:
        items = (
            data.get("reports") if item_type == "Report" else data.get("dashboards")
        ) or data.get("items") or data.get("records") or []
    # Normalize keys to include developerName when possible
    norm: List[Dict[str, str]] = []
    for it in items:
        name = it.get("name") or it.get("title") or it.get("label") or ""
        dev = it.get("developerName") or _to_devname(name)
        norm.append({
            "Id": it.get("id", ""),
            "name": name,
            "developerName": dev,
        })
    return norm


