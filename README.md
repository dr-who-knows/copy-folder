This app helps you copy Salesforce reports and dashboards. That’s it.

### Required Salesforce permissions
- API Enabled
- Modify Metadata Through Metadata API
- Manage Reports in Public Folders and Manage Dashboards in Public Folders

### Environment
- Create a `.env` file with:
```
SF_USERNAME=your.username@example.com
SF_PASSWORD=yourPassword
SF_SECURITY_TOKEN=yourSecurityToken
SF_IS_SANDBOX=true
```
Optional:
```
SF_DOMAIN=mydomain.my.salesforce.com
SF_API_VERSION=62.0
LOG_LEVEL=INFO
```

### Run with Docker
- `docker build -t sf-copier .`
- `docker run --rm -p 8000:8000 --env-file .env sf-copier`

### Usage flow
1) Open http://localhost:8000
2) Select reports or a dashboard you want to copy
3) Select the target folders — for the dashboard, you can specify both the dashboard folder and the folder for its reports
4) Prepare a deploy package, review the generated `package.xml`, then deploy

Notes:
- Copy only adds new items; it does not modify the source. Names are suffixed to avoid conflicts.
- Deployment uses the Salesforce Metadata API (not change sets).

### You can also run locally (venv)
- `python3 -m venv venv`
- `source venv/bin/activate`
- `pip install -r requirements.txt`
- `uvicorn app.main:app --host 0.0.0.0 --port 8000`

### Stack
- FastAPI + Uvicorn
- Jinja2 templates + Bootstrap 5 (CDN)
- `salesforce-api` library for retrieve/deploy

### License
MIT, provided “AS IS” without warranties or liability, and with no support obligations. This README serves as the license notice. Full text: https://opensource.org/licenses/MIT