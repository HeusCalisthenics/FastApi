from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from fastapi.responses import HTMLResponse
from ..database import SessionLocal, APIRequestLog
from ..crud import get_logs

router = APIRouter()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/logs/json")
def get_logs_json(
    db: Session = Depends(get_db),
    page: int = Query(1, alias="page", ge=1),
    per_page: int = Query(10, alias="per_page", ge=1, le=100),
    symbol: str = Query(None, alias="symbol"),
    status_code: int = Query(None, alias="status"),
):
    query = db.query(APIRequestLog)

    if symbol:
        query = query.filter(APIRequestLog.requested_symbol == symbol.upper())
    if status_code:
        query = query.filter(APIRequestLog.status_code == status_code)

    logs = query.offset((page - 1) * per_page).limit(per_page).all()

    return logs  # ✅ Now returns JSON


# Paginated & Filtered Logs API
@router.get("/logs/", response_class=HTMLResponse)
def get_logs_html(
    db: Session = Depends(get_db),
    page: int = Query(1, alias="page", ge=1),  # Pagination (default page 1)
    per_page: int = Query(10, alias="per_page", ge=1, le=100),  # Items per page
    symbol: str = Query(None, alias="symbol"),  # Filter by crypto symbol
    status_code: int = Query(None, alias="status"),  # Filter by status code
):
    query = db.query(APIRequestLog)

    # Apply filters
    if symbol:
        query = query.filter(APIRequestLog.requested_symbol == symbol.upper())
    if status_code:
        query = query.filter(APIRequestLog.status_code == status_code)

    # Get total logs count (for pagination info)
    total_logs = query.count()

    # Apply pagination
    logs = query.offset((page - 1) * per_page).limit(per_page).all()

    # Generate HTML table
    table_html = f"""
    <html>
    <head>
        <title>API Request Logs</title>
        <style>
            body {{ background-color: #121212; color: white; font-family: Arial, sans-serif; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ border: 1px solid #333; padding: 8px; text-align: left; }}
            th {{ background-color: #1E1E1E; color: cyan; font-weight: bold; }}
            td {{ color: yellow; }}
            tr:nth-child(even) {{ background-color: #1E1E1E; }}
            tr:hover {{ background-color: #333; }}
            h2 {{ color: cyan; text-align: center; }}
        </style>
    </head>
    <body>
        <h2>API Request Logs (Page {page}/{(total_logs // per_page) + 1})</h2>
        <table>
            <tr>
                <th>ID</th>
                <th>User IP</th>
                <th>Requested Symbol</th>
                <th>Timestamp</th>
                <th>Status Code</th>
                <th>Success</th>
                <th>Response Data</th>
            </tr>
    """

    for log in logs:
        table_html += f"""
            <tr>
                <td>{log.id}</td>
                <td>{log.user_ip}</td>
                <td>{log.requested_symbol}</td>
                <td>{log.timestamp}</td>
                <td>{log.status_code}</td>
                <td>{'✅' if log.success else '❌'}</td>
                <td>{log.response_data[:100]}...</td>
            </tr>
        """

    table_html += "</table>"

    # Pagination Links
    table_html += "<br><div style='text-align:center;'>"
    if page > 1:
        table_html += f"<a href='/logs/?page={page-1}&per_page={per_page}' style='color:cyan;'>⬅ Previous</a> | "
    if (page * per_page) < total_logs:
        table_html += f"<a href='/logs/?page={page+1}&per_page={per_page}' style='color:cyan;'>Next ➡</a>"
    table_html += "</div>"

    table_html += "</body></html>"

    return table_html
