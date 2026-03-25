"""
Saturn Skill: n8n Workflow Builder
Builds, validates, deploys and manages n8n workflows.
Used by: Forge
All methods return dict — never raise. Fail-open design.
"""

import os


def _base():
    return os.environ.get("N8N_BASE_URL", "http://localhost:5678")


def _key():
    return os.environ.get("N8N_API_KEY", "")


def _headers():
    return {"X-N8N-API-KEY": _key(), "Content-Type": "application/json"}


def _get(path: str, timeout: int = 10) -> dict:
    try:
        import requests

        r = requests.get(f"{_base()}{path}", headers=_headers(), timeout=timeout)
        r.raise_for_status()
        return {"status": "success", "data": r.json()}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


def _post(path: str, body: dict = None, timeout: int = 15) -> dict:
    try:
        import requests

        r = requests.post(
            f"{_base()}{path}",
            headers=_headers(),
            json=body or {},
            timeout=timeout,
        )
        r.raise_for_status()
        return {"status": "success", "data": r.json()}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


def _delete(path: str, timeout: int = 10) -> dict:
    try:
        import requests

        r = requests.delete(f"{_base()}{path}", headers=_headers(), timeout=timeout)
        r.raise_for_status()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


def n8n_health() -> dict:
    try:
        import requests

        r = requests.get(f"{_base()}/healthz", timeout=5)
        return {"status": "ok" if r.status_code == 200 else "degraded", "code": r.status_code}
    except Exception as e:
        return {"status": "unreachable", "reason": str(e)}


def n8n_list() -> dict:
    result = _get("/api/v1/workflows")
    if result.get("status") != "success":
        return result
    raw = result.get("data")
    items = raw if isinstance(raw, list) else raw.get("data", []) if isinstance(raw, dict) else []
    workflows = []
    for w in items:
        if isinstance(w, dict):
            workflows.append(
                {
                    "id": w.get("id", ""),
                    "name": w.get("name", ""),
                    "active": w.get("active", False),
                }
            )
    return {"status": "success", "count": len(workflows), "workflows": workflows}


def n8n_run(workflow_id: str, payload: dict = None) -> dict:
    return _post(f"/api/v1/workflows/{workflow_id}/run", payload or {}, timeout=30)


def n8n_activate(workflow_id: str, active: bool = True) -> dict:
    endpoint = "activate" if active else "deactivate"
    return _post(f"/api/v1/workflows/{workflow_id}/{endpoint}")


def n8n_deploy(workflow_dict: dict, activate: bool = False) -> dict:
    result = _post("/api/v1/workflows", workflow_dict)
    if result.get("status") != "success":
        return result
    data = result.get("data")
    wf_id = data.get("id") if isinstance(data, dict) else None
    if activate and wf_id:
        n8n_activate(str(wf_id), True)
    return {
        "status": "success",
        "workflow_id": wf_id,
        "name": data.get("name", "") if isinstance(data, dict) else "",
        "active": activate,
    }


def n8n_delete(workflow_id: str) -> dict:
    return _delete(f"/api/v1/workflows/{workflow_id}")


def n8n_build_simple(name: str, webhook_path: str, target_url: str, method: str = "POST") -> dict:
    """Build a minimal webhook -> HTTP request workflow dict."""
    wf = {
        "name": name,
        "nodes": [
            {
                "id": "node1",
                "name": "Webhook",
                "type": "n8n-nodes-base.webhook",
                "typeVersion": 1,
                "position": [250, 300],
                "parameters": {
                    "path": webhook_path,
                    "httpMethod": method,
                    "responseMode": "onReceived",
                },
            },
            {
                "id": "node2",
                "name": "HTTP Request",
                "type": "n8n-nodes-base.httpRequest",
                "typeVersion": 4,
                "position": [500, 300],
                "parameters": {
                    "url": target_url,
                    "method": method,
                    "sendBody": True,
                    "bodyContentType": "json",
                },
            },
        ],
        "connections": {
            "Webhook": {
                "main": [[{"node": "HTTP Request", "type": "main", "index": 0}]],
            }
        },
        "settings": {"executionOrder": "v1"},
    }
    return {"status": "success", "workflow": wf}
