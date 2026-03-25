from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
from pathlib import Path

import httpx

DB_PATH = Path('/home/navin/Workspace/Saturn/database/saturn.db')
N8N_BASE = 'http://localhost:5678/api/v1'
WORKFLOWS_DIR = Path('/home/navin/Workspace/Saturn/configs/workflows')
BASE_PATH = Path(os.environ.get('SATURN_BASE_PATH', str(Path.home() / 'Workspace' / 'Saturn'))).expanduser().resolve()
SKILLS_DIR = BASE_PATH / 'skills' / 'n8n'
ERROR_TYPES = {'API_ERROR', 'AUTH_ERROR', 'RATE_LIMIT', 'NETWORK_ERROR', 'DB_ERROR', 'LOGIC_ERROR'}


def utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat()


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def log_agent(action: str, detail: str, result: str) -> None:
    conn = db_conn()
    try:
        conn.execute(
            'INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)',
            ('Forge', action, detail[:500], result[:200], utc_now()),
        )
        conn.commit()
    finally:
        conn.close()


def log_error(action: str, error_type: str, message: str, detail: str = '') -> None:
    safe_type = error_type if error_type in ERROR_TYPES else 'LOGIC_ERROR'
    now = utc_now()
    conn = db_conn()
    try:
        conn.execute(
            'INSERT INTO error_log (agent, action, error_type, message, detail, ts) VALUES (?,?,?,?,?,?)',
            ('Forge', action, safe_type, message[:300], detail[:500], now),
        )
        conn.execute(
            'INSERT INTO agent_log (agent, action, detail, result, ts) VALUES (?,?,?,?,?)',
            ('Forge', action, detail[:500], safe_type, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_n8n_headers() -> dict:
    key = os.environ.get('N8N_API_KEY', '')
    if not key:
        raise ValueError('N8N_API_KEY not set')
    return {'X-N8N-API-KEY': key, 'Content-Type': 'application/json'}


def validate_workflow_json(workflow: dict) -> tuple[bool, str]:
    if not isinstance(workflow, dict):
        return False, 'workflow must be a dict'

    nodes = workflow.get('nodes')
    connections = workflow.get('connections')

    if not isinstance(nodes, list) or len(nodes) < 1:
        return False, 'nodes[] missing or empty'
    if not isinstance(connections, dict):
        return False, 'connections{} missing'

    node_ids: set[str] = set()
    node_names: set[str] = set()
    for idx, node in enumerate(nodes):
        if not isinstance(node, dict):
            return False, f'node[{idx}] must be an object'
        for key in ('id', 'name', 'type', 'position'):
            if key not in node:
                return False, f'node[{idx}] missing {key}'
        node_id = str(node.get('id'))
        if node_id in node_ids:
            return False, f'duplicate node id: {node_id}'
        node_ids.add(node_id)
        node_names.add(str(node.get('name')))
        if not isinstance(node.get('position'), list) or len(node.get('position')) != 2:
            return False, f'node[{idx}] position must be [x,y]'

    for source_name, mapping in connections.items():
        if source_name not in node_names:
            return False, f'connection source not found: {source_name}'
        if not isinstance(mapping, dict):
            return False, f'connection map invalid for source: {source_name}'

        for edge_group in mapping.values():
            if not isinstance(edge_group, list):
                return False, f'connection group invalid for source: {source_name}'
            for branch in edge_group:
                if not isinstance(branch, list):
                    return False, f'connection branch invalid for source: {source_name}'
                for edge in branch:
                    if not isinstance(edge, dict):
                        return False, f'connection edge invalid for source: {source_name}'
                    target_id = edge.get('nodeId') or edge.get('id')
                    target_name = edge.get('node')
                    if target_id is not None:
                        if str(target_id) not in node_ids:
                            return False, f'connection target id not found: {target_id}'
                    elif target_name is not None:
                        if str(target_name) not in node_names:
                            return False, f'connection target not found: {target_name}'
                    else:
                        return False, f'connection edge missing target for source: {source_name}'

    return True, 'ok'


def deploy_workflow(workflow: dict, activate: bool = False) -> dict:
    valid, reason = validate_workflow_json(workflow)
    if not valid:
        log_agent('deploy_workflow', f'rejected reason={reason}', 'rejected')
        return {'status': 'rejected', 'reason': reason}

    try:
        headers = get_n8n_headers()
    except Exception as exc:
        log_error('deploy_workflow', 'AUTH_ERROR', 'n8n api key missing', str(exc))
        return {'status': 'failed', 'reason': str(exc)}

    workflow_id = ''
    active = False
    status = 'failed'
    try:
        payload_to_send = {
            key: value
            for key, value in workflow.items()
            if key not in {'id', 'active', 'createdAt', 'updatedAt', 'versionId'}
        }
        with httpx.Client(timeout=20.0) as client:
            resp = client.post(f'{N8N_BASE}/workflows', headers=headers, json=payload_to_send)
            resp.raise_for_status()
            payload = resp.json()
            workflow_id = str(payload.get('id') or payload.get('data', {}).get('id') or '')
            active = bool(payload.get('active') or payload.get('data', {}).get('active'))
            status = 'deployed'

            if activate and workflow_id:
                act_resp = client.patch(f'{N8N_BASE}/workflows/{workflow_id}/activate', headers=headers)
                act_resp.raise_for_status()
                active = True

        name = str(workflow.get('name') or f'workflow_{workflow_id or "unknown"}')
        safe_name = re.sub(r'[^A-Za-z0-9._-]+', '_', name).strip('_') or 'workflow'
        WORKFLOWS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = WORKFLOWS_DIR / f'{safe_name}.json'
        out_path.write_text(json.dumps(workflow, indent=2), encoding='utf-8')

        log_agent('deploy_workflow', f'name={name} id={workflow_id} activate={activate}', 'success')
        return {'status': status, 'workflow_id': workflow_id, 'active': active}
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code if exc.response is not None else 0
        err_type = 'AUTH_ERROR' if code in (401, 403) else ('RATE_LIMIT' if code == 429 else 'API_ERROR')
        log_error('deploy_workflow', err_type, 'n8n workflow deploy failed', str(exc))
        log_agent('deploy_workflow', str(exc), 'failed')
        return {'status': 'failed', 'workflow_id': workflow_id, 'active': active}
    except Exception as exc:
        log_error('deploy_workflow', 'API_ERROR', 'n8n workflow deploy failed', str(exc))
        log_agent('deploy_workflow', str(exc), 'failed')
        return {'status': 'failed', 'workflow_id': workflow_id, 'active': active}


def list_workflows() -> list:
    try:
        headers = get_n8n_headers()
    except Exception as exc:
        log_error('list_workflows', 'AUTH_ERROR', 'n8n api key missing', str(exc))
        return []

    try:
        resp = httpx.get(f'{N8N_BASE}/workflows', headers=headers, timeout=20.0)
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get('data') if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            rows = []
        out = [
            {'id': str(item.get('id', '')), 'name': item.get('name', ''), 'active': bool(item.get('active', False))}
            for item in rows
        ]
        log_agent('list_workflows', f'count={len(out)}', 'success')
        return out
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code if exc.response is not None else 0
        err_type = 'AUTH_ERROR' if code in (401, 403) else ('RATE_LIMIT' if code == 429 else 'API_ERROR')
        log_error('list_workflows', err_type, 'n8n list workflows failed', str(exc))
        return []
    except Exception as exc:
        log_error('list_workflows', 'NETWORK_ERROR', 'n8n list workflows failed', str(exc))
        return []


def delete_workflow(workflow_id: str) -> dict:
    workflow_id = str(workflow_id or '').strip()
    if not workflow_id:
        return {'status': 'failed', 'workflow_id': workflow_id}

    try:
        headers = get_n8n_headers()
    except Exception as exc:
        log_error('delete_workflow', 'AUTH_ERROR', 'n8n api key missing', str(exc))
        return {'status': 'failed', 'workflow_id': workflow_id}

    try:
        resp = httpx.delete(f'{N8N_BASE}/workflows/{workflow_id}', headers=headers, timeout=20.0)
        resp.raise_for_status()
        log_agent('delete_workflow', f'workflow_id={workflow_id}', 'success')
        return {'status': 'deleted', 'workflow_id': workflow_id}
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code if exc.response is not None else 0
        err_type = 'AUTH_ERROR' if code in (401, 403) else ('RATE_LIMIT' if code == 429 else 'API_ERROR')
        log_error('delete_workflow', err_type, 'n8n delete workflow failed', str(exc))
        return {'status': 'failed', 'workflow_id': workflow_id}
    except Exception as exc:
        log_error('delete_workflow', 'NETWORK_ERROR', 'n8n delete workflow failed', str(exc))
        return {'status': 'failed', 'workflow_id': workflow_id}


def build_hunter_workflow() -> dict:
    system_context = f"""
You are Forge, SATURN's automation engineer.
Follow these n8n workflow patterns:
{read_skill('n8n-workflow-patterns')[:2000]}

Validation rules:
{read_skill('n8n-validation-expert')[:1000]}
"""
    _ = system_context

    nodes = [
        {
            'id': '1',
            'name': 'Schedule Trigger',
            'type': 'n8n-nodes-base.scheduleTrigger',
            'typeVersion': 1.1,
            'position': [0, 0],
            'parameters': {
                'rule': {
                    'interval': [
                        {
                            'field': 'cronExpression',
                            'expression': '0 9 * * *',
                        }
                    ]
                }
            },
        },
        {
            'id': '2',
            'name': 'SerpAPI Search',
            'type': 'n8n-nodes-base.httpRequest',
            'typeVersion': 4.2,
            'position': [200, 0],
            'parameters': {
                'method': 'GET',
                'url': 'https://serpapi.com/search.json',
                'sendQuery': True,
                'queryParameters': {
                    'parameters': [
                        {'name': 'q', 'value': '={{$env.SERPAPI_QUERY}}'},
                        {'name': 'api_key', 'value': '={{$env.SERPAPI_KEY}}'},
                        {'name': 'num', 'value': '10'},
                    ]
                },
            },
        },
        {
            'id': '3',
            'name': 'Extract Leads',
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [400, 0],
            'parameters': {
                'jsCode': (
                    "const results = $input.first().json.organic_results || [];\n"
                    "return results.slice(0,10).map(r => ({\n"
                    "  json: {\n"
                    "    title: r.title || '',\n"
                    "    link: r.link || '',\n"
                    "    snippet: r.snippet || '',\n"
                    "    domain: (r.link||'').replace(/https?:\\/\\/(www\\.)?/,'').split('/')[0]\n"
                    "  }\n"
                    "}));"
                ),
            },
        },
        {
            'id': '4',
            'name': 'Add Lead to SATURN',
            'type': 'n8n-nodes-base.httpRequest',
            'typeVersion': 4.2,
            'position': [600, 0],
            'parameters': {
                'method': 'POST',
                'url': 'http://localhost:18789/tools/exec',
                'sendBody': True,
                'specifyBody': 'json',
                'jsonBody': (
                    '{\n'
                    '  "tool": "add_lead",\n'
                    '  "params": {\n'
                    '    "name": "={{$json.title}}",\n'
                    '    "company": "={{$json.domain}}",\n'
                    '    "source": "serpapi_n8n",\n'
                    '    "notes": "={{$json.snippet}}",\n'
                    '    "website": "={{$json.link}}"\n'
                    '  }\n'
                    '}'
                ),
            },
        },
        {
            'id': '5',
            'name': 'Log Result',
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [800, 0],
            'parameters': {
                'jsCode': (
                    'const items = $input.all();\n'
                    'console.log(`Hunter workflow: processed ${items.length} leads`);\n'
                    'return [{json: {processed: items.length, timestamp: new Date().toISOString()}}];'
                )
            },
        },
    ]

    connections = {
        'Schedule Trigger': {
            'main': [[{'node': 'SerpAPI Search', 'type': 'main', 'index': 0, 'nodeId': '2'}]]
        },
        'SerpAPI Search': {
            'main': [[{'node': 'Extract Leads', 'type': 'main', 'index': 0, 'nodeId': '3'}]]
        },
        'Extract Leads': {
            'main': [[{'node': 'Add Lead to SATURN', 'type': 'main', 'index': 0, 'nodeId': '4'}]]
        },
        'Add Lead to SATURN': {
            'main': [[{'node': 'Log Result', 'type': 'main', 'index': 0, 'nodeId': '5'}]]
        },
    }

    workflow = {
        'name': 'Hunter Daily Lead Sourcing',
        'nodes': nodes,
        'connections': connections,
        'settings': {},
    }

    valid, reason = validate_workflow_json(workflow)
    if not valid:
        workflow['nodes'] = [dict(node) for node in workflow['nodes']]
        for i, node in enumerate(workflow['nodes'], start=1):
            node['id'] = str(i)
            node['position'] = [200 * (i - 1), 0]
        valid, reason = validate_workflow_json(workflow)
        if not valid:
            log_error('build_hunter_workflow', 'LOGIC_ERROR', 'workflow validation failed', reason)

    return workflow


def read_skill(skill_name: str) -> str:
    target = SKILLS_DIR / skill_name / 'SKILL.md'
    if not target.exists():
        return ''
    try:
        return target.read_text(encoding='utf-8')
    except Exception as exc:
        log_error('read_skill', 'API_ERROR', 'failed reading skill file', str(exc))
        return ''


if __name__ == '__main__':
    import sqlite3

    conn = sqlite3.connect('/home/navin/Workspace/Saturn/database/saturn.db')
    WORKFLOWS_DIR.mkdir(parents=True, exist_ok=True)
    wf = build_hunter_workflow()
    valid, reason = validate_workflow_json(wf)
    print(f'Hunter workflow validation: {valid} | {reason}')
    if valid:
        result = deploy_workflow(wf, activate=False)
        print(f'Deploy result: {result}')
    conn.close()
