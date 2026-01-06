# airflow/dags/census_pipeline_per_task_fixed.py
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.hooks.base import BaseHook
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule

LOG = logging.getLogger(__name__)

# ---------------- CONFIG - EDIT ----------------
DAG_ID = "census_full_pipeline"
DATABRICKS_CONN_ID = "databricks_default"
EXISTING_CLUSTER_ID = "1231-101229-yohzziqj"  # <- update
ALERT_EMAILS = ["dataops@example.com"]

NOTEBOOKS = {
    "register": "/Workspace/Users/keshavuk.150@gmail.com/register_raw_files_to_registry",
    "bronze_ingest": "/Workspace/Users/keshavuk.150@gmail.com/bronze_ingest",
    "bronze_validation": "/Workspace/Users/keshavuk.150@gmail.com/bronze_validation",
    "reset_registry": "/Workspace/Users/keshavuk.150@gmail.com/reset_registry_for_batch",
    "silver_conform": "/Workspace/Users/keshavuk.150@gmail.com/silver_conform",
    "silver_validation": "/Workspace/Users/keshavuk.150@gmail.com/silver_validation",
    "silver_optimize": "/Workspace/Users/keshavuk.150@gmail.com/silver_optimize",
    "gold_materialize": "/Workspace/Users/keshavuk.150@gmail.com/gold_materialize",
    "gold_validation": "/Workspace/Users/keshavuk.150@gmail.com/gold_validation",
}

# which notebooks MUST return structured JSON via dbutils.notebook.exit(json.dumps(...))
EXPECT_JSON = {
    "register": False,
    "bronze_ingest": False,
    "bronze_validation": True,
    "reset_registry": False,
    "silver_conform": False,
    "silver_validation": True,
    "silver_optimize": False,
    "gold_materialize": False,
    "gold_validation": True,
}

MAX_REINGEST_ATTEMPTS = 2
POLL_INTERVAL_SECONDS = 6
RUN_TIMEOUT_SECONDS = 60 * 60  # 1 hour
PRELIGHT_TIMEOUT = 10

# ---------------- DAG default args ----------------
default_args = {
    "owner": "census-team",
    "depends_on_past": False,
    "email_on_failure": False,  # handle failure emails via callback
    "email_on_retry": False,
    "retries": 2,
    "start_date": datetime(2025, 1, 1),
}

# ---------------- DAG Failure Callback ----------------
def dag_failure_callback(context):
    """Send email on DAG failure."""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    
    subject = f"[ALERT] {dag_id} failed on task {task_id}"
    html_content = f"""
    <h3>Pipeline Failure Alert</h3>
    <p>DAG: {dag_id}</p>
    <p>Failed task: {task_id}</p>
    <p>Execution date: {execution_date}</p>
    <p>Log: {context.get('task_instance').log_url}</p>
    """
    
    send_email(to=ALERT_EMAILS, subject=subject, html_content=html_content)

# ---------------- Helpers: Databricks REST ----------------
def get_databricks_conn() -> (str, str):
    conn = BaseHook.get_connection(DATABRICKS_CONN_ID)
    extra = {}
    try:
        extra = conn.extra_dejson or {}
    except Exception:
        extra = {}
    host = conn.host or extra.get("host") or extra.get("workspace_url")
    token = conn.password or extra.get("token") or extra.get("access_token")
    if not host or not token:
        raise RuntimeError("Databricks connection missing host/token in Airflow connection")
    if host.endswith("/"):
        host = host[:-1]
    return host, token

def fetch_run_body_until_finished(run_id: int, timeout_seconds: int = RUN_TIMEOUT_SECONDS) -> Dict[str, Any]:
    host, token = get_databricks_conn()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{host}/api/2.1/jobs/runs/get"
    start = time.time()
    while True:
        r = requests.get(url, headers=headers, params={"run_id": run_id}, timeout=30)
        if r.status_code != 200:
            LOG.warning("runs/get returned %s for run %s: %s", r.status_code, run_id, r.text)
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        body = r.json()
        state = (body.get("state") or {})
        life = state.get("life_cycle_state")
        result_state = state.get("result_state")
        LOG.debug("run %s lifecycle=%s result=%s", run_id, life, result_state)
        if life in ("TERMINATED","INTERNAL_ERROR","SKIPPED"):
            return body
        if time.time() - start > timeout_seconds:
            raise AirflowFailException(f"run {run_id} timed out after {timeout_seconds}s")
        time.sleep(POLL_INTERVAL_SECONDS)

def try_get_output_via_get_output(run_id: int) -> Optional[str]:
    host, token = get_databricks_conn()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{host}/api/2.0/jobs/runs/get-output"
    try:
        r = requests.get(url, headers=headers, params={"run_id": run_id}, timeout=30)
        if r.status_code == 200:
            return (r.json().get("notebook_output") or {}).get("result")
    except Exception:
        LOG.debug("get-output fallback failed for run %s", run_id)
    return None

def parse_notebook_result(run_body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    nb_out = (run_body.get("notebook_output") or {}).get("result")
    if not nb_out:
        nb_out = try_get_output_via_get_output(run_body.get("run_id"))
    if not nb_out:
        return None
    try:
        return json.loads(nb_out)
    except Exception:
        LOG.exception("Failed to parse notebook_output.result JSON for run_id=%s raw=%s", run_body.get("run_id"), nb_out)
        return None

# ---------------- Submit + get_result factories ----------------
def make_databricks_submit(task_id: str, notebook_path: str):
    """
    DatabricksSubmitRunOperator that pushes its response to XCom (do_xcom_push=True).
    """
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        existing_cluster_id=EXISTING_CLUSTER_ID,
        notebook_task={"notebook_path": notebook_path, "base_parameters": {}},
        timeout_seconds=RUN_TIMEOUT_SECONDS,
        do_xcom_push=True,
    )

def make_get_result_callable(submit_task_id: str, expect_json: bool):
    """
    Callable for a PythonOperator that:
      - reads submit_task_id XCom to get run_id
      - polls Databricks runs/get until finished
      - parses notebook_output.result JSON if present
      - pushes two XCom keys:
          - "{submit_task_id}_run_body" -> full run body
          - "{submit_task_id}_result" -> parsed dict (or {} if absent)
      - if expect_json and run succeeded but no structured JSON -> raise AirflowFailException (with run_id + run_page_url)
    """
    def _callable(**context):
        ti = context["ti"]
        
        # Get the XCom value - DatabricksSubmitRunOperator pushes the run_id directly
        # It might be in different formats depending on version
        run_id = None
        
        # Try multiple ways to get the run_id
        xcom_val = ti.xcom_pull(task_ids=submit_task_id)
        
        # Debug logging
        LOG.info(f"XCom value from {submit_task_id}: {xcom_val}, type: {type(xcom_val)}")
        
        # Handle different possible return formats
        if isinstance(xcom_val, dict):
            # Check for nested structure
            if "run_id" in xcom_val:
                run_id = xcom_val["run_id"]
            elif "run" in xcom_val and isinstance(xcom_val["run"], dict):
                run_id = xcom_val["run"].get("run_id")
            # Some versions return the run_id as a dict key directly
            elif "databricks_run_id" in xcom_val:
                run_id = xcom_val["databricks_run_id"]
        elif isinstance(xcom_val, int):
            run_id = xcom_val
        elif isinstance(xcom_val, str):
            try:
                parsed = json.loads(xcom_val)
                if isinstance(parsed, dict):
                    run_id = parsed.get("run_id")
            except json.JSONDecodeError:
                # If it's just a string containing a number
                try:
                    run_id = int(xcom_val)
                except ValueError:
                    run_id = None
        
        # If still not found, try pulling with specific key
        if run_id is None:
            # Try with key='run_id' (some versions use this)
            run_id = ti.xcom_pull(task_ids=submit_task_id, key='run_id')
            if run_id is None:
                # Try with key='return_value' (default XCom key)
                run_id = ti.xcom_pull(task_ids=submit_task_id, key='return_value')
        
        # Last resort: check if the operator returned the run_id directly
        if run_id is None and xcom_val is not None:
            # Some versions return the run_id directly as return value
            try:
                run_id = int(xcom_val)
            except (ValueError, TypeError):
                pass
        
        if run_id is None:
            LOG.error("No run_id found in XCom from %s. Raw XCom value: %s", 
                      submit_task_id, str(xcom_val)[:500])
            # Try one more approach - check the operator's documentation
            # Sometimes the operator returns a dict with run_id in different structure
            if isinstance(xcom_val, dict):
                LOG.error("Available keys in XCom dict: %s", list(xcom_val.keys()))
            raise AirflowFailException(f"No run_id found in XCom from {submit_task_id}")
        
        run_id = int(run_id)
        LOG.info("Polling Databricks run_id=%s for %s", run_id, submit_task_id)
        run_body = fetch_run_body_until_finished(run_id)
        ti.xcom_push(key=f"{submit_task_id}_run_body", value=run_body)
        parsed = parse_notebook_result(run_body)
        if parsed is None:
            # If JSON required and run marked SUCCESS -> surface clear error
            state = (run_body.get("state") or {})
            result_state = state.get("result_state")
            run_page_url = run_body.get("run_page_url")
            if expect_json and result_state == "SUCCESS":
                msg = {
                    "error": "NO_STRUCTURED_NOTEBOOK_RESULT",
                    "message": f"Notebook {submit_task_id} finished SUCCESS but did not return structured JSON via dbutils.notebook.exit(...).",
                    "run_id": run_id,
                    "run_page_url": run_page_url
                }
                LOG.error("Expected JSON but missing for run %s", run_id)
                raise AirflowFailException(json.dumps(msg))
            # not required or not successful: push empty dict so downstream can continue
            ti.xcom_push(key=f"{submit_task_id}_result", value={})
            LOG.info("No structured JSON found for %s; pushed empty result and saved run body", submit_task_id)
            return {}
        # parsed JSON present
        ti.xcom_push(key=f"{submit_task_id}_result", value=parsed)
        LOG.info("Parsed JSON pushed for %s (run_id=%s)", submit_task_id, run_id)
        return parsed
    return _callable

# ---------------- Reingest handler (single node) ----------------
def reingest_handler_callable(**context):
    """
    Single operator that runs up to MAX_REINGEST_ATTEMPTS:
       for attempt in 1..N:
           - submit reset_registry, poll
           - submit bronze_ingest, poll
           - submit bronze_validation, poll -> parse JSON
           - if validation parsed JSON and validated==True -> success and return that JSON
    If all attempts fail -> raise AirflowFailException
    """
    ti = context["ti"]
    host, token = get_databricks_conn()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    submit_url = f"{host}/api/2.1/jobs/runs/submit"
    get_url = f"{host}/api/2.1/jobs/runs/get"

    def submit_and_wait_notebook(path: str, base_parameters: Optional[Dict[str,Any]] = None, timeout=RUN_TIMEOUT_SECONDS):
        payload = {
            "run_name": f"airflow-reingest-{path.split('/')[-1]}-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
            "existing_cluster_id": EXISTING_CLUSTER_ID,
            "notebook_task": {"notebook_path": path, "base_parameters": base_parameters or {}}
        }
        LOG.info("Submitting reingest notebook %s", path)
        r = requests.post(submit_url, headers=headers, json=payload, timeout=30)
        if r.status_code not in (200,201):
            raise RuntimeError(f"Failed to submit {path}: {r.status_code} {r.text}")
        run_id = int(r.json().get("run_id"))
        # poll
        start = time.time()
        while True:
            r2 = requests.get(get_url, headers=headers, params={"run_id": run_id}, timeout=30)
            if r2.status_code != 200:
                LOG.warning("runs/get non-200 for run %s: %s %s", run_id, r2.status_code, r2.text)
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            body = r2.json()
            life = (body.get("state") or {}).get("life_cycle_state")
            if life in ("TERMINATED","INTERNAL_ERROR","SKIPPED"):
                return body
            if time.time() - start > timeout:
                raise RuntimeError(f"run {run_id} timed out after {timeout}s")
            time.sleep(POLL_INTERVAL_SECONDS)

    attempt = 0
    last_report = None
    while attempt < MAX_REINGEST_ATTEMPTS:
        attempt += 1
        LOG.info("Reingest attempt %d/%d", attempt, MAX_REINGEST_ATTEMPTS)
        # reset_registry best-effort
        try:
            body = submit_and_wait_notebook(NOTEBOOKS["reset_registry"], timeout=300)
            LOG.info("reset_registry finished (attempt %d)", attempt)
        except Exception as e:
            LOG.exception("reset_registry failed: %s", e)
        # bronze_ingest
        try:
            body = submit_and_wait_notebook(NOTEBOOKS["bronze_ingest"])
            LOG.info("bronze_ingest finished (attempt %d)", attempt)
        except Exception as e:
            LOG.exception("bronze_ingest failed: %s", e)
            continue
        # bronze_validation
        try:
            body = submit_and_wait_notebook(NOTEBOOKS["bronze_validation"])
            parsed = parse_notebook_result(body)
            last_report = parsed or {"note":"no_parsed_json","run_body":body}
            if parsed and parsed.get("validated", False):
                ti.xcom_push(key="reingest_final_report", value=parsed)
                LOG.info("Reingest succeeded on attempt %d", attempt)
                return parsed
            else:
                LOG.warning("Validation failed on attempt %d (validated=%s)", attempt, parsed.get("validated") if parsed else None)
                continue
        except AirflowFailException as e:
            # means validation failed due to missing required JSON or run error
            LOG.exception("bronze_validation during reingest attempt failed: %s", e)
            last_report = {"error": str(e)}
            continue
        except Exception as e:
            LOG.exception("Unexpected error during bronze_validation reingest attempt: %s", e)
            last_report = {"error": str(e)}
            continue

    # exhausted
    LOG.error("Reingest failed after %d attempts; last_report=%s", MAX_REINGEST_ATTEMPTS, last_report)
    raise AirflowFailException(f"Reingest failed after {MAX_REINGEST_ATTEMPTS} attempts; last_report={json.dumps(last_report)[:1500]}")

# ---------------- Silver reingest handler ----------------
def silver_reingest_handler_callable(**context):
    """
    Handle silver validation failures by:
      1. Re-running silver_conform
      2. Re-running silver_validation
      3. Checking validation result
    """
    ti = context["ti"]
    host, token = get_databricks_conn()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    submit_url = f"{host}/api/2.1/jobs/runs/submit"
    get_url = f"{host}/api/2.1/jobs/runs/get"

    def submit_and_wait_notebook(path: str, base_parameters: Optional[Dict[str,Any]] = None, timeout=RUN_TIMEOUT_SECONDS):
        payload = {
            "run_name": f"airflow-silver-reingest-{path.split('/')[-1]}-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
            "existing_cluster_id": EXISTING_CLUSTER_ID,
            "notebook_task": {"notebook_path": path, "base_parameters": base_parameters or {}}
        }
        LOG.info("Submitting silver reingest notebook %s", path)
        r = requests.post(submit_url, headers=headers, json=payload, timeout=30)
        if r.status_code not in (200,201):
            raise RuntimeError(f"Failed to submit {path}: {r.status_code} {r.text}")
        run_id = int(r.json().get("run_id"))
        # poll
        start = time.time()
        while True:
            r2 = requests.get(get_url, headers=headers, params={"run_id": run_id}, timeout=30)
            if r2.status_code != 200:
                LOG.warning("runs/get non-200 for run %s: %s %s", run_id, r2.status_code, r2.text)
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            body = r2.json()
            life = (body.get("state") or {}).get("life_cycle_state")
            if life in ("TERMINATED","INTERNAL_ERROR","SKIPPED"):
                return body
            if time.time() - start > timeout:
                raise RuntimeError(f"run {run_id} timed out after {timeout}s")
            time.sleep(POLL_INTERVAL_SECONDS)

    attempt = 0
    last_report = None
    while attempt < MAX_REINGEST_ATTEMPTS:
        attempt += 1
        LOG.info("Silver reingest attempt %d/%d", attempt, MAX_REINGEST_ATTEMPTS)
        
        # silver_conform (reprocess)
        try:
            body = submit_and_wait_notebook(NOTEBOOKS["silver_conform"])
            LOG.info("silver_conform finished (attempt %d)", attempt)
        except Exception as e:
            LOG.exception("silver_conform failed: %s", e)
            continue
        
        # silver_validation
        try:
            body = submit_and_wait_notebook(NOTEBOOKS["silver_validation"])
            parsed = parse_notebook_result(body)
            last_report = parsed or {"note":"no_parsed_json","run_body":body}
            if parsed and parsed.get("validated", False):
                ti.xcom_push(key="silver_reingest_final_report", value=parsed)
                LOG.info("Silver reingest succeeded on attempt %d", attempt)
                return parsed
            else:
                LOG.warning("Silver validation failed on attempt %d (validated=%s)", attempt, parsed.get("validated") if parsed else None)
                continue
        except AirflowFailException as e:
            LOG.exception("silver_validation during reingest attempt failed: %s", e)
            last_report = {"error": str(e)}
            continue
        except Exception as e:
            LOG.exception("Unexpected error during silver_validation reingest attempt: %s", e)
            last_report = {"error": str(e)}
            continue

    # exhausted
    LOG.error("Silver reingest failed after %d attempts; last_report=%s", MAX_REINGEST_ATTEMPTS, last_report)
    raise AirflowFailException(f"Silver reingest failed after {MAX_REINGEST_ATTEMPTS} attempts; last_report={json.dumps(last_report)[:1500]}")


def gold_reingest_handler_callable(**context):
    """
    Handle gold validation failures by:
      1. Re-running gold_materialize
      2. Re-running gold_validation
      3. Checking validation result
    """
    ti = context["ti"]
    host, token = get_databricks_conn()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    submit_url = f"{host}/api/2.1/jobs/runs/submit"
    get_url = f"{host}/api/2.1/jobs/runs/get"

    def submit_and_wait_notebook(path: str, base_parameters: Optional[Dict[str,Any]] = None, timeout=RUN_TIMEOUT_SECONDS):
        payload = {
            "run_name": f"airflow-gold-reingest-{path.split('/')[-1]}-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
            "existing_cluster_id": EXISTING_CLUSTER_ID,
            "notebook_task": {"notebook_path": path, "base_parameters": base_parameters or {}}
        }
        LOG.info("Submitting gold reingest notebook %s", path)
        r = requests.post(submit_url, headers=headers, json=payload, timeout=30)
        if r.status_code not in (200,201):
            raise RuntimeError(f"Failed to submit {path}: {r.status_code} {r.text}")
        run_id = int(r.json().get("run_id"))
        # poll
        start = time.time()
        while True:
            r2 = requests.get(get_url, headers=headers, params={"run_id": run_id}, timeout=30)
            if r2.status_code != 200:
                LOG.warning("runs/get non-200 for run %s: %s %s", run_id, r2.status_code, r2.text)
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            body = r2.json()
            life = (body.get("state") or {}).get("life_cycle_state")
            if life in ("TERMINATED","INTERNAL_ERROR","SKIPPED"):
                return body
            if time.time() - start > timeout:
                raise RuntimeError(f"run {run_id} timed out after {timeout}s")
            time.sleep(POLL_INTERVAL_SECONDS)

    attempt = 0
    last_report = None
    while attempt < MAX_REINGEST_ATTEMPTS:
        attempt += 1
        LOG.info("Gold reingest attempt %d/%d", attempt, MAX_REINGEST_ATTEMPTS)
        
        # gold_materialize (reprocess)
        try:
            body = submit_and_wait_notebook(NOTEBOOKS["gold_materialize"])
            LOG.info("gold_materialize finished (attempt %d)", attempt)
        except Exception as e:
            LOG.exception("gold_materialize failed: %s", e)
            continue
        
        # gold_validation
        try:
            body = submit_and_wait_notebook(NOTEBOOKS["gold_validation"])
            parsed = parse_notebook_result(body)
            last_report = parsed or {"note":"no_parsed_json","run_body":body}
            if parsed and parsed.get("validated", False):
                ti.xcom_push(key="gold_reingest_final_report", value=parsed)
                LOG.info("Gold reingest succeeded on attempt %d", attempt)
                return parsed
            else:
                LOG.warning("Gold validation failed on attempt %d (validated=%s)", attempt, parsed.get("validated") if parsed else None)
                continue
        except AirflowFailException as e:
            LOG.exception("gold_validation during reingest attempt failed: %s", e)
            last_report = {"error": str(e)}
            continue
        except Exception as e:
            LOG.exception("Unexpected error during gold_validation reingest attempt: %s", e)
            last_report = {"error": str(e)}
            continue

    # exhausted
    LOG.error("Gold reingest failed after %d attempts; last_report=%s", MAX_REINGEST_ATTEMPTS, last_report)
    raise AirflowFailException(f"Gold reingest failed after {MAX_REINGEST_ATTEMPTS} attempts; last_report={json.dumps(last_report)[:1500]}")




# ---------------- Branch decision callable ----------------
def decide_on_bronze_validation(**context):
    ti = context["ti"]
    res = ti.xcom_pull(task_ids="bronze_validation_submit", key="bronze_validation_submit_result")
    # Try common alternatives if necessary
    if not res:
        res = ti.xcom_pull(task_ids="bronze_validation_get_result", key="bronze_validation_submit_result")
    if not res:
        res = ti.xcom_pull(task_ids="bronze_validation_get_result")
    if not res:
        # last attempt: check submit task raw XCom
        res = ti.xcom_pull(task_ids="bronze_validation_submit")
    if not res:
        raise AirflowFailException("No parsed validation result found in XCom for bronze_validation.")
    validated = bool(res.get("validated", False))
    LOG.info("Branch decision: validated=%s", validated)
    return "continue_to_silver_start" if validated else "reingest_handler"

def decide_on_silver_validation(**context):
    ti = context["ti"]
    res = ti.xcom_pull(task_ids="silver_validation_submit", key="silver_validation_submit_result")
    # Try common alternatives if necessary
    if not res:
        res = ti.xcom_pull(task_ids="silver_validation_get_result", key="silver_validation_submit_result")
    if not res:
        res = ti.xcom_pull(task_ids="silver_validation_get_result")
    if not res:
        # last attempt: check submit task raw XCom
        res = ti.xcom_pull(task_ids="silver_validation_submit")
    if not res:
        raise AirflowFailException("No parsed validation result found in XCom for silver_validation.")
    validated = bool(res.get("validated", False))
    LOG.info("Silver validation branch decision: validated=%s", validated)
    return "continue_to_gold_start" if validated else "silver_reingest_handler"

def decide_on_gold_validation(**context):
    ti = context["ti"]
    res = ti.xcom_pull(task_ids="gold_validation_submit", key="gold_validation_submit_result")
    # Try common alternatives if necessary
    if not res:
        res = ti.xcom_pull(task_ids="gold_validation_get_result", key="gold_validation_submit_result")
    if not res:
        res = ti.xcom_pull(task_ids="gold_validation_get_result")
    if not res:
        # last attempt: check submit task raw XCom
        res = ti.xcom_pull(task_ids="gold_validation_submit")
    if not res:
        raise AirflowFailException("No parsed validation result found in XCom for gold_validation.")
    validated = bool(res.get("validated", False))
    LOG.info("Gold validation branch decision: validated=%s", validated)
    # UPDATE 1: Return "check_state" instead of "notify_success"
    return "check_state" if validated else "gold_reingest_handler" 

# ---------------- DAG definition ----------------
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["census","per-task-fixed"],
    on_failure_callback=dag_failure_callback,  # Add failure callback
) as dag:

    # Preflight to ensure notebooks exist (fail fast)
    def preflight_callable(**context):
        host, token = get_databricks_conn()
        headers = {"Authorization": f"Bearer {token}"}
        missing = []
        for k, p in NOTEBOOKS.items():
            url = f"{host}/api/2.0/workspace/get-status"
            r = requests.get(url, headers=headers, params={"path": p}, timeout=PRELIGHT_TIMEOUT)
            if r.status_code != 200:
                LOG.error("Preflight missing: %s -> %s %s", p, r.status_code, r.text)
                missing.append(p)
        if missing:
            raise AirflowFailException(f"Preflight failed; missing notebooks: {missing}")
        return True

    preflight = PythonOperator(task_id="preflight_check", python_callable=preflight_callable)

    # register
    register_submit = make_databricks_submit("register_submit", NOTEBOOKS["register"])
    register_get = PythonOperator(task_id="register_get_result", python_callable=make_get_result_callable("register_submit", EXPECT_JSON["register"]))

    # bronze ingest
    bronze_ingest_submit = make_databricks_submit("bronze_ingest_submit", NOTEBOOKS["bronze_ingest"])
    bronze_ingest_get = PythonOperator(task_id="bronze_ingest_get_result", python_callable=make_get_result_callable("bronze_ingest_submit", EXPECT_JSON["bronze_ingest"]))

    # bronze validation
    bronze_validation_submit = make_databricks_submit("bronze_validation_submit", NOTEBOOKS["bronze_validation"])
    bronze_validation_get = PythonOperator(task_id="bronze_validation_get_result", python_callable=make_get_result_callable("bronze_validation_submit", EXPECT_JSON["bronze_validation"]))

    # bronze branch
    decide = BranchPythonOperator(task_id="decide_on_bronze_validation", python_callable=decide_on_bronze_validation)

    # continue to silver start
    continue_to_silver_start = PythonOperator(task_id="continue_to_silver_start", python_callable=lambda **ctx: LOG.info("continuing to silver"), trigger_rule=TriggerRule.NONE_FAILED)

    # reingest handler (single node)
    reingest_handler = PythonOperator(task_id="reingest_handler", python_callable=reingest_handler_callable)

    # Silver & Gold visible tasks (submit + get)
    silver_conform_submit = make_databricks_submit("silver_conform_submit", NOTEBOOKS["silver_conform"])
    silver_conform_get = PythonOperator(task_id="silver_conform_get_result", python_callable=make_get_result_callable("silver_conform_submit", EXPECT_JSON["silver_conform"]))

    silver_validation_submit = make_databricks_submit("silver_validation_submit", NOTEBOOKS["silver_validation"])
    silver_validation_get = PythonOperator(task_id="silver_validation_get_result", python_callable=make_get_result_callable("silver_validation_submit", EXPECT_JSON["silver_validation"]))

    # silver validation branch
    silver_validation_decide = BranchPythonOperator(task_id="decide_on_silver_validation", python_callable=decide_on_silver_validation)

    # continue to gold start
    continue_to_gold_start = PythonOperator(task_id="continue_to_gold_start", python_callable=lambda **ctx: LOG.info("continuing to gold"), trigger_rule=TriggerRule.NONE_FAILED)

    # silver reingest handler
    silver_reingest_handler = PythonOperator(task_id="silver_reingest_handler", python_callable=silver_reingest_handler_callable)

    silver_opt_submit = make_databricks_submit("silver_optimize_submit", NOTEBOOKS["silver_optimize"])
    silver_opt_get = PythonOperator(task_id="silver_optimize_get_result", python_callable=make_get_result_callable("silver_optimize_submit", EXPECT_JSON["silver_optimize"]))

    gold_mat_submit = make_databricks_submit("gold_materialize_submit", NOTEBOOKS["gold_materialize"])
    gold_mat_get = PythonOperator(task_id="gold_materialize_get_result", python_callable=make_get_result_callable("gold_materialize_submit", EXPECT_JSON["gold_materialize"]))

    gold_validation_submit = make_databricks_submit("gold_validation_submit", NOTEBOOKS["gold_validation"])
    gold_validation_get = PythonOperator(task_id="gold_validation_get_result", python_callable=make_get_result_callable("gold_validation_submit", EXPECT_JSON["gold_validation"]))
    
    # gold validation branch
    gold_validation_decide = BranchPythonOperator(task_id="decide_on_gold_validation", python_callable=decide_on_gold_validation)
    
    # gold reingest handler
    gold_reingest_handler = PythonOperator(task_id="gold_reingest_handler", python_callable=gold_reingest_handler_callable)

    # UPDATE 2: Added check_state dummy task
    check_state = PythonOperator(task_id="check_state", python_callable=lambda **ctx: LOG.info("Final validation state check passed."))

    # UPDATE 3: Changed trigger_rule to ONE_SUCCESS
    notify_success = PythonOperator(
        task_id="notify_success", 
        python_callable=lambda **ctx: send_email(to=ALERT_EMAILS, subject=f"[OK] {DAG_ID} succeeded", html_content="Pipeline succeeded"), 
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # ---------------- wiring ----------------
    preflight >> register_submit >> register_get >> bronze_ingest_submit >> bronze_ingest_get >> bronze_validation_submit >> bronze_validation_get >> decide

    # bronze branch wires
    decide >> continue_to_silver_start
    decide >> reingest_handler

    # reingest_handler on success continues to the same continue node
    reingest_handler >> continue_to_silver_start

    # continue to silver
    continue_to_silver_start >> silver_conform_submit >> silver_conform_get >> silver_validation_submit >> silver_validation_get >> silver_validation_decide

    # silver branch wires
    silver_validation_decide >> continue_to_gold_start
    silver_validation_decide >> silver_reingest_handler

    # silver reingest handler on success continues to gold
    silver_reingest_handler >> continue_to_gold_start

    # continue to gold and beyond
    continue_to_gold_start >> silver_opt_submit >> silver_opt_get >> gold_mat_submit >> gold_mat_get >> gold_validation_submit >> gold_validation_get >> gold_validation_decide

    # UPDATE 4: Updated wiring at the end
    # Happy path: decide -> check_state -> notify_success
    gold_validation_decide >> check_state
    check_state >> notify_success

    # Recovery path: decide -> reingest -> notify_success
    gold_validation_decide >> gold_reingest_handler
    gold_reingest_handler >> notify_success