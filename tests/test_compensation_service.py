import json

from src.app.compensation_service import CompensationService


def test_compensation_service_falls_back_to_jsonl(tmp_path):
    queue_file = tmp_path / "compensation.jsonl"
    service = CompensationService(storage=None, queue_file=queue_file)

    task = service.record(
        operation_type="BUY_CASH_DEDUCT_FAILED",
        account="test",
        payload={"cash_delta": -10},
        error="failed",
        related_record_id="rec1",
    )

    rows = [json.loads(line) for line in queue_file.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["task_id"] == task.task_id
    assert rows[0]["status"] == "PENDING"
    assert rows[0]["payload"]["cash_delta"] == -10
