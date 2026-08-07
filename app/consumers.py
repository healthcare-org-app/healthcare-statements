"""Kafka consumers for statements-service.

One handler per subscribed topic. Real handlers write to this service's own
database and/or publish follow-up events; stub handlers just log + audit.
"""
from __future__ import annotations

import logging

from psycopg.types.json import Json

from healthcare_common.audit import emit_audit

log = logging.getLogger("statements-service.consumers")

TABLE = "statements"


def register(svc) -> None:
    bus = svc.bus
    db = svc.db
    clients = svc.clients

    @bus.on("invoice.issued")
    def _on_invoice_issued(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"invoice_id": data.get("invoice_id"),
                                      "patient_id": data.get("patient_id"),
                                      "amount": data.get("amount"),
                                      "issued_at": envelope.get("occurred_at")}),))
        except Exception as e:
            log.exception("statements-service/invoice.issued handler failed: %s", e)
        emit_audit(bus, action="consume.invoice.issued", actor="system:statements-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("invoice.paid")
    def _on_invoice_paid(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    inv_id = data.get("invoice_id")
                    if inv_id:
                        db.execute(f"UPDATE {TABLE} SET data = data || %s "
                                   f"WHERE data->>'invoice_id' = %s",
                                   (Json({"paid_at": envelope.get("occurred_at")}), str(inv_id)))
        except Exception as e:
            log.exception("statements-service/invoice.paid handler failed: %s", e)
        emit_audit(bus, action="consume.invoice.paid", actor="system:statements-service",
                   target=None, details={"envelope_id": envelope.get("id")})

