"""Kafka consumers for statements-service.

One handler per subscribed topic. Handlers are best-effort logging plus
audit — services override this file to implement real cross-domain behavior.
"""
from __future__ import annotations

import logging

from healthcare_common.audit import emit_audit

log = logging.getLogger("statements-service.consumers")


def register(svc) -> None:
    bus = svc.bus

    @bus.on("invoice.issued")
    def _on_invoice_issued(envelope: dict) -> None:
        log.info("statements-service: received invoice.issued id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.invoice.issued", actor="system:statements-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("invoice.paid")
    def _on_invoice_paid(envelope: dict) -> None:
        log.info("statements-service: received invoice.paid id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.invoice.paid", actor="system:statements-service",
                   target=None, details={"envelope_id": envelope.get("id")})

