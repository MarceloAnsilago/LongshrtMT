from __future__ import annotations

import uuid

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("operacoes", "0002_operationmt5trade"),
    ]

    operations = [
        migrations.CreateModel(
            name="MT5AuditEvent",
            fields=[
                (
                    "id",
                    models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID"),
                ),
                (
                    "request_id",
                    models.UUIDField(default=uuid.uuid4, editable=False, unique=True),
                ),
                (
                    "leg",
                    models.CharField(max_length=16),
                ),
                (
                    "symbol",
                    models.CharField(max_length=32),
                ),
                (
                    "volume",
                    models.FloatField(),
                ),
                (
                    "action",
                    models.CharField(
                        choices=[("OPEN", "Abertura"), ("CLOSE", "Encerramento"), ("MODIFY", "Modificação")],
                        max_length=16,
                    ),
                ),
                (
                    "reason",
                    models.CharField(max_length=64),
                ),
                (
                    "request_payload",
                    models.JSONField(blank=True, null=True),
                ),
                (
                    "response_payload",
                    models.JSONField(blank=True, null=True),
                ),
                (
                    "retcode",
                    models.IntegerField(blank=True, null=True),
                ),
                (
                    "order",
                    models.BigIntegerField(blank=True, null=True),
                ),
                (
                    "deal",
                    models.BigIntegerField(blank=True, null=True),
                ),
                (
                    "position_id",
                    models.BigIntegerField(blank=True, null=True),
                ),
                (
                    "ticket",
                    models.BigIntegerField(blank=True, null=True),
                ),
                (
                    "error_message",
                    models.TextField(blank=True, default=""),
                ),
                (
                    "account_login",
                    models.CharField(blank=True, default="", max_length=32),
                ),
                (
                    "account_server",
                    models.CharField(blank=True, default="", max_length=128),
                ),
                (
                    "created_at",
                    models.DateTimeField(auto_now_add=True),
                ),
                (
                    "updated_at",
                    models.DateTimeField(auto_now=True),
                ),
                (
                    "operation",
                    models.ForeignKey(
                        on_delete=models.CASCADE,
                        related_name="mt5_audit_events",
                        to="operacoes.operation",
                    ),
                ),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
    ]
