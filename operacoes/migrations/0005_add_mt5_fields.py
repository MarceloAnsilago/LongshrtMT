from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("operacoes", "0004_operationmt5trade_close_reason_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="operation",
            name="symbol",
            field=models.CharField(blank=True, default="", max_length=32),
        ),
        migrations.AddField(
            model_name="operation",
            name="entry_price",
            field=models.DecimalField(
                blank=True,
                decimal_places=6,
                max_digits=12,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="operation",
            name="mt5_ticket",
            field=models.BigIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="operation",
            name="executed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
