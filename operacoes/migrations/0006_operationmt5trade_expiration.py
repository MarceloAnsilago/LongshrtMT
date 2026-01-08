from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("operacoes", "0005_add_mt5_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="operationmt5trade",
            name="expiration_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
