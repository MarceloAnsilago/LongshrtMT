from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("cotacoes", "0001_initial"),
    ]

    operations = [
        migrations.DeleteModel(
            name="MissingQuoteLog",
        ),
    ]
