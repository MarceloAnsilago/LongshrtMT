from __future__ import annotations
from django.db import models


class QuoteDaily(models.Model):
    asset = models.ForeignKey("acoes.Asset", on_delete=models.CASCADE, related_name="quotes")
    date = models.DateField()
    open = models.FloatField(null=True, blank=True)
    high = models.FloatField(null=True, blank=True)
    low = models.FloatField(null=True, blank=True)
    close = models.FloatField()
    is_provisional = models.BooleanField(default=False)

    class Meta:
        unique_together = (("asset", "date"),)
        indexes = [
            models.Index(fields=["asset", "date"]),
        ]
        ordering = ["-date"]

    def __str__(self):
        return f"{self.asset.ticker} {self.date} = {self.close}"


# ---------------------------------------------------------------------
# 🟢 Novo modelo — cotação intradiária (tempo real via Yahoo)
# ---------------------------------------------------------------------
class QuoteLive(models.Model):
    asset = models.OneToOneField("acoes.Asset", on_delete=models.CASCADE, related_name="live_quote")
    price = models.FloatField()
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Cotação Atual"
        verbose_name_plural = "Cotações Atuais"

    def __str__(self):
        return f"{self.asset.ticker}: {self.price:.2f}"
