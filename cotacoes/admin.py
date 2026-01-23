
# Register your models here.
from django.contrib import admin
from .models import QuoteDaily

@admin.register(QuoteDaily)
class QuoteDailyAdmin(admin.ModelAdmin):
    list_display = ("asset", "date", "close")
    list_filter = ("asset",)
    search_fields = ("asset__ticker",)
    ordering = ("-date",)

