# cotacoes/urls.py
from django.urls import path
from . import views
from .views import (
    QuotesHomeView, update_quotes, quotes_pivot,
    quotes_progress, update_quotes_ajax,
    update_live_quotes_view,
)

app_name = "cotacoes"

urlpatterns = [
    path("", QuotesHomeView.as_view(), name="home"),
    path("atualizar/", update_quotes, name="update"),
    path("atualizar-ao-vivo/", update_live_quotes_view, name="update_live"),
    path("ajax/atualizar/", update_quotes_ajax, name="update_ajax"),
    path("progresso/", quotes_progress, name="progress"),
    path("pivot/", quotes_pivot, name="pivot"),
]
