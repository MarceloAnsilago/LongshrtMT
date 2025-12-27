from django.contrib import admin
from django.urls import path, include
from django.http import JsonResponse

from core import views as core_views


def healthcheck(request):
    return JsonResponse({"status": "ok"})


urlpatterns = [
    path("admin/", admin.site.urls),

    path("acoes/", include("acoes.urls")),
    path("cotacoes/", include("cotacoes.urls")),

    path("pares/", include(("pairs.urls", "pairs"), namespace="pairs")),

    path("accounts/", include("accounts.urls")),

    path("health/", healthcheck, name="healthcheck"),

    path("", include("core.urls")),
    path("teste-mt5/", core_views.teste_mt5, name="teste_mt5"),
]
