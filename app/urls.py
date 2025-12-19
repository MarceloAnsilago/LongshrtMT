from django.shortcuts import redirect
from django.urls import path

from monitor import views as monitor_views

urlpatterns = [
    path("api/monitor/state/", monitor_views.monitor_state, name="monitor-state"),
    path("api/monitor/history/", monitor_views.monitor_history_api, name="monitor-history"),
    path("monitor/", monitor_views.dashboard, name="monitor-dashboard"),
    path("monitor/pair/<path:pair>/", monitor_views.pair_view, name="monitor-pair"),
    path("", lambda request: redirect("monitor-dashboard", permanent=False)),
]
