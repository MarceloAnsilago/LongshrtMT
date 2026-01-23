from django.urls import path

from . import views


app_name = "bridge"


urlpatterns = [
    path("api/terminal/<str:terminal_id>/status", views.terminal_status, name="terminal_status"),
    path("api/terminal/<str:terminal_id>/heartbeat", views.terminal_heartbeat, name="terminal_heartbeat"),
    path("api/orders/next", views.orders_next, name="orders_next"),
    path("api/orders/test", views.orders_test, name="orders_test"),
    path("api/orders/<uuid:order_id>/ack", views.order_ack, name="order_ack"),
    path("api/orders/<uuid:order_id>/fill", views.order_fill, name="order_fill"),
    path("api/orders/<uuid:order_id>/reject", views.order_reject, name="order_reject"),
]
