from django import forms

from .models import OrderRequest


class OrderRequestForm(forms.Form):
    terminal_id = forms.CharField(
        max_length=64,
        widget=forms.TextInput(attrs={"class": "form-control", "list": "terminal-list"}),
    )
    pair_id = forms.CharField(
        max_length=64,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    side = forms.ChoiceField(
        choices=OrderRequest.Side.choices,
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    symbol_a = forms.CharField(
        max_length=32,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    qty_a = forms.DecimalField(
        max_digits=18,
        decimal_places=6,
        widget=forms.NumberInput(attrs={"class": "form-control", "step": "0.000001"}),
    )
    symbol_b = forms.CharField(
        max_length=32,
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    qty_b = forms.DecimalField(
        max_digits=18,
        decimal_places=6,
        required=False,
        widget=forms.NumberInput(attrs={"class": "form-control", "step": "0.000001"}),
    )
    order_type = forms.ChoiceField(
        choices=OrderRequest.OrderType.choices,
        widget=forms.Select(attrs={"class": "form-select"}),
    )
