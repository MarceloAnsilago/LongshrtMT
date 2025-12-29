from __future__ import annotations

from django import forms

from operacoes.models import Operation


class OperationForm(forms.ModelForm):
    """Formulário simplificado para expor o campo `is_real`."""

    is_real = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={"class": "form-check-input", "id": "realSwitch"}),
    )

    class Meta:
        model = Operation
        fields = ["is_real"]
