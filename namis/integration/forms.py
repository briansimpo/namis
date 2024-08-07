from django import forms
from django.core.validators import FileExtensionValidator


class UploadForm(forms.Form):
    file = forms.FileField(
        widget=forms.FileInput(attrs={'accept': 'application/csv'}),
        required=True,
        label="CSV File",
        validators=[FileExtensionValidator(allowed_extensions=['csv'])],
    )
    