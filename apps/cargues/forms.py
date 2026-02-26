from django import forms


class CargueInfoVentasForm(forms.Form):
    excel_file = forms.FileField(
        label="Archivo Excel (.xlsx)",
        required=True,
        widget=forms.ClearableFileInput(attrs={"accept": ".xlsx"}),
    )

    def clean_excel_file(self):
        file = self.cleaned_data["excel_file"]
        if not file.name.endswith(".xlsx"):
            raise forms.ValidationError("El archivo debe tener extensión .xlsx")
        if file.size > 900 * 1024 * 1024:  # 900MB
            raise forms.ValidationError("El archivo es demasiado grande (máx 900MB)")
        return file
