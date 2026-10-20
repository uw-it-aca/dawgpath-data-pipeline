# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from django import forms

from dawgpath_data_pipeline.utilities.reference_data import (
    MAX_UPLOAD_BYTES,
    ReferenceValidationError,
    validate_reference,
)


class ReferenceUploadForm(forms.Form):
    kind = forms.ChoiceField(choices=[
        ("coi", "COI scores"), ("career", "Career-center mappings")])
    file = forms.FileField(label="UTF-8 CSV")

    def clean(self):
        cleaned = super().clean()
        uploaded = cleaned.get("file")
        kind = cleaned.get("kind")
        if uploaded is None or kind is None:
            return cleaned
        if uploaded.size > MAX_UPLOAD_BYTES:
            self.add_error("file", "Maximum file size is 10 MiB.")
            return cleaned
        content = uploaded.read(MAX_UPLOAD_BYTES + 1)
        try:
            canonical, _ = validate_reference(kind, content)
        except ReferenceValidationError as exc:
            self.add_error("file", str(exc))
        else:
            cleaned["content"] = canonical
        return cleaned
