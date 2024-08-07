from django.shortcuts import redirect
from django.views.generic import FormView
from django.contrib import messages
from django.core.files.storage import default_storage

from .util import get_message
from .forms import UploadForm
from .tasks import post_file

class UploadView(FormView):
    template_name = 'integration/upload.html'
    form_class = UploadForm
    success_url = "upload"

    def form_valid(self, form):
        
        fileupload = form.cleaned_data["file"]
        filepath = default_storage.save(fileupload.name, fileupload)

        post_file.delay(filepath)

        message = get_message()

        messages.success(self.request, message)

        return redirect(self.success_url)

