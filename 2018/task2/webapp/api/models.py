import uuid

from django.db import models


# Create your models here.


class Md5Task(models.Model):
    guid = models.UUIDField(primary_key=True, default=uuid.uuid4)
    result = models.TextField(null=True)
    has_error = models.BooleanField(default=False)
