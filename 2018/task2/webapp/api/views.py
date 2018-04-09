import json
from json import JSONDecodeError

import pika
from django.conf import settings
from django.db import transaction
from django.http import HttpResponseBadRequest
from django.http import JsonResponse
from django.shortcuts import render, get_object_or_404

# Create your views here.
from django.views import View
from django.views.decorators.csrf import csrf_exempt

from api.lib import UUIDEncoder
from api.models import Md5Task


def run_md5_task(task_id, url):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings.RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue='md5tasks')
    data = {'task_id': task_id, 'url': url}
    body = json.dumps(data, cls=UUIDEncoder)
    channel.basic_publish(exchange='', routing_key='md5tasks', body=body)
    connection.close()


class Md5View(View):
    @staticmethod
    def get(request):
        task_id = request.GET.get('task_id')
        if not task_id:
            return HttpResponseBadRequest()

        task = get_object_or_404(Md5Task, guid=task_id)

        status = 'not ready'

        if task.has_error is True:
            status = 'has error'
        elif task.result:
            status = 'ready'

        return JsonResponse({'result': task.result, 'status': status})

    @staticmethod
    def post(request):
        try:
            data = json.loads(request.body)
        except JSONDecodeError:
            return HttpResponseBadRequest()

        url = data.get('url')
        if not url:
            return HttpResponseBadRequest()

        with transaction.atomic():
            task = Md5Task.objects.create()
            task_id = task.guid
            run_md5_task(task_id, url)
            return JsonResponse({'id': task_id})
