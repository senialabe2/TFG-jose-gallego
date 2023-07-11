from django.shortcuts import render,redirect
from django.contrib.auth.decorators import login_required,user_passes_test
from django.conf import settings
from django.http import HttpResponse
from django.views.generic.edit import FormView
from .models import Aula
import requests

@login_required(login_url='/accounts/login/')
def inicio(request):
    return render(request, "inicio.html")

@login_required(login_url='/accounts/login/')
def fuenlabrada_aulario(request):
    my_context = {
        'img_aulario': '/static/media/edificio_gestion_1.png',
    }

    return render(request, "aulario1_fuenlabrada.html", context=my_context)

@login_required(login_url='/accounts/login/')
def fuenlabrada_aula101(request):
    def get_ultima_calificacion():
        ultima_aula = Aula.objects.order_by('-Timestamp').first()
        if ultima_aula:
            return ultima_aula.Estrellas
        else:
            return None
    my_context = {
        'capacidad': '60 personas',
        'estrellas': get_ultima_calificacion(),
        'IOT': 'http://localhost:3000/d/c6a0456b-fab6-4388-8849-df2ff2919170/aula101?orgId=1',
    }

    return render(request, "fuenlabrada_aula101.html", context=my_context)


