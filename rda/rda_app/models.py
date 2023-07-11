from django.db import models

class Aula(models.Model):
    index = models.AutoField(primary_key=True)
    id_sensor = models.TextField()
    Timestamp = models.DateTimeField()
    Temperatura = models.FloatField()
    Humedad = models.FloatField()
    CO2 = models.FloatField()
    Ocupacion = models.FloatField()
    Icl = models.FloatField()
    IDA = models.TextField()
    temperatura_radiante_itrc = models.FloatField(db_column='Temperatura Radiante ITRC')
    pmv = models.FloatField()
    ppd = models.FloatField()
    Categoria = models.TextField()
    Estrellas = models.BigIntegerField()

    class Meta:
        db_table = 'Data_sensores'