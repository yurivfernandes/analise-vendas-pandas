# Generated by Django 3.2 on 2023-08-03 21:11

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('etl', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='FatoVenda',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('data', models.DateTimeField(default=django.utils.timezone.now)),
                ('nfe', models.IntegerField()),
                ('equipe_vendas', models.TextField()),
                ('quantidade', models.IntegerField()),
                ('preco_unitario', models.DecimalField(decimal_places=2, default=0, max_digits=11)),
                ('load_log', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='etl.loadlog')),
                ('produto', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='etl.produto')),
                ('vendedor', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='etl.vendedor')),
            ],
            options={
                'verbose_name': 'Histórico de vendas',
                'verbose_name_plural': 'Histórico de vendas',
                'db_table': 'fato_venda',
                'unique_together': {('data', 'nfe', 'produto')},
            },
        ),
        migrations.CreateModel(
            name='FatoMeta',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('data', models.DateTimeField(default=django.utils.timezone.now)),
                ('faturamento', models.DecimalField(decimal_places=2, default=0, max_digits=11)),
                ('margem_bruta', models.DecimalField(decimal_places=2, default=0, max_digits=11)),
                ('notas_emitidas', models.IntegerField()),
                ('load_log', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='etl.loadlog')),
                ('vendedor', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='etl.vendedor')),
            ],
            options={
                'verbose_name': 'Cadastro de Metas',
                'verbose_name_plural': 'Cadastro de Metas',
                'db_table': 'fato_meta',
                'unique_together': {('data', 'vendedor')},
            },
        ),
    ]
