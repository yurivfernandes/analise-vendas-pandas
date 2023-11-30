from rest_framework import serializers

class TableSerializer(serializers.Serializer):
    names = serializers.ListField(child=serializers.CharField())