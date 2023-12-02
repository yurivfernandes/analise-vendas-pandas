
from rest_framework.response import Response
from rest_framework.views import APIView
from ...tasks import LoadCalendar
from ...utils import fetch_data_compound_request

class LoadCalendarView(APIView):
    """View que aciona a task de construção da base de [Consolidação Receita]"""
    def post(self, request, *args, **kwargs) -> Response:
        params = fetch_data_compound_request(request)
        
        # load_frota_atual_distribuicao_async.delay(versao_id=versao.id, **params)
        # return Response(
        #     {"message": "A requisição foi recebida e a carga foi iniciada!"}, status=status.HTTP_202_ACCEPTED
        # )
        with LoadCalendar(**params) as load:
            log = load.run()
        return Response(log)