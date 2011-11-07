Arquitetura do serviço de conversão de vídeos

	A ideia atual do serviço de conversão é converter vídeos submetidos à biblioteca digital para o formato livre “.ogm”.
	Inicialmente, o vídeo é submetido por um webservice que recebe como parâmetro o vídeo, importa a funcionalidade de conversão de um módulo hospedado na biblioteca digital, armazena no SAM e retorna para o usuário a chave de identificação de armazenamento.

	Os Métodos utilizados no pacote são:

		get_current_user: Método responsável por autenticar e retornar uma tupla com o usuário e a senha.

		xmlrpc_convert: Método que recebe o vídeo, armazena-o no SAM, coloca o UID na fila de conversão e retorna ele para o cliente

        _enqueue_uid_to_convert: Método responsável por colocar o UID dos vídeos na fila de vídeos a converter

        _pre_store_in_sam: armazenar o vídeo não-convertido no SAM

Autor: Eduardo Braga Ferreira Junior

