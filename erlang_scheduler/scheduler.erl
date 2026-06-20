-module(scheduler).
-export([parsear_nodos/1, iniciar/1, bucle_gerente/3, log_evento/1, armar_peticion/2]).
-define(TIEMPO_USO_RECURSOS_MS, 5000).

%% Función usada para parsear la cadena cruda que envía C.
%% El formato esperado es el presentado en el enunciado del Trabajo Práctico, "IP:Puerto:cpu:(cantidad):gpu:(cantidad);IP:Puerto:..."
%% La función devuelve una lista de tuplas de la forma {IP, Puerto, Recursos}, donde Recursos será una lista de pares de la forma {NombreRecurso, Cantidad}.
parsear_nodos(Cadena) ->
    %% Separamos los nodos por ";"
    NodosCrudos = string:lexemes(Cadena, ";"),

    %% Parseamos cada nodo individualmente a una tupla, separando al encontrar :
    [parsear_nodo(string:lexemes(Nodo, ":")) || Nodo <- NodosCrudos].

%% Convierte una lista plana ["IP","Puerto","cpu","4","mem","8192","gpu","1"] en {"IP", "Puerto", [{"cpu",4},{"mem",8192},{"gpu",1}]}
parsear_nodo([IP, Puerto | Recursos]) ->
    {IP, Puerto, agrupar_pares(Recursos)}.

%% Agrupa una lista plana de strings ["cpu","4","mem","8192",...] en pares {Recurso, Cantidad} con la cantidad ya convertida a entero.
agrupar_pares([]) ->
    [];
agrupar_pares([Recurso, CantidadStr | Resto]) ->
    [{Recurso, list_to_integer(CantidadStr)} | agrupar_pares(Resto)].

%% Función principal para arrancar el Scheduler
iniciar(PuertoC) ->
    io:format("Scheduler: Arrancando y buscando al agente C...~n"),

    %% Iniciamos el cliente TCP.
    case cliente_tcp:iniciar("localhost", PuertoC, self()) of
        {ok, _PidCliente, Socket} ->
            %% Pedimos los nodos a C
            cliente_tcp:enviar_comando(Socket, "GET NODES"),

            %% Esperamos la respuesta inicial
            receive
                {respuesta_c, CadenaNodos} ->
                    %% Parseamos la cadena a tuplas {IP, Puerto, Recursos}
                    NodosParseados = parsear_nodos(CadenaNodos),

                    %% Ordenamos por IP (las tuplas se comparan elemento a elemento, así que esto ordena por el primer campo)
                    NodosOrdenados = lists:sort(NodosParseados),

                    io:format("Scheduler: Nodos ordenados listos: ~p~n", [NodosOrdenados]),

                    %% Iniciamos al simulador, que se encargará de representar pedidos de Jobs.
                    %% No uso spawn_link porque no quiero que un error en el simulador tumbe al scheduler.
                    spawn(simulador, iniciar, [self(), NodosOrdenados]),

                    %% Entramos al bucle principal con el mapa ordenado y la lista de JobsActivos vacía.
                    bucle_gerente(Socket, NodosOrdenados, [])
            after 5000 ->
                %% Timeout por si C no nos responde los nodos rápido
                io:format("Scheduler: El agente C no mandó los nodos a tiempo.~n")
            end;
        {error, Razon} ->
            io:format("Scheduler: No me pude conectar a C: ~p~n", [Razon])
    end.

bucle_gerente(Socket, NodosOrdenados, JobsActivos) ->
    receive
        %% Llega un pedido del simulador.
        %% RecursosPedidos debe ser [{IP, Recurso, Cantidad}, ...], es decir, el simulador ya decidió a qué nodo le pide cada cosa (usando NodosOrdenados para saber qué hay disponible en la red).
        {pedido, PidSimulador, RecursosPedidos} ->
            IdJob = erlang:unique_integer([positive]),

            %% Armamos el string final, ya ordenado por IP para garantizar que todos los jobs pidan en el mismo orden global.
            %% Esa será nuestra estrategia anti deadlock.
            TextoPeticion = armar_peticion(IdJob, RecursosPedidos),

            %% Logueamos y mandamos a C
            log_evento(
                io_lib:format(
                    "NUEVO JOB ~p: Solicitado por Pid ~p - ~s",
                    [IdJob, PidSimulador, TextoPeticion]
                )
            ),
            cliente_tcp:enviar_comando(Socket, TextoPeticion),

            %% Actualizamos la lista de trabajos activos.
            NuevosJobsActivos = [{PidSimulador, IdJob} | JobsActivos],
            bucle_gerente(Socket, NodosOrdenados, NuevosJobsActivos);
        %% Llega una respuesta de C (de la forma "JOB_GRANTED 1001", "JOB_DENIED 1001" o "JOB_TIMEOUT 1001")
        {respuesta_c, MensajeC} ->
            case string:lexemes(MensajeC, " ") of
                [Accion, IdStr | _Resto] ->
                    IdJob = list_to_integer(IdStr),

                    %% Buscamos el Pid del simulador dueño de este Job
                    case lists:keyfind(IdJob, 2, JobsActivos) of
                        {PidSimulador, IdJob} ->
                            %% Le avisamos al simulador cómo le fue.
                            PidSimulador ! {resultado_job, Accion},

                            %% Logueamos el resultado
                            log_evento(io_lib:format("RESOLUCION JOB ~p: ~s", [IdJob, Accion])),
                            %% Si el job fue concedido, programamos su liberación automática después de un tiempo fijo.
                            case Accion of
                                "JOB_GRANTED" ->
                                    erlang:send_after(
                                        ?TIEMPO_USO_RECURSOS_MS, self(), {liberar_job, IdJob}
                                    );
                                _Otra ->
                                    ok
                            end,

                            %% Sacamos el job de la lista activa porque ya se resolvió
                            JobRestantes = lists:keydelete(IdJob, 2, JobsActivos),
                            bucle_gerente(Socket, NodosOrdenados, JobRestantes);
                        false ->
                            %% Si llega un Id que no tenemos anotado, lo ignoramos
                            bucle_gerente(Socket, NodosOrdenados, JobsActivos)
                    end;
                _Otro ->
                    %% Si llega un mensaje con formato inesperado (menos de 2 palabras por ejemplo), lo logueamos y avisamos que no se reconoce.
                    log_evento(
                        io_lib:format("ADVERTENCIA: mensaje no reconocido de C: ~s", [MensajeC])
                    ),
                    bucle_gerente(Socket, NodosOrdenados, JobsActivos)
            end;
        %%Luego de que se cumpla el tiempo de uso de un job que había sido GRANTED, le mandamos JOB_RELEASE a C para que se liberen los recursos que usaba eso job.
        {liberar_job, IdJob} ->
            MensajeLiberacion = io_lib:format("JOB_RELEASE ~p", [IdJob]),
            log_evento(io_lib:format("LIBERACIÓN DEL JOB ~p: tiempo de uso cumplido", [IdJob])),
            cliente_tcp:enviar_comando(Socket, lists:flatten(MensajeLiberacion)),
            bucle_gerente(Socket, NodosOrdenados, JobsActivos);
        %% Desconexión
        conexion_cerrada ->
            log_evento("CRITICO: Se perdió la conexión con el agente C. Apagando planificador."),
            io:format("Scheduler apagado.~n")
    end.

%% Función para escribir en un archivo de texto todo lo que sucede, ya sea generar, aprobar o rechazar un job.
log_evento(Texto) ->
    Linea = io_lib:format("~s~n", [Texto]),
    file:write_file("scheduler.log", Linea, [append]).

%% Función para construir la petición de la forma necesaria, o sea, de la forma [{IP, Recurso, Cantidad}, ...].
%% Ordenamos por IP, lo que garantiza que cualquier job siempre pida primero al nodo de IP más baja y después al de IP más alta, eliminando la espera circular y previniendo un posible caso de deadlock.
armar_peticion(IdJob, RecursosPedidos) ->
    PeticionesOrdenadas = lists:sort(
        fun({IpA, _, _}, {IpB, _, _}) -> IpA =< IpB end,
        RecursosPedidos
    ),

    ComandoBase = io_lib:format("JOB_REQUEST ~p", [IdJob]),

    Fragmentos = [
        io_lib:format(" @~s:~s:~p", [IP, Recurso, Cantidad])
     || {IP, Recurso, Cantidad} <- PeticionesOrdenadas
    ],

    lists:flatten([ComandoBase | Fragmentos]).
