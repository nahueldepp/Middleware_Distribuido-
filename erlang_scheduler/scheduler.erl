-module(scheduler).
-export([parsear_nodos/1, iniciar/1, bucle_gerente/3, log_evento/1, armar_peticion/2]).

%% Parsea la cadena cruda que manda el agente C en respuesta a GET_NODES.
%% Formato esperado: "IP:Puerto:cpu:N:mem:N:gpu:N;IP:Puerto:cpu:N:...;..."
%% Devuelve una lista de tuplas {IP, Puerto, Recursos}, donde Recursos es
%% una lista de pares {NombreRecurso, Cantidad} con Cantidad ya como entero.
parsear_nodos(Cadena) ->
    %% Separamos los nodos por ";"
    NodosCrudos = string:lexemes(Cadena, ";"),

    %% Parseamos cada nodo individualmente a una tupla estructurada
    [parsear_nodo(string:lexemes(Nodo, ":")) || Nodo <- NodosCrudos].

%% Convierte una lista plana ["IP","Puerto","cpu","4","mem","8192","gpu","1"]
%% en {"IP", "Puerto", [{"cpu",4},{"mem",8192},{"gpu",1}]}
parsear_nodo([IP, Puerto | Recursos]) ->
    {IP, Puerto, agrupar_pares(Recursos)}.

%% Agrupa una lista plana de strings ["cpu","4","mem","8192",...] en pares
%% {Recurso, Cantidad} con la cantidad ya convertida a entero.
agrupar_pares([]) ->
    [];
agrupar_pares([Recurso, CantidadStr | Resto]) ->
    [{Recurso, list_to_integer(CantidadStr)} | agrupar_pares(Resto)].

%% Función principal para arrancar el Scheduler
iniciar(PuertoC) ->
    io:format("Scheduler: Arrancando y buscando al agente C...~n"),

    %% 1. Levantamos al Cartero
    case cliente_tcp:iniciar("localhost", PuertoC, self()) of
        {ok, _PidCliente, Socket} ->
            %% 2. Le pedimos el mapa de la red a C
            cliente_tcp:enviar_comando(Socket, "GET NODES"),

            %% 3. Esperamos exclusivamente esa primera respuesta
            receive
                {respuesta_c, CadenaNodos} ->
                    %% 4. Parseamos la cadena a tuplas {IP, Puerto, Recursos}
                    NodosParseados = parsear_nodos(CadenaNodos),

                    %% Ordena por IP (las tuplas se comparan elemento a
                    %% elemento, así que esto ordena por el primer campo)
                    NodosOrdenados = lists:sort(NodosParseados),

                    io:format("Scheduler: Nodos ordenados listos: ~p~n", [NodosOrdenados]),

                    %% 6. Entramos al bucle infinito pasándole el mapa ya
                    %% ordenado y una lista vacía [] que representará los
                    %% JobsActivos.
                    bucle_gerente(Socket, NodosOrdenados, [])
            after 5000 ->
                %% Un timeout por si C no nos responde los nodos rápido
                io:format("Scheduler: El agente C no mandó los nodos a tiempo.~n")
            end;
        {error, Razon} ->
            io:format("Scheduler: No me pude conectar a C: ~p~n", [Razon])
    end.

bucle_gerente(Socket, NodosOrdenados, JobsActivos) ->
    receive
        %% Llega un pedido del simulador.
        %% RecursosPedidos AHORA debe ser [{IP, Recurso, Cantidad}, ...], es decir, el simulador ya decidió a qué nodo le pide cada cosa (usando NodosOrdenados para saber qué hay disponible en la red).
        {pedido, PidSimulador, RecursosPedidos} ->
            IdJob = erlang:unique_integer([positive]),

            %% Armamos el string final, ya ordenado por IP para garantizar que todos los jobs pidan en el mismo orden global (estrategia anti-deadlock por prevención, ver §6 del TP).
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

                            %% Sacamos el job de la lista activa porque ya se resolvió
                            JobRestantes = lists:keydelete(IdJob, 2, JobsActivos),
                            bucle_gerente(Socket, NodosOrdenados, JobRestantes);
                        false ->
                            %% Si llega un Id que no tenemos anotado, lo ignoramos
                            bucle_gerente(Socket, NodosOrdenados, JobsActivos)
                    end;
                _Otro ->
                    %% Mensaje con formato inesperado (menos de 2 palabras). Lo logueamos en vez de romper el bucle con un badmatch.
                    log_evento(
                        io_lib:format("ADVERTENCIA: mensaje no reconocido de C: ~s", [MensajeC])
                    ),
                    bucle_gerente(Socket, NodosOrdenados, JobsActivos)
            end;
        %% Desconexión
        conexion_cerrada ->
            log_evento("CRITICO: Se perdió la conexión con el agente C. Apagando planificador."),
            io:format("Scheduler apagado.~n")
    end.

%% Función para escribir en un archivo de texto todo lo que sucede, ya sea generar, aprobar o rechazar un job.
log_evento(Texto) ->
    Linea = io_lib:format("~s~n", [Texto]),
    file:write_file("scheduler.log", Linea, [append]).

%% Función para construir la petición de la forma que necesitamos.
%% RecursosPedidos: [{IP, Recurso, Cantidad}, ...] - lo que pide ESTE job
%% específico, potencialmente a varios nodos distintos.
%%
%% Ordenamos por IP antes de armar el texto: esto garantiza que CUALQUIER
%% job, sin importar qué recursos necesite, siempre pida primero al nodo
%% de IP más baja y después al de IP más alta. Como todos los nodos de la
%% red respetan el mismo criterio de orden, la espera circular del
%% escenario de deadlock (§6 del TP) se vuelve imposible: ningún job puede
%% estar esperando un recurso "anterior" en el orden mientras otro job
%% espera uno "posterior" sobre él, porque la relación de orden es total.
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
