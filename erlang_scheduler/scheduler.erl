-module(scheduler).
-export([iniciar/1, log_evento/1, parsear_nodos/1, prioridad_recurso/1]).
-export([armar_peticion/2, consultar_status/2]).
-export([bucle_gerente/4]).
-define(ESPERA_REINTENTO_MS, 1000).
-define(MAX_REINTENTOS_DENIED, 3).
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
                    [{IpA, _, _}, {IpB, _, _} | _] = NodosOrdenados,
                    spawn(simulador, forzar_deadlock, [self(), {IpA, IpB}]),

                    %% Entramos al bucle principal con el mapa ordenado, la lista de JobsActivos vacía y la lista de consultas de status pendientes, también vacía al principio.
                    bucle_gerente(Socket, NodosOrdenados, [], [])
            after 5000 ->
                %% Timeout por si C no nos responde los nodos rápido
                io:format("Scheduler: El agente C no mandó los nodos a tiempo.~n")
            end;
        {error, Razon} ->
            io:format("Scheduler: No me pude conectar a C: ~p~n", [Razon])
    end.

bucle_gerente(Socket, NodosOrdenados, JobsActivos, ConsultasPendientes) ->
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
            NuevosJobsActivos = [{PidSimulador, IdJob, RecursosPedidos, 1} | JobsActivos],
            bucle_gerente(Socket, NodosOrdenados, NuevosJobsActivos, ConsultasPendientes);
        %% Caso de un JOB_STATUS. Lo anotamos en consultas pendientes antes de mandar el comando, para poder distinguirlo de un JOB_REQUEST.
        {consultar_status, IdJob} ->
            Comando = io_lib:format("JOB_STATUS ~p", [IdJob]),
            log_evento(io_lib:format("CONSULTA STATUS JOB ~p", [IdJob])),
            cliente_tcp:enviar_comando(Socket, lists:flatten(Comando)),
            NuevasConsultas = [IdJob | ConsultasPendientes],
            bucle_gerente(Socket, NodosOrdenados, JobsActivos, NuevasConsultas);
        %% Llega una respuesta de C (de la forma "JOB_GRANTED 1001", "JOB_DENIED 1001" o "JOB_TIMEOUT 1001")
        {respuesta_c, MensajeC} ->
            case string:lexemes(MensajeC, " ") of
                ["[ERROR]" | _] -> 
                    io:format("Scheduler: Alerta, el agente C respondio un error -> ~s~n", [MensajeC]),
                    bucle_gerente(Socket, NodosOrdenados, JobsActivos, ConsultasPendientes);

                [Accion, IdStr | _Resto] ->
                    IdJob = list_to_integer(IdStr),
                    %% Chequeamos primero si el IdJob tiene una consulta de status pendiente. La mostramos/logueamos pero no disparamos el timer de liberación ni avisamos al simulador.
                    case lists:member(IdJob, ConsultasPendientes) of
                        true ->
                            io:format("Scheduler: STATUS del job ~p -> ~s~n", [IdJob, Accion]),
                            log_evento(io_lib:format("STATUS JOB ~p: ~s", [IdJob, Accion])),
                            ConsultasRestantes = lists:delete(IdJob, ConsultasPendientes),
                            bucle_gerente(Socket, NodosOrdenados, JobsActivos, ConsultasRestantes);
                        false ->
                            %% Buscamos el Pid del simulador dueño de este Job
                            case lists:keyfind(IdJob, 2, JobsActivos) of
                                {PidSimulador, IdJob, RecursosOriginales, Intentos} ->
                                    %% Sacamos el job de la lista activa primero;
                                    %% si corresponde reintentar, lo volvemos a
                                    %% agregar más abajo con el contador actualizado.
                                    JobRestantes = lists:keydelete(IdJob, 2, JobsActivos),

                                    case Accion of
                                        "JOB_GRANTED" ->
                                            %% Caso de siempre: avisamos, logueamos,
                                            %% programamos liberación automática.
                                            PidSimulador ! {resultado_job, Accion},
                                            log_evento(
                                                io_lib:format(
                                                    "RESOLUCION JOB ~p: ~s", [IdJob, Accion]
                                                )
                                            ),
                                            erlang:send_after(
                                                ?TIEMPO_USO_RECURSOS_MS,
                                                self(),
                                                {liberar_job, IdJob}
                                            ),
                                            bucle_gerente(
                                                Socket,
                                                NodosOrdenados,
                                                JobRestantes,
                                                ConsultasPendientes
                                            );
                                        _DeniedOTimeout when Intentos < ?MAX_REINTENTOS_DENIED ->
                                            %% Todavía nos quedan reintentos: NO le
                                            %% avisamos al simulador (para él, el job
                                            %% sigue en curso) y programamos un
                                            %% reintento del MISMO IdJob después de
                                            %% una espera, para no chocar de nuevo
                                            %% de inmediato contra el mismo conflicto.
                                            log_evento(
                                                io_lib:format(
                                                    "REINTENTO JOB ~p (intento ~p/~p) tras ~s",
                                                    [
                                                        IdJob,
                                                        Intentos + 1,
                                                        ?MAX_REINTENTOS_DENIED,
                                                        Accion
                                                    ]
                                                )
                                            ),
                                            erlang:send_after(
                                                ?ESPERA_REINTENTO_MS,
                                                self(),
                                                {reintentar_job, IdJob, PidSimulador,
                                                    RecursosOriginales, Intentos + 1}
                                            ),
                                            bucle_gerente(
                                                Socket,
                                                NodosOrdenados,
                                                JobRestantes,
                                                ConsultasPendientes
                                            );
                                        _DeniedOTimeoutAgotado ->
                                            %% Se acabaron los reintentos: ahí sí le
                                            %% avisamos al simulador que el job se
                                            %% descartó definitivamente.
                                            PidSimulador ! {resultado_job, Accion},
                                            log_evento(
                                                io_lib:format(
                                                    "JOB ~p DESCARTADO tras ~p intentos: ~s",
                                                    [IdJob, Intentos, Accion]
                                                )
                                            ),
                                            bucle_gerente(
                                                Socket,
                                                NodosOrdenados,
                                                JobRestantes,
                                                ConsultasPendientes
                                            )
                                    end;
                                false ->
                                    %% Si llega un Id que no tenemos anotado, lo ignoramos
                                    bucle_gerente(
                                        Socket, NodosOrdenados, JobsActivos, ConsultasPendientes
                                    )
                            end
                    end;
                _Otro ->
                    %% Si llega un mensaje con formato inesperado (menos de 2 palabras por ejemplo), lo logueamos y avisamos que no se reconoce.
                    log_evento(
                        io_lib:format("ADVERTENCIA: mensaje no reconocido de C: ~s", [MensajeC])
                    ),
                    bucle_gerente(Socket, NodosOrdenados, JobsActivos, ConsultasPendientes)
            end;
        %% Disparamos cuando corresponde reintentar un job que recibió DENIED/TIMEOUT y todavía tiene reintentos disponibles.
        {reintentar_job, IdJob, PidSimulador, RecursosOriginales, IntentoActual} ->
            TextoPeticion = armar_peticion(IdJob, RecursosOriginales),
            log_evento(
                io_lib:format(
                    "REINTENTANDO JOB ~p (intento ~p): ~s",
                    [IdJob, IntentoActual, TextoPeticion]
                )
            ),
            cliente_tcp:enviar_comando(Socket, TextoPeticion),
            NuevosJobsActivos = [
                {PidSimulador, IdJob, RecursosOriginales, IntentoActual} | JobsActivos
            ],
            bucle_gerente(Socket, NodosOrdenados, NuevosJobsActivos, ConsultasPendientes);
        %%Luego de que se cumpla el tiempo de uso de un job que había sido GRANTED, le mandamos JOB_RELEASE a C para que se liberen los recursos que usaba eso job.
        {liberar_job, IdJob} ->
            MensajeLiberacion = io_lib:format("JOB_RELEASE ~p", [IdJob]),
            log_evento(io_lib:format("LIBERACIÓN DEL JOB ~p: tiempo de uso cumplido", [IdJob])),
            cliente_tcp:enviar_comando(Socket, lists:flatten(MensajeLiberacion)),
            bucle_gerente(Socket, NodosOrdenados, JobsActivos, ConsultasPendientes);
        %% Desconexión
        conexion_cerrada ->
            log_evento("CRITICO: Se perdió la conexión con el agente C. Apagando planificador."),
            io:format("Scheduler apagado.~n")
    end.

%% Función para consultar el estado de un job.
consultar_status(PidScheduler, IdJob) ->
    PidScheduler ! {consultar_status, IdJob}.

%% Función para escribir en un archivo de texto todo lo que sucede, ya sea generar, aprobar o rechazar un job.
log_evento(Texto) ->
    Linea = io_lib:format("~s~n", [Texto]),
    file:write_file("scheduler.log", Linea, [append]).

%% Define la prioridad de cada recurso dentro de un mismo nodo.
%% Esto lo hacemos porque se acordó como curso tomar primero cpu, después memoria, y después gpu.
prioridad_recurso("cpu") -> 1;
prioridad_recurso("mem") -> 2;
prioridad_recurso("gpu") -> 3.

%% Función para construir la petición de la forma necesaria, o sea, de la forma [{IP, Recurso, Cantidad}, ...].
%% Ordenamos por dos niveles: primero por IP y luego por el orden de recurso establecido (cpu -> mem -> gpu). Esto garantiza un Orden Total en toda la red y previene deadlocks.
armar_peticion(IdJob, RecursosPedidos) ->
    PeticionesOrdenadas = lists:sort(
        fun({IpA, RecursoA, _}, {IpB, RecursoB, _}) ->
            case IpA =:= IpB of
                true -> prioridad_recurso(RecursoA) =< prioridad_recurso(RecursoB);
                false -> IpA =< IpB
            end
        end,
        RecursosPedidos
    ),

    ComandoBase = io_lib:format("JOB_REQUEST ~p", [IdJob]),

    Fragmentos = [
        io_lib:format(" @~s:~s:~p", [IP, Recurso, Cantidad])
     || {IP, Recurso, Cantidad} <- PeticionesOrdenadas
    ],

    lists:flatten([ComandoBase | Fragmentos]).
