-module(simulador).
-export([iniciar/2, iniciar_random/2, forzar_deadlock/2]).

iniciar_random(PidScheduler, NodosDisponibles) ->
    iniciar_random(PidScheduler, NodosDisponibles, 0).

iniciar_random(PidScheduler, NodosDisponibles, Contador) ->
    %% Elegimos entre 1 y 2 nodos al azar para pedirles recursos.
    PedidoGenerado = generar_pedido_random(NodosDisponibles),
    case PedidoGenerado of
        [] ->
            timer:sleep(200),
            iniciar_random(PidScheduler, NodosDisponibles, Contador);
        _NoVacio ->
            io:format("Simulador: generando job N°~p -> ~p~n", [Contador, PedidoGenerado]),
            PidScheduler ! {pedido, self(), PedidoGenerado},

            %% Esperamos la resolución de ESTE job antes de generar el próximo. Pedimos de a uno por vez.
            receive
                {resultado_job, Accion} ->
                    io:format("Simulador: job N°~p resuelto -> ~s~n", [Contador, Accion])
            after 10000 ->
                %% Si el scheduler no contesta en 10s (por ejemplo, se cayó la conexión con C), no nos quedamos colgados para siempre.
                io:format("Simulador: job #~p sin respuesta, sigo de todas formas~n", [Contador])
            end,

            %% Esperamos un intervalo random antes de generar el siguiente job.
            timer:sleep(500 + rand:uniform(1500)),
            iniciar_random(PidScheduler, NodosDisponibles, Contador + 1)
    end.

%% Arma un pedido eligiendo 1 o 2 nodos al azar de NodosDisponibles, y para cada nodo, entre 1 y todos sus recursos disponibles.
generar_pedido_random(NodosDisponibles) ->
    CantidadNodos = rand:uniform(2),
    NodosElegidos = elegir_n_al_azar(CantidadNodos, NodosDisponibles),
    %% lists:append junta las listas de pedidos de cada nodo en una sola lista plana [{IP, Puerto, Recurso, Cantidad}, ...], que es lo que espera scheduler:armar_peticion.
    lists:append([pedidos_para_nodo(Nodo) || Nodo <- NodosElegidos]).

%% Para un nodo dado, elige entre 1 y todos sus recursos disponibles y arma un pedido por cada uno.
%% Ahora conservamos el Puerto: cada pedido es {IP, Puerto, Recurso, Cantidad}, porque la identidad real de un nodo es IP+Puerto (dos nodos pueden compartir IP).
pedidos_para_nodo({_IP, _Puerto, []}) ->
    [];
pedidos_para_nodo({IP, Puerto, Recursos}) ->
    Disponibles = [R || {_Nombre, Total} = R <- Recursos, Total > 0],
    case Disponibles of
        [] ->
            [];
        _ ->
            CantidadRecursos = rand:uniform(length(Disponibles)),
            RecursosElegidos = elegir_n_al_azar(CantidadRecursos, Disponibles),
            [
                pedido_para_recurso(IP, Puerto, Recurso, Total)
             || {Recurso, Total} <- RecursosElegidos
            ]
    end.

pedido_para_recurso(IP, Puerto, Recurso, Total) ->
    %% Pedimos entre 1 y el total disponible del recurso.
    Cantidad = rand:uniform(max(1, Total)),
    {IP, Puerto, Recurso, Cantidad}.

elegir_n_al_azar(N, Lista) when N >= length(Lista) ->
    Lista;
elegir_n_al_azar(N, Lista) ->
    Mezclada = [{rand:uniform(), X} || X <- Lista],
    [X || {_, X} <- lists:sublist(lists:sort(Mezclada), N)].

%% Función para forzar el deadlock del escenario propuesto en el Trabajo Práctico. Ahora recibe la identidad completa de cada nodo: {IP, Puerto}.
forzar_deadlock(PidScheduler, {{IpNodoA, PuertoA}, {IpNodoB, PuertoB}}) ->
    io:format(
        "Simulador: FORZANDO escenario de deadlock (Nodo A=~s:~s, Nodo B=~s:~s)~n",
        [IpNodoA, PuertoA, IpNodoB, PuertoB]
    ),

    %% Job1: necesita 2 CPUs de A y 1 GPU de B (tal como describe el TP).
    Pedido1 = [{IpNodoA, PuertoA, "cpu", 2}, {IpNodoB, PuertoB, "gpu", 1}],

    %% Job2: necesita 1 GPU de B y 2 CPUs de A
    Pedido2 = [{IpNodoB, PuertoB, "gpu", 1}, {IpNodoA, PuertoA, "cpu", 2}],

    %% Los lanzamos como dos procesos separados, "casi al mismo tiempo".
    PidJob1 = self(),
    spawn(fun() -> lanzar_un_job(PidScheduler, Pedido1, "Job1", PidJob1) end),
    spawn(fun() -> lanzar_un_job(PidScheduler, Pedido2, "Job2", PidJob1) end),

    %% Esperamos las dos resoluciones para mostrar el resultado final.
    esperar_resultados(2).

%% Orquesta el despacho asíncrono de un pedido individual hacia el planificador central.
lanzar_un_job(PidScheduler, Pedido, Etiqueta, PidPadre) ->
    PidScheduler ! {pedido, self(), Pedido},
    receive
        {resultado_job, Accion} ->
            io:format("Simulador: ~s resuelto -> ~s~n", [Etiqueta, Accion]),
            PidPadre ! {fin_forzado, Etiqueta, Accion}
    after 10000 ->
        io:format("Simulador: ~s sin respuesta (posible problema)~n", [Etiqueta]),
        PidPadre ! {fin_forzado, Etiqueta, sin_respuesta}
    end.

esperar_resultados(0) ->
    io:format("Simulador: escenario de deadlock forzado, ambos jobs resueltos sin colgarse.~n"),
    init:stop();
esperar_resultados(N) ->
    receive
        {fin_forzado, _Etiqueta, _Accion} ->
            esperar_resultados(N - 1)
    end.

iniciar(PidScheduler, NodosDisponibles) ->
    iniciar_random(PidScheduler, NodosDisponibles).
