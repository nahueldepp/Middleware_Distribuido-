-module(cliente_tcp).
-export([bucle/2, enviar_comando/2]).
-export([iniciar/3]).

%% Inicia la conexión con el agente C local.
iniciar(Host, Puerto, SchedulerPid) ->
    %% {packet, line}: Erlang ensambla los datos hasta encontrar \n y entrega exactamente una línea por evento.
    Opciones = [binary, {packet, line}, {active, true}],

    case gen_tcp:connect(Host, Puerto, Opciones) of
        {ok, Socket} ->
            io:format("TCP Client: Conectado al agente C en ~p:~p~n", [Host, Puerto]),
            %% Con la línea siguiente nos aseguramos de que si el scheduler muere, este proceso muere también (y viceversa), evitando procesos huérfanos.
            Pid = spawn_link(fun() -> bucle(Socket, SchedulerPid) end),
            %% Al abrir un socket con gen_tcp:connect, el proceso que ejecutó esa línea se hace dueño del socket. Sin la línea siguiente, todo mensaje asincrónico iban a ir al buzón del Planificador y no al del bucle.
            ok = gen_tcp:controlling_process(Socket, Pid),
            {ok, Pid, Socket};
        {error, Razon} ->
            io:format("TCP Client: Falló la conexión: ~p~n", [Razon]),
            {error, Razon}
    end.

%% Función para enviar un comando al agente de C.
enviar_comando(Socket, Comando) ->
    %% El protocolo exige que las líneas ASCII terminen en \n.
    Mensaje = list_to_binary([Comando, "\n"]),
    gen_tcp:send(Socket, Mensaje).

%% Bucle infinito que procesa lo que llega del socket.
bucle(Socket, SchedulerPid) ->
    receive
        {tcp, Socket, Data} ->
            %% Con {packet, line}, Data contiene exactamente una línea (incluyendo el \n). Debemos eliminar ese \n, y lo logramos usando string:trim.
            Cadena = binary_to_list(Data),
            MensajeLimpio = string:trim(Cadena),
            SchedulerPid ! {respuesta_c, MensajeLimpio},
            bucle(Socket, SchedulerPid);
        {tcp_closed, Socket} ->
            io:format("TCP Client: El agente C cerró la conexión.~n"),
            gen_tcp:close(Socket),
            SchedulerPid ! conexion_cerrada;
        {tcp_error, Socket, Razon} ->
            io:format("TCP Client: Error en el socket: ~p~n", [Razon]),
            gen_tcp:close(Socket),
            SchedulerPid ! {tcp_error, Razon}
    end.
