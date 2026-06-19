-module(tcp_client).
-export([iniciar/3, enviar_comando/2, bucle/2]).

%% Inicia la conexión con el agente C local.
%% Host: típicamente "localhost" o "127.0.0.1".
%% Puerto: el puerto donde escucha el Agente C local.
%% SchedulerPid: el ID del proceso del scheduler para mandarle los mensajes.
iniciar(Host, Puerto, SchedulerPid) ->
    %% {packet, line}: Erlang ensambla los datos hasta encontrar \n y nos
    %% entrega exactamente una línea por evento. Resuelve la fragmentación TCP.
    Opciones = [binary, {packet, line}, {active, true}],

    case gen_tcp:connect(Host, Puerto, Opciones) of
        {ok, Socket} ->
            io:format("TCP Client: Conectado al agente C en ~p:~p~n", [Host, Puerto]),
            %% spawn_link/1: si el scheduler muere, este proceso muere también
            %% (y viceversa), evitando procesos huérfanos.
            Pid = spawn_link(fun() -> bucle(Socket, SchedulerPid) end),
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
            %% Con {packet, line}, Data contiene exactamente una línea
            %% (incluyendo el \n). string:trim/1 lo limpia.
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
