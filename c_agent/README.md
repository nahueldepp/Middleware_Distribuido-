 #C_Agent



#Server_C

1. Crear socket de escucha.
2. Hacer bind().
3. Hacer listen().
4. Poner el socket en modo no bloqueante.
5. Crear epoll.
6. Agregar los sockets de escucha a epoll.
7. Aceptar clientes.
8. Leer datos de clientes.
9. Detectar cierre.
10. Responder algo.

copilacion: 
gcc -Wall -Wextra -g server.c -o agent

uso: 

./agent 8100 9100

en otra terminal: 

nc localhost 8100

nc localhost 9100