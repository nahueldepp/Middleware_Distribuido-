#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "server.h"

#define MAX_EVENTS 64
#define BUFFER_SIZE 1024

/*Toma un file descriptor (socket) y lo setea a modo no bloqueante*/
static int set_nobloqueante(int fd){
    /*F_GETFL (void)
    Return (as the function result) the file access mode and the file status flags; arg is ignored.*/
    int flags = fcntl(fd, F_GETFL, 0);

    if(flags == -1){
        perror("fcntl F_GETFL");
        return -1;
    }

    /* F_SETFL (int)
    Set  the  file  status flags to the value specified by arg.*/
    if(fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1){
        perror("fcntl F_SETFL");
        return -1;
    }

    return 0; 
}


static int crear_socket_escucha(const char* ip, int port){
    /*Creo socket TCP*/
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if(fd == -1){
        perror("socket");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    /*
    Manipulate options for the socket referred to by the file descriptor sockfd.
    To manipulate options  at  the  sockets  API  level,
    level  is specified as SOL_SOCKET.
    SO_REUSEADDR: Permite reutilizar una dirección 
    y puerto locales inmediatamente después de cerrar un servidor,
    evitando el error "Address already in use" (Dirección ya en uso)
    Basicamente seteamos el socket para que cuando se cierre un servidor reusar la direccion y su puerto
    */
    if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1){
        perror("setsocket");
        exit(EXIT_FAILURE);
    }

    /*Seteamos para que sea no bloqueante*/
    if(set_nobloqueante(fd) == -1){
        perror("set_nobloqueante");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in addr; 
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    /*This  function converts the character string src into a network address structure in the af address family, 
    then copies the network address structure to dst.  The af argu‐
    ment must be either AF_INET or AF_INET6.  dst is written in network byte order.
    Convierte ip(el string del puerto) en una estructura de direccion de red en la familia AF_INET
    */

    if(inet_pton(AF_INET, ip, &addr.sin_addr) <= 0){
        perror("inet_pton");
        close(fd);
        exit(EXIT_FAILURE);
    }

    /*Asignamos un nobre al socket 
    When a socket is created with socket(2), it exists in a name space (address family)
    but has no address assigned to it.  bind() assigns the address specified by addr to the
    socket  referred  to  by  the file descriptor sockfd.  addrlen specifies the size, in bytes,
    of the address structure pointed to by addr.  Traditionally, this operation is
    called “assigning a name to a socket”.*/

    if(bind(fd, (struct sockaddr*) &addr, sizeof(addr) ) == -1){
        perror("bind");
        close(fd);
        exit(EXIT_FAILURE);
    }


    /*Lo ponemos a escuchar
    SOMAXCONN indica al sistema operativo que permita 
    la longitud máxima posible en la cola de conexiones pendientes (backlog), 
    optimizando el rendimiento bajo alta concurrencia.*/
    if(listen(fd, SOMAXCONN) == -1){
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf("[INFO] >> Escuchando en %s:%d\n", ip, port);

    return fd;
}

/*Crea un agrega a un epoll_fd el fd de un cierto tipo (de escuha publica o local)*/
static void agregar_fd_a_epoll(int epoll_fd, int fd, FdTipo tipo){
    FdInfo* info = malloc(sizeof(FdInfo));

    if(info == NULL){
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    info->fd = fd;
    info->type = tipo; 

    struct epoll_event ev; 
    memset(&ev, 0, sizeof(ev));

    
    ev.events = EPOLLIN; //espero eventos EPOLLIN
    ev.data.ptr = info; //información que quiereo conocer de un socket en el momento que haya un EPOLLIN

    /*agrego el fd  a epoll_fd*/
    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1){
        perror("epoll_ctl ADD");
        free(info);
        close(fd);
        exit(EXIT_FAILURE);
    }

}

static void cerrar_conexion(int epoll_fd, FdInfo* info){
    printf("[INFO]>> Cerrando conexion fd= %d...\n", info->fd);

    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, info->fd, NULL);
    close(info->fd);
    free(info);
}

static void aceptar_clientes(int epollFd, int escuhaFd){

    while(1){
        struct sockaddr_in cliente_addr;
        socklen_t cliente_len = sizeof(cliente_addr);

        //acepto el cliente
        int cliente_fd = accept(escuhaFd, (struct sockaddr *)&cliente_addr, &cliente_len);

        if(cliente_fd == -1){
            /*
             On error, -1 is returned, errno is set to indicate the error, and
            addrlen is left unchanged.  
            EAGAIN or EWOULDBLOCK
              The  socket is marked nonblocking and no connections are present to be accepted.*/
            if(errno == EAGAIN || errno ==  EWOULDBLOCK){
                break;
            }

            perror("accept");
            break;

        }

        if(set_nobloqueante(cliente_fd) == -1){
            perror("set_nobloqueante CLIENTE");
            close(cliente_fd);
            continue;
        }

        /* This function converts the network address structure src in the af address family into a character string. 
         The resulting string is copied to the buffer pointed to by dst*/
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &cliente_addr.sin_addr,ip, sizeof(ip));

        printf("[INFO]>> Conección aceptada fd=%d de %s:%d\n",
        cliente_fd,
        ip,
        ntohs(cliente_addr.sin_port));

        agregar_fd_a_epoll(epollFd, cliente_fd, FD_CLIENTE);
    }
}

static void manejar_lectura_cliente(int epoll_fd, FdInfo* info){

    char buffer[BUFFER_SIZE];
    
    while(1){
        ssize_t n = read(info->fd, buffer, sizeof(buffer));

        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
           perror("read");
            cerrar_conexion(epoll_fd, info);
            return; 
        }
        
        if(n == 0){
            printf("[INFO]>> Cliente desconectado fd=%d\n", info->fd);
            cerrar_conexion(epoll_fd, info);
            return;
        }

        buffer[n] = '\0';

        printf("[RECV fd=%d] %s\n", info->fd, buffer);

        manejar_linea(info->fd, buffer);
        
    }
}

static void manejar_linea(int fd, const char* linea){

    char cmd[32];
    int job_id;
    char resource[32];
    int cantidad;

    printf("[LINE fd =%d] %s\n", fd, linea);
    /*sscanf usa el formato %31s para leer el comando y el recurso pedido*/
    if(sscanf(linea, "%31s %d %31s %d",cmd, &job_id, resource, &cantidad) == 4){
        if(strncmp(cmd,"RESERVE", 7) == 0){
            printf("[INFO]>> RESERVE job_id:<%d> resourse:<%s> amount:<%d>\n",
            job_id, resource, cantidad);

            dprintf(fd, "GRANTED %d\n", job_id);
            return;
        }

        if(strncmp(cmd,"RELEASE", 7) == 0){
            printf("[INFO]>> RELEASE job_id:<%d> resourse:<%s> amount:<%d>\n",
            job_id, resource, cantidad);
            dprintf(fd, "OK\n");
            return;
        }


        
    }

    if(strncmp(linea,"PING",4) == 0){
        dprintf(fd, "PONG\n");
        return;
    }

    dprintf(fd, "ERROR comando_desconocido\n");
    
    
}
int main(int argc, char* argv[]){

    if(argc!=3){
        fprintf(stderr, "Uso: %s <puerto_publico> <puerto_local>\n",argv[0]);
        return EXIT_FAILURE;
    }

    int puerto_publico= atoi(argv[1]);
    int puerto_local = atoi(argv[2]);

    int escucha_publica = crear_socket_escucha("0.0.0.0", puerto_publico);
    int escucha_local = crear_socket_escucha("0.0.0.0", puerto_local);

    int epoll_fd = epoll_create1(0);

    if(epoll_fd == -1){
        perror("epoll_create1");
        return EXIT_FAILURE;
    }

    agregar_fd_a_epoll(epoll_fd, escucha_local, FD_ESCUCHA_LOCAL);
    agregar_fd_a_epoll(epoll_fd, escucha_publica, FD_ESCUCHA_PUBLICO);

    struct epoll_event eventos[MAX_EVENTS];

    printf("[INFO] Escucha activa\n");

    while(1){
        int n = epoll_wait(epoll_fd, eventos, MAX_EVENTS, -1);

        if(n == -1){
            perror("epoll_wait");
            break;
        }

        for(int i = 0; i<n; i++){
            FdInfo* info = eventos[i].data.ptr; // info del fd

            //si el evento se registro algunos de los puertos de escucha sera un cliente, lo aceptamos
            if(info->type == FD_ESCUCHA_LOCAL || info->type == FD_ESCUCHA_PUBLICO ){
                aceptar_clientes(epoll_fd, info->fd);
            }
            else if(info->type == FD_CLIENTE){
                if(eventos[i].events == EPOLLIN){
                    manejar_lectura_cliente(epoll_fd, info);
                }
                if (eventos[i].events & (EPOLLHUP | EPOLLERR)) {
                    cerrar_conexion(epoll_fd, info);
                }
            }
        }
    }
    close(escucha_local);
    close(escucha_publica);
    close(epoll_fd);

    return EXIT_SUCCESS;
}