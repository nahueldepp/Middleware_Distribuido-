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
#include <time.h>

#include "server.h"





#define READ_BUFFER_SIZE 4096
#define WRITE_BUFFER_SIZE 4096
#define MAX_EVENTS 64
#define MAX_NOTIFICACIONES 67
#define MAX_NODOS 32

typedef enum {
    FD_ESCUCHA_PUBLICO,
    FD_ESCUCHA_LOCAL,
    FD_CLIENTE,
    FD_AGENTE_REMOTO,
    FD_AGENTE_ERLANG,
    FD_UDP_BROADCAST
}FdTipo;

typedef struct {
    char ip[INET_ADDRSTRLEN];
    int puerto;
    char recursos[128]; 
    time_t ultima_vez;  
} NodoVecino;

NodoVecino tabla_nodos[MAX_NODOS];
int cantidad_nodos = 0;

typedef struct {
    int fd;
    FdTipo type;

    //buffer de lectura por conexion
    //para los bytes recibidos que todavia se estan procesando
    char read_buffer[READ_BUFFER_SIZE];
    size_t read_len;

    //bytes que quiero mandar pero todavia no termine de escribir
    char write_buffer[WRITE_BUFFER_SIZE];
    //bytes totales para mandar
    size_t write_len;
    //bytes que se mandaron
    size_t write_sent;
} FdInfo;

typedef struct {
    int epoll_fd;
    ResourceManager *rm;
} ServerState;


/*Prototipos*/
static const char* obtener_recurso(int intRecurso);
static int set_nobloqueante(int fd);
static int crear_socket_escucha(const char* ip, int port);
static void agregar_fd_a_epoll(int epoll_fd, int fd, FdTipo tipo);
static void actualizar_eventos_epoll(int epoll_fd, FdInfo* info, uint16_t events);
static void cerrar_conexion(int epoll_fd, FdInfo* info);
static const char* fd_tipo(FdTipo tipo);
static void aceptar_clientes(int epollFd, int escuhaFd, FdTipo tipoCliente);
static void manejar_linea_completa(ServerState* state, FdInfo* info, const char* linea);
static void procesar_lineas(ServerState* state, FdInfo* info);
static void manejar_lectura_cliente(ServerState* state, FdInfo* info);
void enviar(int epoll_fd, FdInfo* info, const char* msg);
static int enviar_fd(int fd, const char *mensaje);
static void manejar_escritura_cliente(ServerState* state, FdInfo* info);
static int crear_socket_udp_broadcast(int puerto);
static void enviar_anuncio_presence(int udp_fd, int puerto_publico);
static void limpiar_nodos_expirados();
static void manejar_lectura_udp(int udp_fd);
/*==================================================Implementaciones ===========================================*/

static const char* obtener_recurso(int intRecurso){
    switch (intRecurso)
    {
    case 0:
        return "cpu";
        break;
    case 1:
        return "gpu";
        break;
    case 2:
        return "mem";
        break;
    default:
        return "__";
        break;
    }
}
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
    info->read_len = 0;
    info->write_len = 0;
    info->write_sent = 0;


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
    printf("[INFO] Cerrando conexion fd= %d...\n", info->fd);

    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, info->fd, NULL);
    close(info->fd);
    free(info);
}

static const char* fd_tipo(FdTipo tipo){

    switch(tipo){
        case FD_AGENTE_ERLANG:
            return "agente_erlang";
        case FD_AGENTE_REMOTO:
            return "agente_remoto";
        case FD_CLIENTE:
            return "cliente";
        case FD_ESCUCHA_LOCAL:
            return "puerto_escucha_local";
        case FD_ESCUCHA_PUBLICO:
            return "puerto_escucha_publica";
        case FD_UDP_BROADCAST:
            return "udp_broadcast";
        default:
            return "no_registrado";
    }
}

static void aceptar_clientes(int epollFd, int escuhaFd, FdTipo tipoCliente){

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

        /* Esta función convierte la estructura de dirección de red `src` de la familia de direcciones `af` en una cadena de caracteres.
        La cadena resultante se copia al búfer al que apunta `dst`.
         */
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &cliente_addr.sin_addr,ip, sizeof(ip));

        printf("[INFO] Conección aceptada fd=%d de %s:%d\n",
        cliente_fd,
        ip,
        ntohs(cliente_addr.sin_port));

        agregar_fd_a_epoll(epollFd, cliente_fd, tipoCliente);
    }
}


static void manejar_lectura_cliente(ServerState* state, FdInfo* info){

    
    while(1){
        /*Leemos que leemos de fd lo almacenamos en info->read_buffer para guardar lineas completas en el 
        caso que read() lea comandos lineas incompletas. Luego se parsean las lineas completas */


        /*Como cada fd tiene asosiado un buffer de lectura chequeamos que no este lleno*/
        if(info->read_len >= READ_BUFFER_SIZE){
            printf("[ERROR] buffer de lectura de fd:<%d> lleno", info->fd);
            handler_disconnect(state->rm, state->epoll_fd);
            cerrar_conexion(state->epoll_fd, info);
            return;
        }
        //leemos sobre read_buffer asosiado al fd a partir de donde se hizo la ultima lectura
        ssize_t n = read(info->fd,
                         info->read_buffer + info->read_len //almacenamos la lectura desde el ultimo espcaio en el buffer que se usó
                         , READ_BUFFER_SIZE - info->read_len );// tamaño restante para usar en read_buffer

        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            perror("read");
            handler_disconnect(state->epoll_fd, state->epoll_fd);
            cerrar_conexion(state->epoll_fd, info);
            return; 
        }
        
        if(n == 0){
            printf("[INFO]>> Cliente desconectado fd=%d\n", info->fd);
            handler_disconnect(state->rm, state->epoll_fd);
            cerrar_conexion(state->epoll_fd, info);
            return;
        }

        //aumentamos read_len en los n espacios que se usaron para la lectura
        info->read_len += (size_t)n;        
        
        procesar_lineas(state, info);
    }
}

static void procesar_lineas(ServerState* state, FdInfo* info){
    /* queremos determinar el largo de la ultima linea completa del tipo 
    COMANDO job_id resourse amount*/

    //valor en el que almacenaremos el comienzo de cada linea despues de un \n
    size_t comienzo = 0;

    for(size_t i = 0; i<info->read_len; i++){

        //si llegamos a un \n tenemos una linea completa para parsear
        if(info->read_buffer[i] == '\n'){
            //largo de la linea desde el ultimo \n(sin incluir) hasta el siguiente
            size_t largo_linea = i - comienzo + 1;

            char linea[1024];
            //si se pasa una linea demasiado larga
            if(largo_linea >= sizeof(linea)){
                dprintf(info->fd, "[ERROR] linea demasiado larga\n");
                comienzo= i + 1;
                continue;
            }

            //copiamos en el buffer linea a partir de una posición adelante del ultimo \n una cantidad largo_linea
            memcpy(linea, info->read_buffer + comienzo, largo_linea);
            linea[largo_linea] = '\0';

            manejar_linea_completa(state, info, linea);

            //actualizamos el comienzo en el siguiente lugar despues de un \n
            comienzo = i + 1;
        }
    }

    /*porcesamos las lineas hasta el ultimo \n, asi que movemos los que nos haya sobrado en read_buffer al principio de read_buffer*/
    if(comienzo>0){
        size_t resto_buffer = info->read_len-comienzo;
        memmove(info->read_buffer, info->read_buffer + comienzo, resto_buffer);
        info->read_len = resto_buffer;
    }
}


/*Una vez procesadas las lineas leidas procesar_lineas(), manejar_linea_completa() se encarga
de parsear el comando dado*/
static void manejar_linea_completa(ServerState* state, FdInfo* info, const char* linea){

    char cmd[32];
    char job_id[32];
    char resource[32];
    char cantidad[32];

    printf("[LINE fd:<%d> tipo:<%s>] %s\n", info->fd,fd_tipo(info->type), linea);

    if (strncmp(linea, "GET NODES", 9) == 0) {
        limpiar_nodos_expirados();

        char respuesta[4096] = "NODES\n";
        char listado[3500] = "";

        for (int i = 0; i < cantidad_nodos; i++) {
            char nodo_str[256];
            char rec_formateados[128];
            strncpy(rec_formateados, tabla_nodos[i].recursos, 128);
            
            // Reemplaza espacios por dos puntos para cumplir el protocolo de Erlang
            for(int k = 0; rec_formateados[k]; k++) {
                if(rec_formateados[k] == ' ') rec_formateados[k] = ':';
            }

            snprintf(nodo_str, sizeof(nodo_str), "%s:%d:%s", 
                     tabla_nodos[i].ip, tabla_nodos[i].puerto, rec_formateados);
            
            strcat(listado, nodo_str);
            if (i < cantidad_nodos - 1) {
                strcat(listado, ";");
            }
        }
        strcat(respuesta, listado);
        strcat(respuesta, "\n");
        enviar(state->epoll_fd, info, respuesta);
        return;
    }
    
    /*sscanf usa el formato %31s para leer el comando y el recurso pedido*/
    if(sscanf(linea, "%31s %31s %31s %31s",cmd, job_id, resource, cantidad) == 4){

        if(strncmp(cmd,"RESERVE", 7) == 0){
            printf("[INFO] RESERVE job_id:<%s> resourse:<%s> amount:<%s>\n",
            job_id, resource, cantidad);
            
            int res = handler_reserve(state->rm,info->fd, job_id, resource, cantidad);
            
            if(res == 1){
                printf("[INFO] GRANTED job_id:<%s> resourse:<%s> amount:<%s>\n",
                    job_id, resource, cantidad);
                enviar(state->epoll_fd, info, "GRANTED\n");
            }
            else if(res == 0){
                printf("[INFO] QUEUED job_id:<%s> resourse:<%s> amount:<%s>\n",
                    job_id, resource, cantidad);
                enviar(state->epoll_fd, info, "QUEUED\n");
            }
            else
                enviar(state->epoll_fd, info, "ERROR\n");
                
            return;
        }

        if(strncmp(cmd,"RELEASE", 7) == 0){
            printf("[INFO] RELEASE job_id:<%s> resourse:<%s> amount:<%s>\n",
            job_id, resource, cantidad);
            
            Notificacion notificaciones[MAX_NOTIFICACIONES];
            int cant_notificaciones = 0;
            
            int res = handler_release(state->rm, info->fd, job_id, resource, cantidad, 
                                        notificaciones, &cant_notificaciones,MAX_NOTIFICACIONES );

            if(res == 0){
                printf("[INFO] RELEASE OK job_id:<%s> \n",job_id);
                enviar(state->epoll_fd, info, "OK\n");

                for(int i = 0; i < cant_notificaciones; i++){
                    char respuesta[128];

                    snprintf(respuesta, sizeof(respuesta), "GRANTED job_id:<%d> resource:<%s> amount:<%d>\n", 
                    notificaciones[i].job_id, 
                    obtener_recurso(notificaciones[i].recurso),
                    notificaciones[i].cantidad);

                    enviar_fd(notificaciones[i].socket, respuesta);
                }
                    
            }        
            else{
                enviar(state->epoll_fd,info, "ERROR\n");
            }                    
            return;
        }


        
    }

    if(strncmp(linea,"PING",4) == 0){
        enviar(state->epoll_fd, info, "PONG\n");
        return;
    }

    enviar(state->epoll_fd, info, "[ERROR] comando desconocido\n");
    
    
}

static int enviar_fd(int fd, const char *mensaje) {
    ssize_t n = send(fd, mensaje, strlen(mensaje), 0);

    if (n < 0) {
        perror("send");
        return -1;
    }

    return 0;
}

/*Guarda un mensaje en el buffer y activa EPOLLOUT*/
void enviar(int epoll_fd, FdInfo* info, const char* msg){
    size_t msg_len = strlen(msg);

    if(info->write_len + msg_len >= WRITE_BUFFER_SIZE){
        printf("[ERROR]>> Buffer de lectura de fd:<%d> lleno\n", info->fd);
        return;
    }

    //copiamos el mensaje en el espacio restante en write_buffer
    memcpy(info->write_buffer + info->write_len, msg, msg_len);
    info->write_len += msg_len;

    //hacemos que epoll este atento a eventos EPOLLIN y EPOLLOUT en el cliente info->fd
    actualizar_eventos_epoll(epoll_fd, info, EPOLLIN | EPOLLOUT);
}

static void manejar_escritura_cliente(ServerState* state, FdInfo* info){
    //mientras haya bytes para enviar
    while(info->write_sent < info->write_len){
        ssize_t n = write(info->fd,
             info->write_buffer + info->write_sent,// mando desde donde quede
             info->write_len - info->write_sent);//mando lo que falta

        if( n == -1){
            //no hay error de escritura, se espera a la proxima oportunidad 
            if(errno == EAGAIN || errno == EWOULDBLOCK){
                return;
            }

            perror("write");
            handler_disconnect(state->rm,state->epoll_fd);
            cerrar_conexion(state->epoll_fd, info);
            return;
        }
        info->write_sent += (size_t)n;

    }

    info->write_len = 0;
    info->write_sent = 0;

    //una vez escrito el mensaje, prestamos atención solo a los eventos de entrada
    actualizar_eventos_epoll(state->epoll_fd, info, EPOLLIN);

}

/**/
static void actualizar_eventos_epoll(int epoll_fd, FdInfo* info, uint16_t events){
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));

    ev.events = events;
    ev.data.ptr = info;

    //se modifica el tipo de evento de un fd en el q epoll esta interesado
    if(epoll_ctl(epoll_fd, EPOLL_CTL_MOD,info->fd ,&ev) == -1){
        perror("epoll_ctl MOD");
    }

}

static int crear_socket_udp_broadcast(int puerto) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) { perror("socket UDP"); exit(EXIT_FAILURE); }

    int broadcast_enable = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_BROADCAST, &broadcast_enable, sizeof(broadcast_enable)) == -1) {
        perror("setsockopt SO_BROADCAST");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        perror("setsockopt SO_REUSEADDR UDP");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(puerto);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind UDP");
        exit(EXIT_FAILURE);
    }

    if (set_nobloqueante(fd) == -1) { exit(EXIT_FAILURE); }
    return fd;
}

static void enviar_anuncio_presence(int udp_fd, int puerto_publico) {
    struct sockaddr_in bc_addr;
    memset(&bc_addr, 0, sizeof(bc_addr));
    bc_addr.sin_family = AF_INET;
    bc_addr.sin_port = htons(8888); // Puerto compartido de broadcast
    bc_addr.sin_addr.s_addr = inet_addr("255.255.255.255");

    char mensaje[256];
    snprintf(mensaje, sizeof(mensaje), "ANNOUNCE %d cpu:4 mem:8192 gpu:1\n", puerto_publico);

    sendto(udp_fd, mensaje, strlen(mensaje), 0, (struct sockaddr*)&bc_addr, sizeof(bc_addr));
}

static void limpiar_nodos_expirados() {
    time_t ahora = time(NULL);
    int i = 0;
    while (i < cantidad_nodos) {
        if (difftime(ahora, tabla_nodos[i].ultima_vez) >= 15.0) {
            printf("[UDP] Nodo caído por inactividad: %s:%d\n", tabla_nodos[i].ip, tabla_nodos[i].puerto);
            for (int j = i; j < cantidad_nodos - 1; j++) {
                tabla_nodos[j] = tabla_nodos[j + 1];
            }
            cantidad_nodos--;
        } else {
            i++;
        }
    }
}

static void manejar_lectura_udp(int udp_fd) {
    char buffer[512];
    struct sockaddr_in desde_addr;
    socklen_t desde_len = sizeof(desde_addr);

    ssize_t n = recvfrom(udp_fd, buffer, sizeof(buffer) - 1, 0, (struct sockaddr*)&desde_addr, &desde_len);
    if (n <= 0) return;
    buffer[n] = '\0';

    char cmd[32], recursos[128];
    int puerto;
    
    // El sscanf ahora busca: ANNOUNCE <puerto> <recursos>
    if (sscanf(buffer, "%31s %d %[^\n]", cmd, &puerto, recursos) == 3) {
        if (strcmp(cmd, "ANNOUNCE") == 0) {
            
            // EXTRAEMOS LA IP DINÁMICAMENTE DESDE recvfrom() COMO PIDE EL CAMBIO
            char ip_remota[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &desde_addr.sin_addr, ip_remota, sizeof(ip_remota));
            
            time_t ahora = time(NULL);
            int encontrado = 0;

            // Verificamos si ya conocemos al nodo para actualizar su timestamp
            for (int i = 0; i < cantidad_nodos; i++) {
                if (strcmp(tabla_nodos[i].ip, ip_remota) == 0 && tabla_nodos[i].puerto == puerto) {
                    tabla_nodos[i].ultima_vez = ahora;
                    strncpy(tabla_nodos[i].recursos, recursos, sizeof(tabla_nodos[i].recursos));
                    encontrado = 1;
                    break;
                }
            }

            // Si es un nodo nuevo, lo agregamos a la tabla con la IP extraída de recvfrom
            if (!encontrado && cantidad_nodos < MAX_NODOS) {
                strncpy(tabla_nodos[cantidad_nodos].ip, ip_remota, INET_ADDRSTRLEN);
                tabla_nodos[cantidad_nodos].puerto = puerto;
                strncpy(tabla_nodos[cantidad_nodos].recursos, recursos, 128);
                tabla_nodos[cantidad_nodos].ultima_vez = ahora;
                cantidad_nodos++;
                printf("[UDP] Nodo registrado dinámicamente desde recvfrom: %s:%d\n", ip_remota, puerto);
            }
        }
    }
}

void server_run(int puerto_publico, int puerto_local, ResourceManager *rm){
    int escucha_publica = crear_socket_escucha("0.0.0.0", puerto_publico);
    int escucha_local = crear_socket_escucha("127.0.0.1", puerto_local);
    int udp_fd = crear_socket_udp_broadcast(8888);

    int epoll_fd = epoll_create1(0);

    ServerState state;
    state.epoll_fd = epoll_fd;
    state.rm = rm;

    if(epoll_fd == -1){
        perror("epoll_create1");
        close(escucha_local);
        close(escucha_publica);
        close(udp_fd);
    return;
    }

    agregar_fd_a_epoll(epoll_fd, escucha_local, FD_ESCUCHA_LOCAL);
    agregar_fd_a_epoll(epoll_fd, escucha_publica, FD_ESCUCHA_PUBLICO);
    agregar_fd_a_epoll(epoll_fd, udp_fd, FD_UDP_BROADCAST);

    struct epoll_event eventos[MAX_EVENTS];

    printf("[INFO] Lanzando anuncio inicial de presencia (espera pasiva de 2 segundos)...\n");
    enviar_anuncio_presence(udp_fd, puerto_publico); // Anuncio inmediato al arrancar 
    
    time_t inicio_espera = time(NULL);
    while(difftime(time(NULL), inicio_espera) < 2.0) { // Bloque de espera activa controlada de 2s 
        int n = epoll_wait(epoll_fd, eventos, MAX_EVENTS, 500); // Ciclos cortos de timeout para capturar ráfagas UDP
        for(int i = 0; i < n; i++) {
            FdInfo* info = eventos[i].data.ptr;
            if(info->type == FD_UDP_BROADCAST && (eventos[i].events & EPOLLIN)) {
                manejar_lectura_udp(info->fd);
            }
        }
    }
    printf("[INFO] Fin del arranque inicial. Pasando a modo de escucha activa regular.\n");

    time_t ultimo_anuncio = time(NULL);

    while(1){
        // Cambiamos -1 por 1000ms (1s) para que el bucle despierte solo y podamos computar el paso del tiempo
        int n = epoll_wait(epoll_fd, eventos, MAX_EVENTS, 1000); 

        if(n == -1){
            if(errno == EINTR) continue; // Por si una señal interrumpe la llamada del epoll
            perror("epoll_wait");
            break;
        }

        time_t ahora = time(NULL);
        if(difftime(ahora, ultimo_anuncio) >= 5.0) {
            enviar_anuncio_presence(udp_fd, puerto_publico); // Re-anunciar presencia
            limpiar_nodos_expirados();                       // Limpieza automática tras 15s de silencio 
            ultimo_anuncio = ahora;
        }

        for(int i = 0; i < n; i++){
            FdInfo* info = eventos[i].data.ptr;

            if(info->type == FD_ESCUCHA_LOCAL ){
                aceptar_clientes(epoll_fd, info->fd, FD_AGENTE_ERLANG);
            }
            else if(info->type == FD_ESCUCHA_PUBLICO){
                aceptar_clientes(epoll_fd, info->fd, FD_AGENTE_REMOTO);
            }
            else if(info->type == FD_UDP_BROADCAST) {
                if(eventos[i].events & EPOLLIN) {
                    manejar_lectura_udp(info->fd);
                }
            }
            else if(info->type == FD_AGENTE_ERLANG || info->type == FD_AGENTE_REMOTO){
                //events puede tener varios flags juntos, asiq que usamos &
                if(eventos[i].events & EPOLLIN){
                    manejar_lectura_cliente(&state, info);
                }
                if (eventos[i].events & EPOLLOUT) {
                    manejar_escritura_cliente(&state, info);
                }
                if (eventos[i].events & (EPOLLHUP | EPOLLERR)) {
                    handler_disconnect(state.rm, info->fd);
                    cerrar_conexion(state.epoll_fd, info);
                    continue;
                }
            }
        }
    }
    close(escucha_local);
    close(escucha_publica);
    close(udp_fd);
    close(epoll_fd);
    return;
}
