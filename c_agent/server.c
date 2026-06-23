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
#include "coordinador_jobs.h"

#include "resource_manager.h"
#include "server.h"

#define MAX_NODOS 32


//para hacer rollback de lo que fue concedido
#define MAX_RECURSOS_JOB 32
#define MAX_IP_LEN 64
#define MAX_RECURSO_LEN 32

/*Estructura usada mintras se intenta reservar recursos
para el proceso tmporal de JOB_REQUEST*/
typedef struct {
    char ip[MAX_IP_LEN];
    int puerto;
    char recurso[MAX_RECURSO_LEN];
    int cantidad;
    int es_local;
    int concedido;
} ReservaParcial;

// Estructura para registrar los atributos y la última actividad de los nodos vecinos
typedef struct {
    char ip[INET_ADDRSTRLEN];
    int puerto;
    char recursos[128]; 
    time_t ultima_vez;  
} NodoVecino;

NodoVecino tabla_nodos[MAX_NODOS];
int cantidad_nodos = 0;

/* Prototipos de funciones internas */
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
static void enviar_anuncio_presence(int udp_fd, int puerto_publico, ResourceManager *rm);
static void limpiar_nodos_expirados();
static void manejar_lectura_udp(int udp_fd);
static int parse_request_job(const char *token,char *ip,size_t ip_size,int *puerto,char *recurso,size_t recurso_size,int *cantidad);

// Retorna la representación en texto del índice numérico de un recurso
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

// Inicializa un socket TCP pasivo, configura la reutilización de puertos y lo pone a escuchar
static int crear_socket_escucha(const char* ip, int port){
    /*Creo socket TCP*/
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if(fd == -1){
        perror("socket");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    // SO_REUSEADDR: Permite reutilizar la dirección y el puerto local inmediatamente 
    // tras cerrar el proceso, evitando el error "Address already in use"
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

    // Convierte la dirección IP en formato string a una estructura de red binaria
    if(inet_pton(AF_INET, ip, &addr.sin_addr) <= 0){
        perror("inet_pton");
        close(fd);
        exit(EXIT_FAILURE);
    }

    // Vincula y asigna de forma física la dirección de red y el puerto al socket
    if(bind(fd, (struct sockaddr*) &addr, sizeof(addr) ) == -1){
        perror("bind");
        close(fd);
        exit(EXIT_FAILURE);
    }


    // Configura la longitud máxima de conexiones pendientes (SOMAXCONN) para alta concurrencia
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

// Acepta ráfagas de conexiones entrantes de forma no bloqueante y las registra en epoll
static void aceptar_clientes(int epollFd, int escuhaFd, FdTipo tipoCliente){

    while(1){
        struct sockaddr_in cliente_addr;
        socklen_t cliente_len = sizeof(cliente_addr);

        //acepto el cliente
        int cliente_fd = accept(escuhaFd, (struct sockaddr *)&cliente_addr, &cliente_len);

        if(cliente_fd == -1){
            // EAGAIN/EWOULDBLOCK indica que el socket no bloqueante ya no tiene conexiones pendientes por aceptar
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

        // Esta función convierte la estructura de dirección de red `src` de la familia de direcciones `af` en una cadena de caracteres
        // La cadena resultante se copia al búfer al que apunta `dst`
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &cliente_addr.sin_addr,ip, sizeof(ip));

        printf("[INFO] Conección aceptada fd=%d de %s:%d\n",
        cliente_fd,
        ip,
        ntohs(cliente_addr.sin_port));

        agregar_fd_a_epoll(epollFd, cliente_fd, tipoCliente);
    }
}

// Lee los buffers de red y gestiona la desconexión o fallas críticas de los descriptores
static void manejar_lectura_cliente(ServerState* state, FdInfo* info){

    while(1){
        /* Leemos lo que llega de fd y lo almacenamos en info->read_buffer */

        /* Como cada fd tiene asociado un buffer de lectura chequeamos que no este lleno */
        
        
        ssize_t n = read(info->fd,
                         info->read_buffer + info->read_len,
                         READ_BUFFER_SIZE - info->read_len);

        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            perror("read");
            
            /* // Manejo de notificaciones en error de lectura
            Notificacion notificaciones[MAX_NOTIFICACIONES];
            int cant_notificaciones = 0;
            handler_disconnect(state->rm, info->fd, notificaciones, &cant_notificaciones, MAX_NOTIFICACIONES);
            
            for(int i = 0; i < cant_notificaciones; i++){
                char respuesta[128];
                snprintf(respuesta, sizeof(respuesta), "GRANTED %d\n", 
                         notificaciones[i].job_id);
            } */
            
            cerrar_conexion(state->epoll_fd, info);
            return; 
        }
        /*El otro lado cerro la conexion de forma ordenada*/
        if (n == 0) {

            cerrar_conexion(state->epoll_fd, info);
            return;
        } 
        info->read_len += (size_t)n;        
        procesar_lineas(state, info);
    }
}

/*parsea una request de forma  @IP:PUERTO:RECURSO:CANTIDAD*/
static int parse_request_job(const char *token,char *ip,size_t ip_size,int *puerto,char *recurso,size_t recurso_size,int *cantidad){

    char ip_tmp[MAX_IP_LEN];
    char recurso_tmp[MAX_RECURSO_LEN];
    int puerto_tmp;
    int cantidad_tmp;

    int parsed = sscanf(token, "@%63[^:]:%d:%31[^:]:%d", ip_tmp, &puerto_tmp, recurso_tmp, &cantidad_tmp);
    if(parsed != 4){
        return 0;
    }

    if(cantidad_tmp <= 0 || puerto_tmp <= 0){
        return 0;
    }

    strncpy(ip, ip_tmp, ip_size - 1);
    ip[ip_size -1] = '\0';

    strncpy(recurso, recurso_tmp, recurso_size);
    recurso[recurso_size - 1] = '\0';
    *puerto = puerto_tmp;
    *cantidad = cantidad_tmp;

    return 1;
}

static int es_recurso_local(ServerState* state, int puerto_req){
    return puerto_req == state->puerto_publico;
}

static int reserve_local(ServerState* state, FdInfo* info, int job_id, const char* recurso, int cantidad){
    char str_id[32];
    char str_cant[32];

    snprintf(str_id, sizeof(str_id), "%d", job_id);
    snprintf(str_cant, sizeof(str_cant),"%d", cantidad);

    int res = handler_reserve(state->rm, info->fd, str_id, recurso, str_cant);

    /*
    res == 1 concedido
    res == 0 puede significar encolado/no concedido
    Para JOB_REQUEST compuesto(ej:JOB_REQUEST 1002 @127.0.0.1:8100:cpu:1 @127.0.0.1:8101:gpu:1), solo aceptamos concedido de inmediato*/
    return res == 1;
}

static void releace_local(ServerState* state, FdInfo* info, int job_id, const char* recurso, int cantidad){

    char str_id[32];
    char str_cant[32];

    snprintf(str_id, sizeof(str_id), "%d", job_id);
    snprintf(str_cant, sizeof(str_cant),"%d", cantidad);

    Notificacion notificaciones[MAX_NOTIFICACIONES];
    int c_not = 0;

    handler_release(state->rm, info->fd, str_id, recurso, str_cant, notificaciones, &c_not, MAX_NOTIFICACIONES);
    /*Solo llamamos liberar_local para reservas que ya sabemos que fueron concedidas, no necesitamos recorrer*/
    return;
}

static int recv_line_blocking(int fd, char *buffer, size_t buffer_size) {
    size_t pos = 0;
    while (pos + 1 < buffer_size) {
        char c;
        
        ssize_t n = recv(fd, &c, 1, 0);
        
        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }

            perror("[REMOTE] recv_line");
            return 0;
        }
        
        if (n == 0) {
            printf("[REMOTE] conexion cerrada antes de recibir linea completa\n");
            return 0;
        }

        buffer[pos++] = c;
        
        if (c == '\n') {
            break;
        }
    }
    
    buffer[pos] = '\0';

    return pos > 0;
}
static int reserve_remoto_bloqueante(const char* ip, int puerto, int job_id, const char* recurso, int cantidad){

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    
    if(fd == -1){
        perror("socket remote_reserve");
        return 0;
    }
    
    struct sockaddr_in addr; 
    memset(&addr, 0, sizeof(addr));
    
    addr.sin_family = AF_INET; 
    addr.sin_port = htons(puerto);
    
    if(inet_pton(AF_INET, ip, &addr.sin_addr) != 1){
        fprintf(stderr, "[ERROR] IP remota invalida: %s\n", ip);
        close(fd);
        return 0;
    }
    if(connect(fd, (struct sockaddr*) &addr, sizeof(addr)) == -1){
        perror("connect release_remoto");
        close(fd);
        return 0;
    }
    
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "RESERVE %d %s %d\n", job_id, recurso, cantidad);
    
    if(send(fd, cmd, strlen(cmd), MSG_NOSIGNAL) == -1){
        perror("send remote_reserve");
        close(fd);
        return 0;
    }
    
    char resp[128];
    
    if (recv_line_blocking(fd, resp, sizeof(resp)) == 0) {
        close(fd);
        return 0;
    }

    resp[strlen(resp)-1] = '\0';
    printf("[REMOTE] respuesta=<%s>\n", resp);


    char resp_cmd[32];
    int resp_job_id;

    if(sscanf(resp, "%31s %d", resp_cmd, &resp_job_id) != 2){
        printf("[REMOTE] respuesta invalida=<%s>\n", resp);
        close(fd);
        return 0;
    }

    if (strcmp(resp_cmd, "GRANTED") == 0 && resp_job_id == job_id) {
        close(fd);
        return 1;
    }

    close(fd);
    return 0;
}

static void releace_remoto_bloqueante(const char* ip, int puerto, int job_id, const char* recurso, int cantidad){
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if(fd == -1){
        perror("socket release_remoto");
        return ;
    }

    struct sockaddr_in addr;
    memset(&addr,0 ,sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(puerto);

    if(inet_pton(AF_INET, ip, &addr.sin_addr) != 1){
        fprintf(stderr, "[ERROR] IP remota inválida: %s\n", ip);
        close(fd);
        return;
    }

    if(connect(fd, (struct sockaddr*) &addr, sizeof(addr)) == -1){
        perror("connect release_remoto");
        close(fd);
        return;
    }

    char cmd[256];
    snprintf(cmd, sizeof(cmd), "RELEASE %d %s %d\n", job_id, recurso, cantidad);
    
    if(send(fd, cmd, strlen(cmd), MSG_NOSIGNAL) == -1){
        perror("send release_remoto");
        close(fd);
        return;
    }
   

    close(fd);
}

/*Si ocurre un fallo o desconeccion rollback_reserve libera jobs en orden contrario a los que se reservo*/
static void rollback_reserve(ServerState* state, FdInfo* info, int job_id, ReservaParcial parciales[], int cant_parciales){

    printf("[JOB] Rollback job:<%d>\n", job_id);

    for(int i = cant_parciales - 1; i >= 0; i--){
        ReservaParcial* r = &parciales[i];

        if(!r->concedido){
            continue;
        }

        if(r->es_local){
            /*si es local tengo que liberar los recursos de manera local usando e resourse manager*/
            printf("[JOB] Rollback local job_id:<%d> resource:<%s> amount:<%d>\n",
                job_id, r->recurso, r->cantidad);
            
            releace_local(state, info, job_id, r->recurso, r->cantidad);   
        }
        else{
            /*si no es local tengo que pedir al agent c remoto que los libere*/
            printf("[JOB] Rollback remoto job_id:<%d> node:<%s:%d> resource:<%s> amount:<%d>\n",
            job_id, r->ip, r->puerto, r->recurso, r->cantidad );
            releace_remoto_bloqueante(r->ip, r->puerto, job_id, r->recurso,r->cantidad);
        }
        r->concedido = 0;
    }
}


static void manejar_job_request(ServerState* state,FdInfo* info,const char* linea){

    char linea_copia[1024];

    strncpy(linea_copia, linea, sizeof(linea_copia) - 1);
    linea_copia[sizeof(linea_copia) - 1] = '\0';

    char* saveptr = NULL;

    /*segun el manual:
    The  strtok_r()  function is a reentrant version of strtok().  The saveptr argument is a pointer to a char * variable that is used internally by strtok_r() in order
    to maintain context between successive calls that parse the same string.
    recordemos que tenemos que parsear algo de la forma
    JOB REQUEST 1001 @192.168.1.2:cpu:2 @192.168.1.3:gpu:1
    */
    char* cmd = strtok_r(linea_copia, " \t\r\n", &saveptr);
    char* job_id_str = strtok_r(NULL, " \t\r\n", &saveptr);
    
    if(cmd == NULL || job_id_str == NULL){
        enviar(state->epoll_fd, info, "JOB_DENIED 0\n");
        return;
    }

    int job_id = atoi(job_id_str);
    if(job_id <= 0){
        enviar(state->epoll_fd,info, "JOB_DENIED 0\n");
        return;
    }

    /*evita que dos JOB_REQUEST distintos usen el mismo job_id y se pisen en la tabla*/
    if (coordinator_jobs_get(&state->coordinador_jobs, job_id) != NULL) {
    char respuesta[128];
    snprintf(respuesta, sizeof(respuesta), "JOB_DENIED %d\n", job_id);
    enviar(state->epoll_fd, info, respuesta);
    return;
}

    ReservaParcial parciales[MAX_RECURSOS_JOB];
    int cant_parciales = 0; 

    int todos_concedidos = 1;
    int hubo_al_menos_un_recurso = 0;

    char* token;

    while((token = strtok_r(NULL, " \t\r\n", &saveptr)) != NULL){
        if(token[0] != '@'){
            continue;
        }

        hubo_al_menos_un_recurso = 1;

        if(cant_parciales >= MAX_RECURSOS_JOB){
            todos_concedidos = 0;
            break;
        }

        ReservaParcial* reserva = &parciales[cant_parciales];
        memset(reserva, 0 , sizeof(*reserva));

        if(!parse_request_job(token,reserva->ip,sizeof(reserva->ip) ,&reserva->puerto, 
                reserva->recurso, sizeof(reserva->recurso),&reserva->cantidad )){
            
            printf("[JOB] Token invalido en JOB_REQUEST: %s\n", token);
            todos_concedidos = 0;
            break;
        }

        reserva->es_local = es_recurso_local(state, reserva->puerto);
        reserva->concedido = 0;

        printf("[JOB] Intentando reserva job=%d nodo=%s:%d recurso=%s cant=%d local=%d\n",
               job_id,
               reserva->ip,
               reserva->puerto,
               reserva->recurso,
               reserva->cantidad,
               reserva->es_local);

        int ok;

        if(reserva->es_local){
            //printf("Entre al reserve_local\n");
            ok = reserve_local(state, info, job_id, reserva->recurso, reserva->cantidad);
        }
        else{
           // printf("Entre al reserve_remoto_bloqueante\n");
            ok = reserve_remoto_bloqueante(reserva->ip, reserva->puerto, job_id, reserva->recurso, reserva->cantidad);
        }

        if(!ok){
            printf("[JOB] Reserva fallida job_id:<%d> node:<%s:%d> resource:<%s> amount:<%d>\n",
                job_id, reserva->ip, reserva->puerto, reserva->recurso, reserva->cantidad);

            todos_concedidos = 0;
            break;
        }

        reserva->concedido = 1;
        cant_parciales++;
    }

    char respuesta[128];

    if(todos_concedidos && hubo_al_menos_un_recurso){
        if (!coordinator_jobs_create(&state->coordinador_jobs, job_id)) {

        rollback_reserve(state, info, job_id, parciales, cant_parciales);
        snprintf(respuesta, sizeof(respuesta), "JOB_DENIED %d\n", job_id);
        enviar(state->epoll_fd, info, respuesta);
        return;
    }

        for (int i = 0; i < cant_parciales; i++) {
            ReservaParcial *r = &parciales[i];

            coordinator_jobs_add_resource(&state->coordinador_jobs,
                                        job_id,
                                        r->ip,
                                        r->puerto,
                                        r->recurso,
                                        r->cantidad,
                                        r->es_local);
        }
        snprintf(respuesta, sizeof(respuesta), "JOB_GRANTED %d\n", job_id);
        printf("[JOB] JOB_GRANTED %d\n", job_id);
    }
    else{
        rollback_reserve(state, info, job_id, parciales, cant_parciales);

        snprintf(respuesta, sizeof(respuesta),"JOB_DENIED %d\n", job_id);

        printf("[JOB] JOB_DENIED %d\n", job_id);
    }

    enviar(state->epoll_fd, info, respuesta);


}

static void manejar_job_release(ServerState* state,FdInfo* info,const char* linea){
    char cmd[32];
    int job_id;

    if (sscanf(linea, "%31s %d", cmd, &job_id) != 2) {
        enviar(state->epoll_fd, info, "ERROR\n");
        return;
    }

    CoordinatedJob *job = coordinator_jobs_get(&state->coordinador_jobs, job_id);

    if (job == NULL) {
        enviar(state->epoll_fd, info, "ERROR\n");
        return;
    }
    /*La razón de liberar en orden inverso es la misma que en rollback: 
    si se reservo A después B después C, soltar C, B, A es más prolijo.*/
    for (int i = job->cant_recursos - 1; i >= 0; i--) {
        CoordinatedResource *r = &job->recursos[i];

        if (r->es_local) {
            releace_local(state,info,job_id,r->recurso,  r->cantidad);
        } else {
            releace_remoto_bloqueante(r->ip,r->puerto,job_id,r->recurso,  r->cantidad);
        }
    }

    coordinator_jobs_remove(&state->coordinador_jobs, job_id);

    enviar(state->epoll_fd, info, "OK\n");
}

// Trocea el flujo continuo de bytes buscando caracteres '\n' para extraer líneas completas
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

// Una vez procesadas las lineas leidas procesar_lineas(), manejar_linea_completa() se encarga de parsear el comando dado
static void manejar_linea_completa(ServerState* state, FdInfo* info, const char* linea){

    char cmd[32];
    char job_id[32];
    char resource[32];
    char cantidad[32];

    printf("[LINE fd:<%d> tipo:<%s>] %s\n", info->fd,fd_tipo(info->type), linea);

    // Comando GET NODES: Devuelve el listado de nodos vecinos activos formateado para erlang
    if (strncmp(linea, "GET NODES", 9) == 0) {
        limpiar_nodos_expirados();

        char respuesta[4096] = "";
        char listado[3500] = "";

        for (int i = 0; i < cantidad_nodos; i++) {
            char nodo_str[256];
            char rec_formateados[128];
            strncpy(rec_formateados, tabla_nodos[i].recursos, sizeof(rec_formateados) - 1);
            rec_formateados[sizeof(rec_formateados) - 1] = '\0';
            
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
                char respuesta[32];
                snprintf(respuesta, sizeof(respuesta),"GRANTED %d\n", atoi(job_id));
                enviar(state->epoll_fd, info, respuesta);
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

        // Comando RELEASE: Libera una cantidad de hardware y despacha notificaciones pendientes
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

                    snprintf(respuesta, sizeof(respuesta), "GRANTED %d\n", 
                    notificaciones[i].job_id);

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

    // Comando JOB_REQUEST: atiende la petición compuesta de erlang local aplicando ruteo real

 
    if(strncmp(linea, "JOB_REQUEST",11) == 0){
        /*cuando A recibe  JOB_REQUEST 1005 @A:cpu:1 @B:gpu:1
        A guarda en coordinador_jobs el job completo que usa CPU en A y GPU en B.*/
        manejar_job_request(state, info, linea);
        return;

    }
     
    // Comando JOB_RELEASE: Deshace todas las asignaciones de un Job en el nodo y replica a los vecinos
    if (strncmp(linea, "JOB_RELEASE", 11) == 0) {

        /*cuando A recibe */
      manejar_job_release(state, info, linea);
      return;
    } 


    // Comando JOB_STATUS: Responde afirmativamente de forma directa para confirmar al planificador
    // distribuido que el Job consultado se encuentra activo
    if (strncmp(linea, "JOB_STATUS", 10) == 0) {
        int req_id;
        if (sscanf(linea, "JOB_STATUS %d", &req_id) == 1) {
            char buffer_respuesta[64];
            snprintf(buffer_respuesta, sizeof(buffer_respuesta), "JOB_GRANTED %d\n", req_id);
            enviar(state->epoll_fd, info, buffer_respuesta);
            return;
        }
    }

    enviar(state->epoll_fd, info, "[ERROR] comando desconocido\n");
    
    
}

// Realiza un envío síncrono y directo a través de un descriptor específico
static int enviar_fd(int fd, const char *mensaje) {
    ssize_t n = send(fd, mensaje, strlen(mensaje), 0);

    if (n < 0) {
        perror("send");
        return -1;
    }

    return 0;
}

// Acumula datos en el buffer de salida del cliente y activa EPOLLOUT para escritura asíncrona
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

// Vacía de forma no bloqueante el buffer de salida hacia el socket del cliente
static void manejar_escritura_cliente(ServerState* state, FdInfo* info){
    while(info->write_sent < info->write_len){
        ssize_t n = write(info->fd,
             info->write_buffer + info->write_sent,
             info->write_len - info->write_sent);

        if( n == -1){
            if(errno == EAGAIN || errno == EWOULDBLOCK){
                return;
            }

            perror("write");
            
            // Declaramos buffers y mandamos notificaciones en caso de error de escritura
            Notificacion notificaciones[MAX_NOTIFICACIONES];
            int cant_notificaciones = 0;
            handler_disconnect(state->rm, info->fd, notificaciones, &cant_notificaciones, MAX_NOTIFICACIONES);
            
            for(int i = 0; i < cant_notificaciones; i++){
                char respuesta[128];
                snprintf(respuesta, sizeof(respuesta), "GRANTED %d\n", 
                         notificaciones[i].job_id);
            }
            
            cerrar_conexion(state->epoll_fd, info);
            return;
        }
        info->write_sent += (size_t)n;
    }

    info->write_len = 0;
    info->write_sent = 0;

    actualizar_eventos_epoll(state->epoll_fd, info, EPOLLIN);
}

// Modifica la máscara de eventos (intereses) de un descriptor dentro de epoll
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

// Inicializa y configura un socket UDP pasivo listo para recibir anuncios por broadcast
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

// Emite un paquete de broadcast UDP informando el puerto público y la disponibilidad de hardware actual
static void enviar_anuncio_presence(int udp_fd, int puerto_publico, ResourceManager *rm) {
    struct sockaddr_in bc_addr;
    memset(&bc_addr, 0, sizeof(bc_addr));
    bc_addr.sin_family = AF_INET;
    bc_addr.sin_port = htons(12529); 
    bc_addr.sin_addr.s_addr = inet_addr("255.255.255.255");

    char mensaje[256];
    // EXTRAE LOS VALORES TOTALES DEL RESOURCE MANAGER
    snprintf(mensaje, sizeof(mensaje), "ANNOUNCE %d cpu:%d mem:%d gpu:%d\n",
             puerto_publico,
             rm->cpu->disponible,
             rm->mem->disponible,
             rm->gpu->disponible);

    sendto(udp_fd, mensaje, strlen(mensaje), 0,
           (struct sockaddr*)&bc_addr, sizeof(bc_addr));
}

// Remueve de la tabla global aquellos nodos vecinos que no enviaron señales de vida por más de 15 segundos
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

// Recibe los anuncios de presencia UDP y registra o actualiza dinámicamente los vecinos usando recvfrom
static void manejar_lectura_udp(int udp_fd) {
    char buffer[512];
    struct sockaddr_in desde_addr;
    socklen_t desde_len = sizeof(desde_addr);

    ssize_t n = recvfrom(udp_fd, buffer, sizeof(buffer) - 1, 0, (struct sockaddr*)&desde_addr, &desde_len);
    if (n <= 0) return;
    buffer[n] = '\0';

    char cmd[32], recursos[128];
    int puerto;
    
    if (sscanf(buffer, "%31s %d %[^\n]", cmd, &puerto, recursos) == 3) {
        if (strcmp(cmd, "ANNOUNCE") == 0) {
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
                printf("ANNOUNCE: %s:%d:%s\n", ip_remota, puerto, recursos);
            }
        }
    }
}

// Lazo principal del Servidor: orquesta la inicialización de sockets, descubrimiento pasivo y multiplexación con epoll
void server_run(int puerto_publico, int puerto_local, ResourceManager *rm){
    int escucha_publica = crear_socket_escucha("0.0.0.0", puerto_publico);
    int escucha_local = crear_socket_escucha("127.0.0.1", puerto_local);
    int udp_fd = crear_socket_udp_broadcast(12529);

    int epoll_fd = epoll_create1(0);

    ServerState state;
    state.epoll_fd = epoll_fd;
    state.rm = rm;
    state.puerto_local = puerto_local;
    state.puerto_publico = puerto_publico;
    coordinator_jobs_init(&state.coordinador_jobs);

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
    enviar_anuncio_presence(udp_fd,puerto_publico,rm); // Anuncio inmediato al arrancar 
    
    // Bloque de descubrimiento inicial: 2 segundos de escucha activa para armar el vecindario antes de aceptar clientes
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

    // Lazo regular no bloqueante controlado por timeouts de 1 segundo para alarmas de tiempo
    while(1){
        // Cambiamos -1 por 1000ms para que el bucle despierte solo y podamos computar el paso del tiempo
        int n = epoll_wait(epoll_fd, eventos, MAX_EVENTS, 1000); 

        if(n == -1){
            if(errno == EINTR) continue; // Por si una señal interrumpe la llamada del epoll
            perror("epoll_wait");
            break;
        }

        time_t ahora = time(NULL);
        if(difftime(ahora, ultimo_anuncio) >= 5.0) {
            enviar_anuncio_presence(udp_fd, puerto_publico, state.rm); // Re-anunciar presencia
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
                if(eventos[i].events & EPOLLIN){
                    manejar_lectura_cliente(&state, info);
                }
                if (eventos[i].events & EPOLLOUT ) {
                    manejar_escritura_cliente(&state, info);
                }
                
                // Gestiona desconexiones abruptas o errores de hardware detectados por epoll
                if (eventos[i].events & (EPOLLHUP | EPOLLERR)) {
                    Notificacion notificaciones[MAX_NOTIFICACIONES];
                    int cant_notificaciones = 0;
                    
                    handler_disconnect(state.rm, info->fd, notificaciones, &cant_notificaciones, MAX_NOTIFICACIONES);
                    
                    for(int k = 0; k < cant_notificaciones; k++){
                        char respuesta[128];
                        snprintf(respuesta, sizeof(respuesta), "GRANTED %d\n", 
                                 notificaciones[k].job_id);
                    }
                    
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
