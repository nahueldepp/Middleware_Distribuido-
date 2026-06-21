#ifndef __SERVER_H__
#define __SERVER_H__

#include "resource_manager.h"

#define READ_BUFFER_SIZE 4096
#define WRITE_BUFFER_SIZE 4096
#define MAX_EVENTS 64
#define MAX_NOTIFICACIONES 67

typedef enum {
    FD_ESCUCHA_PUBLICO,
    FD_ESCUCHA_LOCAL,
    FD_CLIENTE,
    FD_AGENTE_REMOTO,
    FD_AGENTE_ERLANG,
    FD_UDP_BROADCAST
}FdTipo;


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

void enviar(int epoll_fd, FdInfo* info, const char* msg);

void server_run(int puerto_publico, int puerto_local, ResourceManager *rm);


#endif