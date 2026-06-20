#ifndef __SERVER_H__
#define __SERVER_H__

#include <stdio.h>
typedef enum {
    FD_ESCUCHA_PUBLICO,
    FD_ESCUCHA_LOCAL,
    FD_CLIENTE,
    FD_AGENTE_REMOTO,
    FD_AGENTE_ERLANG
}FdTipo;

#define READ_BUFFER_SIZE 4096
#define WRITE_BUFFER_SIZE 4096
#define MAX_EVENTS 64

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

static int set_nobloqueante(int fd);

static int crear_socket_escucha(const char* ip, int port);

static void agregar_fd_a_epoll(int epoll_fd, int fd, FdTipo tipo);

static void aceptar_clientes(int epollFd, int escuhaFd, FdTipo tipoCliente);

static void manejar_lectura_cliente(int epollFd, FdInfo* info);

static void manejar_linea_completa(FdInfo* info, const char* linea);

static void procesar_lineas(FdInfo* info);

static void cerrar_conexion(int epollFd, FdInfo* info);

#endif