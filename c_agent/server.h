#ifndef __SERVER_H__
#define __SERVER_H__


typedef enum {
    FD_ESCUCHA_PUBLICO,
    FD_ESCUCHA_LOCAL,
    FD_CLIENTE
}FdTipo;

typedef struct {
    int fd;
    FdTipo type;
}FdInfo;


static int set_nobloqueante(int fd);

static int crear_socket_escucha(const char* ip, int port);

static void agregar_fd_a_epoll(int epoll_fd, int fd, FdTipo tipo);

static void aceptar_clientes(int epollFd, int escuhaFd);

static void manejar_lectura_cliente(int epollFd, FdInfo* info);

static void cerrar_conexion(int epollFd, FdInfo* info);

//static void parsear_linea()
#endif