#ifndef __SERVER_H__
#define __SERVER_H__

#include "resource_manager.h"

#define READ_BUFFER_SIZE 4096
#define WRITE_BUFFER_SIZE 4096
#define MAX_EVENTS 64
#define MAX_NOTIFICACIONES 67


void enviar(int epoll_fd, FdInfo* info, const char* msg);

void server_run(int puerto_publico, int puerto_local, ResourceManager *rm);


#endif