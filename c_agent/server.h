#ifndef __SERVER_H__
#define __SERVER_H__

#include "resource_manager.h"

typedef struct {
    int epoll_fd;
    ResourceManager *resource_manager;
} ServerState;


void server_run(int puerto_publico, int puerto_local, ResourceManager *rm);


#endif