# Configuración del compilador y banderas
CC = gcc
CFLAGS = -Wall -Wextra -g -O2 -I.

# Nombre del ejecutable final
TARGET = agent

# Archivos fuente y objetos
SRCS = c_agent/server.c c_agent/main.c c_agent/resource_manager.c
OBJS = $(SRCS:.c=.o)

# Regla principal (compila el proyecto completo)
all: $(TARGET)

# Vinculación del binario final
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

# Regla genérica para compilar archivos de código fuente (.c) a objetos (.o)
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Regla para limpiar los archivos binarios generados
clean:
	rm -f $(OBJS) $(TARGET)

# Declarar que estas reglas no corresponden a archivos reales
.PHONY: all clean