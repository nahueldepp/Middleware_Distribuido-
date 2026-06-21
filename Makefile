# Configuración del compilador y banderas para C
CC = gcc
CFLAGS = -Wall -Wextra -g -O2 -I.

# Compilador para Erlang
ERLC = erlc
ERL_FLAGS = -o erlang_scheduler

# Nombre del ejecutable final de C
TARGET = agent

# Archivos fuente y objetos de C
SRCS = c_agent/server.c c_agent/main.c c_agent/resource_manager.c
OBJS = $(SRCS:.c=.o)

# Archivos fuente y binarios (.beam) de Erlang
ERL_SRCS = erlang_scheduler/scheduler.erl erlang_scheduler/simulador.erl erlang_scheduler/cliente_tcp.erl
ERL_BEAMS = $(ERL_SRCS:.erl=.beam)

# Regla principal: compila C y Erlang simultáneamente
all: $(TARGET) erlang

# Vinculación del binario final de C
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

# Regla genérica para compilar archivos .c a objetos .o
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Regla para compilar todos los archivos de Erlang (.erl -> .beam)
erlang: $(ERL_BEAMS)

# Regla genérica para compilar archivos .erl a .beam usando erlc
erlang_scheduler/%.beam: erlang_scheduler/%.erl
	$(ERLC) $(ERL_FLAGS) $<

# Regla para limpiar los archivos binarios generados (C y Erlang)
clean:
	rm -f $(OBJS) $(TARGET)
	rm -f erlang_scheduler/*.beam

# Declarar que estas reglas no corresponden a archivos reales
.PHONY: all clean erlang