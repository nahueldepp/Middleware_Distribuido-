# Compilador y flags para C
CC := gcc
CFLAGS := -Wall -Wextra -g -O2

# Compilador de Erlang
ERLC := erlc

# Ejecutable final del agente C
TARGET := agent

# Detecta si los fuentes estan en c_agent/ o en el directorio actual
C_DIR := $(if $(wildcard c_agent/main.c),c_agent,.)
ERL_DIR := $(if $(wildcard erlang_scheduler/scheduler.erl),erlang_scheduler,.)

# Includes C
CFLAGS += -I$(C_DIR)

# Fuentes C
C_SRCS := \
	$(C_DIR)/main.c \
	$(C_DIR)/server.c \
	$(C_DIR)/resource_manager.c \
	$(C_DIR)/coordinador_jobs.c

C_OBJS := $(C_SRCS:.c=.o)
C_DEPS := $(C_OBJS:.o=.d)

# Fuentes Erlang
ERL_SRCS := \
	$(ERL_DIR)/scheduler.erl \
	$(ERL_DIR)/simulador.erl \
	$(ERL_DIR)/cliente_tcp.erl

ERL_BEAMS := $(ERL_SRCS:$(ERL_DIR)/%.erl=$(ERL_DIR)/%.beam)

# Regla principal
all: $(TARGET) erlang

# Link del binario C
$(TARGET): $(C_OBJS)
	$(CC) $(CFLAGS) -o $@ $^

# Compilacion C con dependencias automaticas de headers
%.o: %.c
	$(CC) $(CFLAGS) -MMD -MP -c $< -o $@

# Compilacion Erlang
erlang: $(ERL_BEAMS)

$(ERL_DIR)/%.beam: $(ERL_DIR)/%.erl
	$(ERLC) -o $(ERL_DIR) $<

# Limpieza
clean:
	rm -f $(TARGET) $(C_OBJS) $(C_DEPS)
	rm -f $(ERL_DIR)/*.beam erl_crash.dump

# Fuerza recompilacion completa
rebuild: clean all

.PHONY: all clean rebuild erlang

-include $(C_DEPS)