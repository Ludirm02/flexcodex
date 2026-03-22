CXX := g++
OPTFLAGS ?= -O3 -DNDEBUG -flto -march=native -funroll-loops -fomit-frame-pointer
CXXFLAGS := $(OPTFLAGS) -std=c++17 -Wall -Wextra -Wpedantic -Iinclude -Isrc -Isrc/client -Isrc/server -Isrc/network -Isrc/query
LDFLAGS := -pthread -flto
BUILD_DIR := build
BIN_DIR := bin

CLIENT_SRCS := src/network/protocol.cpp src/client/flexql_client.cpp
SERVER_SRCS := src/network/protocol.cpp src/query/sql_engine.cpp src/server/server_main.cpp

all: $(BUILD_DIR)/flexql_server $(BUILD_DIR)/flexql-client $(BUILD_DIR)/flexql_repl $(BUILD_DIR)/flexql_smoke_test $(BUILD_DIR)/flexql_benchmark \
	$(BIN_DIR)/flexql_server $(BIN_DIR)/flexql-client $(BIN_DIR)/flexql_repl $(BIN_DIR)/flexql_smoke_test $(BIN_DIR)/flexql_benchmark

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(BUILD_DIR)/flexql_server: $(SERVER_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/flexql-client: src/client/repl_main.cpp $(CLIENT_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(BUILD_DIR)/flexql_repl: $(BUILD_DIR)/flexql-client | $(BUILD_DIR)
	ln -sf flexql-client $@

$(BUILD_DIR)/flexql_smoke_test: tests/smoke_test.cpp $(CLIENT_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(BUILD_DIR)/flexql_benchmark: tests/benchmark_client.cpp $(CLIENT_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(BIN_DIR)/flexql_server: $(BUILD_DIR)/flexql_server | $(BIN_DIR)
	ln -sf ../$(BUILD_DIR)/flexql_server $@

$(BIN_DIR)/flexql-client: $(BUILD_DIR)/flexql-client | $(BIN_DIR)
	ln -sf ../$(BUILD_DIR)/flexql-client $@

$(BIN_DIR)/flexql_repl: $(BUILD_DIR)/flexql_repl | $(BIN_DIR)
	ln -sf ../$(BUILD_DIR)/flexql_repl $@

$(BIN_DIR)/flexql_smoke_test: $(BUILD_DIR)/flexql_smoke_test | $(BIN_DIR)
	ln -sf ../$(BUILD_DIR)/flexql_smoke_test $@

$(BIN_DIR)/flexql_benchmark: $(BUILD_DIR)/flexql_benchmark | $(BIN_DIR)
	ln -sf ../$(BUILD_DIR)/flexql_benchmark $@

clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)

.PHONY: all clean
