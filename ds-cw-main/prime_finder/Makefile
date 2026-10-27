# Makefile for Distributed Prime Finder with AFS integration
SHELL := /bin/bash

# Compilers
CC = gcc
CXX = g++

# Flags
CFLAGS = -Wall -g -std=c99
CXXFLAGS = -std=c++17 -Wall -pthread
LDFLAGS = -lm

# Proto and gRPC settings
PROTO_DIR = ../Basic_Operation/proto_files
BUILD_DIR = ../Basic_Operation/build
PROTO_OBJS = $(BUILD_DIR)/afs_operation.pb.o $(BUILD_DIR)/afs_operation.grpc.pb.o

# Local coordinator-worker proto
LOCAL_PROTO = coordinator_worker
LOCAL_PROTO_SRC = $(LOCAL_PROTO).proto
LOCAL_PROTO_CC = $(LOCAL_PROTO).pb.cc $(LOCAL_PROTO).grpc.pb.cc
LOCAL_PROTO_H = $(LOCAL_PROTO).pb.h $(LOCAL_PROTO).grpc.pb.h
LOCAL_PROTO_OBJS = $(LOCAL_PROTO).pb.o $(LOCAL_PROTO).grpc.pb.o

# gRPC and Protobuf libraries
GRPC_LDFLAGS = -lgrpc++ -lgrpc -lgpr -lprotobuf -pthread -lgmp

# Include paths
INCLUDES = -I$(BUILD_DIR) -I.

# Targets
COORDINATOR_TARGET = coordinator
WORKER_TARGET = worker

# Object files
C_OBJS = primality_test.o
COORDINATOR_OBJS = coordinator.o afs_client.o $(C_OBJS) $(LOCAL_PROTO_OBJS)
WORKER_OBJS = worker.o afs_client.o $(C_OBJS) $(LOCAL_PROTO_OBJS)

# Default target: build coordinator and worker
all: proto $(COORDINATOR_TARGET) $(WORKER_TARGET)

# Compile local proto files
proto: $(LOCAL_PROTO_SRC)
	@echo "Compiling coordinator_worker proto..."
	protoc --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` $(LOCAL_PROTO_SRC)
	@echo "✓ Proto files generated"

# Compile proto object files
$(LOCAL_PROTO).pb.o: $(LOCAL_PROTO).pb.cc
	$(CXX) $(CXXFLAGS) -c $(LOCAL_PROTO).pb.cc

$(LOCAL_PROTO).grpc.pb.o: $(LOCAL_PROTO).grpc.pb.cc
	$(CXX) $(CXXFLAGS) -c $(LOCAL_PROTO).grpc.pb.cc

# Build coordinator (task distributor)
$(COORDINATOR_TARGET): check_proto $(COORDINATOR_OBJS)
	@echo "Linking coordinator with AFS client..."
	$(CXX) $(CXXFLAGS) $(COORDINATOR_OBJS) $(PROTO_OBJS) -o $(COORDINATOR_TARGET) $(LDFLAGS) $(GRPC_LDFLAGS)
	@echo "✓ Built $(COORDINATOR_TARGET)"

# Build worker (independent worker process)
$(WORKER_TARGET): check_proto $(WORKER_OBJS)
	@echo "Linking worker with AFS client..."
	$(CXX) $(CXXFLAGS) $(WORKER_OBJS) $(PROTO_OBJS) -o $(WORKER_TARGET) $(LDFLAGS) $(GRPC_LDFLAGS)
	@echo "✓ Built $(WORKER_TARGET)"

# Check if proto files are built
check_proto:
	@if [ ! -f "$(BUILD_DIR)/afs_operation.pb.o" ]; then \
		echo "Error: Proto files not compiled. Please run 'make' in ../Basic_Operation first."; \
		exit 1; \
	fi

# C++ compilation rules
coordinator.o: coordinator.cpp afs_client.h primality_test.h $(LOCAL_PROTO_H)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c coordinator.cpp

worker.o: worker.cpp afs_client.h primality_test.h $(LOCAL_PROTO_H)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c worker.cpp

afs_client.o: afs_client.cpp afs_client.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c afs_client.cpp

# C++ compilation rules for primality test
primality_test.o: primality_test.cpp primality_test.h
	$(CXX) $(CXXFLAGS) -c primality_test.cpp

# Setup test data (regenerate if needed)
setup:
	@echo "Setting up test data..."
	mkdir -p test_data
	shuf -i 1-30000000 -n 3000000 > test_data/input_large_01.txt
	shuf -i 1-30000000 -n 3000000 > test_data/input_large_02.txt
	shuf -i 1-30000000 -n 3000000 > test_data/input_large_03.txt
	shuf -i 1-30000000 -n 3000000 > test_data/input_large_04.txt
	@echo "✓ Test data created in test_data/"

# Copy test data to AFS server directories
setup_afs:
	@echo "Copying test data to AFS server directories..."
	mkdir -p /tmp/input /tmp/output
	cp test_data/input_large_*.txt /tmp/input/
	@echo "✓ Test data copied to /tmp/input/"
	@echo "✓ Make sure AFS server is running on localhost:50051"

# Clean build artifacts
clean:
	rm -f $(COORDINATOR_TARGET) $(WORKER_TARGET) *.o
	rm -f $(LOCAL_PROTO_CC) $(LOCAL_PROTO_H)
	rm -f temp_worker_*.txt log_worker_*.txt task_*.txt progress_*.txt coordinator_state.txt
	rm -f output.txt

# Clean everything including test data
clean_all: clean
	rm -rf test_data
	rm -rf /tmp/input/* /tmp/output/*

.PHONY: all proto check_proto setup setup_afs clean clean_all

