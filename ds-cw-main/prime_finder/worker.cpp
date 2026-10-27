#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <memory>
#include <unistd.h>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "coordinator_worker.grpc.pb.h"
#include <grpcpp/server_builder.h>
#include <mutex>

#include <random>
#include <chrono>

#include "primality_test.h"
#include "afs_client.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace coordinator_worker;

#define MAX_RETRIES 5
#define INITIAL_DELAY_MS 1000

std::atomic<bool> isSnapshot = false;

class Worker;

class Worker
{
private:
    int worker_id_;
    std::string worker_name_;
    std::string worker_address_;
    std::unique_ptr<CoordinatorService::Stub> coordinator_stub_;
    std::unique_ptr<AFSClient> afs_client_;
    int tasks_completed_;
    std::unique_ptr<grpc::Server> worker_server_;

    // protect shared states
    std::mutex state_mutex_;

    int64_t current_line_process;
    int64_t current_start_line;
    int64_t current_end_line;
    std::string current_filename;
    std::vector<uint64_t> collected_primes;

public:
    // allow worker server to access private states
    friend class WorkerServerImp;

    Worker(const std::string &coordinator_address, const std::string &worker_name, const std::string &worker_address)
        : worker_id_(-1), worker_name_(worker_name), worker_address_(worker_address), tasks_completed_(0)
    {
        grpc::ChannelArguments args;
        args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
        args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 1000);
        args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 10000);
        args.SetInt(GRPC_ARG_MAX_CONNECTION_IDLE_MS, 600000);
        args.SetInt(GRPC_ARG_MAX_CONNECTION_AGE_MS, 0);
        args.SetMaxReceiveMessageSize(100 * 1024 * 1024);
        args.SetMaxSendMessageSize(100 * 1024 * 1024);

        std::shared_ptr<Channel> channel = grpc::CreateCustomChannel(
            coordinator_address,
            grpc::InsecureChannelCredentials(),
            args);

        coordinator_stub_ = CoordinatorService::NewStub(channel);
    }

    bool RegisterWithCoordinator()
    {
        RegisterRequest request;
        request.set_worker_name(worker_name_);
        // add worker address
        request.set_worker_address(worker_address_);

        RegisterResponse response;

        long long current_delay_ms = INITIAL_DELAY_MS;

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> jttr_dist(0, 100);

        int retries = 0;
        Status status;
        // bool success;
        while (retries < MAX_RETRIES)
        {

            ClientContext context;
            status = coordinator_stub_->RegisterWorker(&context, request, &response);

            if (status.ok())
            {
                worker_id_ = response.worker_id();
                std::string afs_addresses = response.afs_server_address();

                // Parse comma-separated server addresses (same as coordinator)
                std::vector<std::string> server_list;
                std::stringstream ss(afs_addresses);
                std::string address;
                while (std::getline(ss, address, ','))
                {
                    // Trim whitespace
                    address.erase(0, address.find_first_not_of(" \t"));
                    address.erase(address.find_last_not_of(" \t") + 1);
                    if (!address.empty())
                    {
                        server_list.push_back(address);
                    }
                }

                if (server_list.empty())
                {
                    std::cerr << "Error: No valid AFS server addresses received from coordinator" << std::endl;
                    return false;
                }

                afs_client_ = std::make_unique<AFSClient>(server_list);

                std::cout << "Worker #" << worker_id_ << " registered with " << server_list.size() << " AFS server(s)" << std::endl;
                return true;
            }

            long long sleep_ms = current_delay_ms + jttr_dist(gen);
            auto sleep_duration = std::chrono::milliseconds(sleep_ms);

            std::this_thread::sleep_for(sleep_duration);
            current_delay_ms *= 2;

            retries++;
        }

        std::cerr << "Registration failed: " << status.error_message() << std::endl;
        return false;
    }

    void Run()
    {
        int consecutive_no_tasks = 0;
        const int MAX_NO_TASK_ATTEMPTS = 5;

        while (consecutive_no_tasks < MAX_NO_TASK_ATTEMPTS)
        {
            TaskResponse task = RequestTask();

            if (!task.has_task())
            {
                consecutive_no_tasks++;
                sleep(2);
                continue;
            }

            consecutive_no_tasks = 0;

            if (ProcessTask(task))
            {
                tasks_completed_++;
            }
        }

        std::cout << "Worker #" << worker_id_ << " completed "
                  << tasks_completed_ << " tasks" << std::endl;
    }

private:
    TaskResponse RequestTask()
    {
        TaskRequest request;
        request.set_worker_id(worker_id_);

        TaskResponse response;

        long long current_delay_ms = INITIAL_DELAY_MS;

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> jttr_dist(0, 100);

        int retries = 0;
        Status status;

        while (retries < MAX_RETRIES)
        {
            ClientContext context;
            status = coordinator_stub_->RequestTask(&context, request, &response);

            if (status.ok())
            {
                return response;
            }
            RegisterWithCoordinator();
            long long sleep_ms = current_delay_ms + jttr_dist(gen);
            auto sleep_duration = std::chrono::milliseconds(sleep_ms);
            std::this_thread::sleep_for(sleep_duration);
            current_delay_ms *= 2;
            retries++;
        }

        std::cerr << "Task request failed: " << status.error_message() << std::endl;

        return response;
    }

    bool ProcessTask(const TaskResponse &task)
    {
        int task_id = task.task_id();
        std::string filename = task.filename();
        int64_t start_line = task.start_line();
        int64_t end_line = task.end_line();

        std::string content;
        if (!afs_client_->read_file(filename, content, 0))
        {
            std::cerr << "Failed to read: " << filename << std::endl;
            return false;
        }

        // record current state
        current_filename = filename;
        current_start_line = start_line;
        current_end_line = end_line;
        current_line_process = start_line;

        std::vector<uint64_t> primes = FindPrimesInChunk(content, start_line, end_line, filename);

        return SubmitResult(task_id, primes);
    }

    std::vector<uint64_t> FindPrimesInChunk(const std::string &content,
                                            int64_t start_line,
                                            int64_t end_line,
                                            const std::string &filename)
    {
        std::vector<uint64_t> primes;
        std::istringstream iss(content);
        std::string line;
        int64_t current_line = 0;

        while (current_line < start_line && std::getline(iss, line))
        {
            current_line++;
        }

        while (current_line < end_line && std::getline(iss, line))
        {
            if (!line.empty())
            {
                try
                {
                    uint64_t num = std::stoull(line);
                    if (is_prime(num))
                    {
                        primes.push_back(num);
                        if (isSnapshot)
                        {
                            std::lock_guard<std::mutex> lock(state_mutex_);
                            current_line_process = current_line;
                            collected_primes.push_back(num);
                        }
                    }
                }
                catch (const std::exception &e)
                {
                    // Skip invalid input
                }
            }
            current_line++;
        }
        // After finishing the lines update the current line to be the end line
        current_line_process = end_line;
        return primes;
    }

    bool SubmitResult(int task_id, const std::vector<uint64_t> &primes)
    {
        ResultSubmission request;
        request.set_worker_id(worker_id_);
        request.set_task_id(task_id);

        for (uint64_t prime : primes)
        {
            request.add_primes(prime);
        }

        ResultAck response;

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> jttr_dist(0, 100);

        Status status(grpc::StatusCode::UNKNOWN, "Initial state for retry loop");

        long long current_delay_ms = INITIAL_DELAY_MS;

        int retries = 0;

        while (retries < MAX_RETRIES)
        {
            ClientContext context;
            Status status = coordinator_stub_->SubmitResult(&context, request, &response);
            if (status.ok() && response.success())
            {
                return true;
            }
            RegisterWithCoordinator();

            long long sleep_ms = current_delay_ms + jttr_dist(gen);
            auto sleep_duration = std::chrono::milliseconds(sleep_ms);

            std::this_thread::sleep_for(sleep_duration);
            current_delay_ms *= 2;

            retries++;
        }
        std::cerr << "Result submission failed for task #" << task_id << std::endl;
        return false;
    }
};

class WorkerServerImp final : public coordinator_worker::WorkerServer::Service
{
private:
    Worker *worker_ptr;

public:
    // initialize pointer to worker instance
    WorkerServerImp(Worker *ptr) : worker_ptr(ptr){}

    grpc::Status sendMarker(grpc::ServerContext *context,
                            const coordinator_worker::MarkerRequest *request,
                            coordinator_worker::MarkerResponse *response) override
    {
        // std::lock_guard<std::mutex> lock(worker_ptr_->state_mutex_);
        std::cout << "Received marker message: " << request->message() << std::endl;
        isSnapshot = true;
        std::cout << "Is snapshot? " << isSnapshot << std::endl;

        response->set_worker_id(worker_ptr->worker_id_);
        response->set_current_filename(worker_ptr->current_filename);
        response->set_current_start_line(worker_ptr->current_start_line);
        response->set_current_end_line(worker_ptr->current_end_line);
        response->set_current_line_process(worker_ptr->current_line_process);
        response->set_tasks_completed(worker_ptr->tasks_completed_);

        for (auto prime : worker_ptr->collected_primes)
        {
            response->add_collected_primes(prime);
        }

        response->set_message("Worker snapshot sent");

        return grpc::Status::OK;
    }

    grpc::Status sendHeartbeat(grpc::ServerContext *context,
                               const coordinator_worker::HeartbeatRequest *request,
                               coordinator_worker::HeartbeatResponse *response) override
    {
        std::cout << "alive!" << std::endl;
        response->set_message("alive!");

        return grpc::Status::OK;
    }
};

// Global pointer to server for graceful shutdown
std::unique_ptr<grpc::Server> g_worker_server = nullptr;

void runWorkerServer(const std::string &server_address, WorkerServerImp *service)
{
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service);

    g_worker_server = builder.BuildAndStart();
    std::cout << "Worker Server Listening on " << server_address << std::endl;
    g_worker_server->Wait();
    std::cout << "Worker Server shutdown complete!" << std::endl;
}

int main(int argc, char *argv[])
{
    if (argc != 2){
        std::cerr << "Usage: " << argv[0] << " <coordinator_address>" << std::endl;
        return 1;
    }

    std::string coordinator_address = argv[1];
    std::string worker_name = "worker_" + std::to_string(getpid());

    // Use dynamic port allocation based on PID
    // Supports up to 1000 workers)
    int pid = getpid();
    int port = 50100 + (pid % 1000); // Map PID to port range 50100-50999
    std::string worker_address = "localhost:" + std::to_string(port);

    Worker worker(coordinator_address, worker_name, worker_address);

    WorkerServerImp worker_service(&worker);

    // start worker RPC server in another thread
    std::thread server_thread(runWorkerServer, worker_address, &worker_service);

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    if (!worker.RegisterWithCoordinator())
    {
        std::cerr << "Worker registration failed. Exiting..." << std::endl;
        // Note: server_thread will be terminated when process exits
        return 1;
    }

    worker.Run();

    // all tasks done, shutdown
    std::cout << "Workers completed all tasks. Shutting down server..." << std::endl;
    if(g_worker_server){
        g_worker_server->Shutdown();
    }

    if(server_thread.joinable()){
        server_thread.join();
    }

    return 0;
}