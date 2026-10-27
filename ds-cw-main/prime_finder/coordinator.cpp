#include <iostream>
#include <sstream>
#include <string>

#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <list>

#include <cstring>
#include <ctime>
#include <dirent.h>
#include <unistd.h>

#include <grpcpp/grpcpp.h>
#include "coordinator_worker.grpc.pb.h"

#include "json.hpp"
#include <fstream>
#include "primality_test.h"
#include "afs_client.h"

#define CHUNK_SIZE 10000 // Open for configuration. Thinking if we can make it dynamic.

using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using json = nlohmann::json;
using namespace coordinator_worker;

struct Task
{
    int task_id;
    std::string filename;
    int64_t start_line;
    int64_t num_lines;
    int worker_id = -1;
};

struct WorkerInfo
{
    int worker_id;
    std::string worker_name;
    time_t register_time;
    int tasks_completed;
    std::string worker_address;
    int task_id = -1;
};

// Initialize snapshot flag
std::atomic<bool> isSnapshot = false;
// initialize map for pending tasks tracing
std::unordered_map<int, Task> pending_tasks;
std::unordered_map<int, Task> comp_tasks;

/*
 Worker client class used to create a
 channel between coordinator and worker.

 it will be used for send marker during snapshot only
*/
class workerClient
{
private:
    std::unique_ptr<WorkerServer::Stub> stub_;

public:
    workerClient(const std::string &worker_address)
    {
        std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
            worker_address,
            grpc::InsecureChannelCredentials());
        stub_ = WorkerServer::NewStub(channel);
    }

    /*
    Client (coordinator) can send marker messages to workrers
    using this stub
    */

    MarkerResponse sendMarker()
    {
        /*
        Send marker message to get the current local state of worker.
        */
        MarkerRequest request;
        request.set_message("start Snapshot now");

        MarkerResponse response;
        ClientContext context;
        grpc::Status status = stub_->sendMarker(&context, request, &response);

        // disable isSnapshot when marker message receives the marker response.
        if (status.ok())
        {
            isSnapshot = false;
        }
        else
        {
            std::cerr << "marker recevied failure" << status.error_message() << std::endl;
        }
        return response;
    }

    bool areYouAlive()
    {
        /*
        Send heartbeat message to worker
        */
        HeartbeatRequest request;
        request.set_message("Are you alive?");

        HeartbeatResponse response;
        ClientContext context;
        grpc::Status status = stub_->sendHeartbeat(&context, request, &response);

        if (status.ok())
        {
            return true;
        }
        else
        {
            return false;
        }
    }
};

class CoordinatorServer final : public CoordinatorService::Service
{
private:
    std::queue<Task> task_queue_;
    // add pending tasks list

    std::unordered_map<int, WorkerInfo> workers_;
    std::unordered_set<uint64_t> all_primes_;
    std::mutex mutex_;
    std::mutex workers_mutex_;
    int next_worker_id_;
    int total_tasks_;
    int completed_tasks_;
    std::string afs_server_address_;
    std::unique_ptr<AFSClient> afs_client_;
    friend void sendHeartbeatMessage(CoordinatorServer *server);

public:
    CoordinatorServer(const std::string &afs_addresses)
        : next_worker_id_(1), total_tasks_(0), completed_tasks_(0),
          afs_server_address_(afs_addresses)
    {
        // Parse comma-separated server addresses
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
            std::cerr << "Error: No valid AFS server addresses provided" << std::endl;
            exit(1);
        }

        afs_client_ = std::make_unique<AFSClient>(server_list);
    }

    Status RegisterWorker(ServerContext *context,
                          const RegisterRequest *request,
                          RegisterResponse *response) override
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (isSnapshot)
        {
            std::string worker_name = request->worker_name();
            std::string worker_address = request->worker_address();
            std::cerr << "worker: " << worker_name << " reqested to register" << std::endl;
            std::cerr << "with address: " << worker_address << std::endl;
        }

        int worker_id = next_worker_id_++;

        WorkerInfo info;
        info.worker_id = worker_id;
        info.worker_name = request->worker_name();
        info.register_time = time(nullptr);
        info.tasks_completed = 0;
        info.worker_address = request->worker_address();

        workers_[worker_id] = info;
        response->set_worker_id(worker_id);
        response->set_afs_server_address(afs_server_address_);

        return Status::OK;
    }

    Status RequestTask(ServerContext *context,
                       const TaskRequest *request,
                       TaskResponse *response) override
    {
        std::lock_guard<std::mutex> lock(mutex_);

        //  if (isSnapshot)
        //  {
        int worker_id = request->worker_id();
        std::cerr << "worker" << worker_id << " reqested task" << std::endl;
        //  }

        if (task_queue_.empty())
        {
            response->set_has_task(false);
            return Status::OK;
        }

        Task task = task_queue_.front();
        task_queue_.pop();

        // add task to pending list
        // add taks id to worker
        {
            std::lock_guard<std::mutex> lock(workers_mutex_);
            workers_[worker_id].task_id = task.task_id;
        }

        task.worker_id = request->worker_id();

        pending_tasks[task.task_id] = task;

        response->set_has_task(true);
        response->set_task_id(task.task_id);
        response->set_filename(task.filename);
        response->set_start_line(task.start_line);
        response->set_end_line(task.start_line + task.num_lines);

        return Status::OK;
    }

    Status SubmitResult(ServerContext *context,
                        const ResultSubmission *request,
                        ResultAck *response) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        completed_tasks_++;
        if (isSnapshot)
        {
            std::cerr << "worker: " << request->worker_id() << " submitted result" << std::endl;
            std::cerr << "prime size: " << request->primes_size() << std::endl;
            std::cerr << "primes: " << std::endl;
            for (int i = 0; i < request->primes_size(); i++)
            {
                std::cerr << "number: " << request->primes(i) << std::endl;
            }
        }

        int worker_id = request->worker_id();

        for (int i = 0; i < request->primes_size(); i++)
        {
            all_primes_.insert(request->primes(i));
        }

        if (workers_.find(worker_id) != workers_.end())
        {
            workers_[worker_id].tasks_completed++;
            // remove the task id from that worker
            workers_[worker_id].task_id = -1;
        }

        // add to complete tasks
        comp_tasks[request->task_id()] = pending_tasks[request->task_id()];

        // remove from pending tasks
        pending_tasks.erase(request->task_id());

        response->set_success(true);
        response->set_message("Task completed");

        return Status::OK;
    }

    bool InitializeTaskQueue(const std::string &input_dir)
    {
        std::vector<std::string> input_files = ScanLocalInputFiles(input_dir);

        if (input_files.empty())
        {
            std::cerr << "No input files found" << std::endl;
            return false;
        }

        int task_id = 0;

        for (const auto &filename : input_files)
        {
            std::string content;
            if (!afs_client_->read_file(filename, content, 0))
            {
                continue;
            }

            int64_t total_lines = CountLines(content);

            for (int64_t pos = 0; pos < total_lines; pos += CHUNK_SIZE)
            {
                Task task;
                task.task_id = task_id++;
                task.filename = filename;
                task.start_line = pos;
                task.num_lines = std::min((int64_t)CHUNK_SIZE, total_lines - pos);

                task_queue_.push(task);
            }
        }

        total_tasks_ = task_id;
        return total_tasks_ > 0;
    }

    bool RecoverTaskQueue()
    {
        /*
        This functions reads snapshot.json file and
        recover the latest system state.
        */
        std::string content;
        std::string filename = "snapshot.json";

        // read snapshot.json content from AFS
        if (!afs_client_->read_file(filename, content, 1))
        {
            std::cerr << "Fail to read snapshot" << std::endl;
            return false;
        }

        json snapshot;

        snapshot = json::parse(content);

        // recover remaining tasks
        for (const auto &t : snapshot["task_queue_"])
        {
            Task task;
            task.task_id = t["task_id"];
            task.filename = t["filename"];
            task.start_line = t["start_line"];
            task.num_lines = t["num_lines"];
            task_queue_.push(task);
        }

        // recover worker information
        for (const auto &w : snapshot["workers"])
        {
            WorkerInfo info;
            info.worker_id = w["id"];
            info.worker_name = w["name"];
            info.register_time = w["register_time"];
            info.tasks_completed = w["tasks_completed"];
            info.worker_address = w["address"];
        }
        // recover all found primes
        for (const auto &prime : snapshot["all_primes"])
        {
            all_primes_.insert(prime.get<uint64_t>());
        }

        // recover pending tasks
        for (const auto &t : snapshot["pending_tasks"])
        {
            Task task;
            task.task_id = t["task_id"];
            task.filename = t["filename"];
            task.start_line = t["start_line"];
            task.num_lines = t["num_lines"];
            pending_tasks[task.task_id] = task;
        }

        // recover comp tasks
        for (const auto &t : snapshot["comp_tasks"])
        {
            Task task;
            task.task_id = t["task_id"];
            task.filename = t["filename"];
            task.start_line = t["start_line"];
            task.num_lines = t["num_lines"];
            comp_tasks[task.task_id] = task;
        }

        total_tasks_ = snapshot["total tasks"];
        completed_tasks_ = snapshot["completed_tasks_"];

        return total_tasks_ > 0;
    }

    void SaveResults(const std::string &output_filename)
    {
        std::ostringstream output;
        for (uint64_t prime : all_primes_)
        {
            output << prime << "\n";
        }

        if (!afs_client_->write_file(output_filename, output.str(), 1))
        {
            std::cerr << "Failed to save results" << std::endl;
        }
    }

    void PrintStatistics()
    {
        std::cout << "\nWorkers: " << workers_.size()
                  << ", Tasks: " << completed_tasks_ << "/" << total_tasks_
                  << ", Primes: " << all_primes_.size() << std::endl;
    }

    bool IsComplete()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return completed_tasks_ >= total_tasks_;
    }

    int GetTotalTasks()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return total_tasks_;
    }

    int GetCompletedTasks()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return completed_tasks_;
    }

    std::string getTime()
    {

        std::time_t t = static_cast<std::time_t>(time(NULL));
        std::string time_str = std::ctime(&t);

        return time_str;
    }
    std::unordered_map<int, WorkerInfo> &GetWorkers()
    {
        return workers_;
    }

    void startSnapshot()
    {
        /*
        This function starts and ends snapshot as following:
        step1: record coordinator local state.
        step2: send marker message to all workers.
        step3: record all states to snapshot.json
        */

        json snapshot;
        {

            std::cout << "==================================="
                      << std::endl;
            std::cout << "\nSnapshot started\n"
                      << std::endl;
            std::cout << "==================================="
                      << std::endl;

            std::lock_guard<std::mutex> lock(mutex_);

            // add snapshot flag
            isSnapshot = true;

            // record current coordinator state
            snapshot["timestamp"] = getTime();
            snapshot["total tasks"] = total_tasks_;
            snapshot["completed_tasks_"] = completed_tasks_;
            snapshot["registered workers"] = workers_.size();
            snapshot["remaining tasks"] = total_tasks_ - completed_tasks_;
            snapshot["total prims"] = all_primes_.size();

            // add all primes to snapshot
            for (auto p : all_primes_)
                snapshot["all_primes"].push_back(p);

            // send marker to all workers
            for (const auto &pair : workers_)
            {
                const WorkerInfo &info = pair.second;
                workerClient client(info.worker_address);
                MarkerResponse response = client.sendMarker();
            }

            // record all workers
            for (const auto &pair : workers_)
            {
                const WorkerInfo &w = pair.second;
                snapshot["workers"].push_back({{"id", w.worker_id},
                                               {"name", w.worker_name},
                                               {"address", w.worker_address},
                                               {"register_time", w.register_time},
                                               {"tasks_completed", w.tasks_completed}});
            }

            // add the pending tasks to the remaining tasks list
            for (const auto &task : pending_tasks)
            {
                snapshot["pending_tasks"].push_back({{"task_id", task.second.task_id},
                                                     {"filename", task.second.filename},
                                                     {"start_line", task.second.start_line},
                                                     {"num_lines", task.second.num_lines}});
            }

            // add the comp tasks
            for (const auto &task : comp_tasks)
            {
                snapshot["comp_tasks"].push_back({{"task_id", task.second.task_id},
                                                  {"filename", task.second.filename},
                                                  {"start_line", task.second.start_line},
                                                  {"num_lines", task.second.num_lines}});
            }

            // record all remaining tasks
            std::queue<Task> tmp_taks_q = task_queue_;
            while (!tmp_taks_q.empty())
            {
                const Task &t = tmp_taks_q.front();
                snapshot["task_queue_"].push_back({{"task_id", t.task_id},
                                                   {"filename", t.filename},
                                                   {"start_line", t.start_line},
                                                   {"num_lines", t.num_lines}});
                tmp_taks_q.pop();
            }

            // add the pending tasks to the remaining tasks list
            for (const auto &task : pending_tasks)
            {
                snapshot["task_queue_"].push_back({{"task_id", task.second.task_id},
                                                   {"filename", task.second.filename},
                                                   {"start_line", task.second.start_line},
                                                   {"num_lines", task.second.num_lines}});
            }

            isSnapshot = false;
        }

        // save snapshot on AFS
        saveSnapshotFile(snapshot, "snapshot.json");

        // save on local to view it
        std::ofstream file("snapshot.json");
        file << snapshot.dump(4);

        std::cout << "==================================="
                  << std::endl;
        std::cout << "\nSnapshot ended\n"
                  << std::endl;
        std::cout << "==================================="
                  << std::endl;
    }

    int saveSnapshotFile(const json &snapshot, const std::string &output_filename)
    {
        // get snapshot string
        std::string snapshot_str = snapshot.dump(4);

        // Save to AFS server
        if (!afs_client_->write_file(output_filename, snapshot_str, 1))
        {
            std::cerr << "failed to save snapshot to AFS!" << std::endl;
            return 0;
        }

        std::cout << "snapshot.json is saved to AFS" << std::endl;
        return 1;
    }

private:
    std::vector<std::string> ScanLocalInputFiles(const std::string &input_dir)
    {
        std::vector<std::string> files;
        DIR *dir = opendir(input_dir.c_str());

        if (!dir)
            return files;

        struct dirent *entry;
        while ((entry = readdir(dir)) != nullptr)
        {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            {
                continue;
            }

            if (entry->d_type == DT_REG)
            {
                std::string filename = entry->d_name;
                if (filename.length() > 4 && filename.substr(filename.length() - 4) == ".txt")
                {
                    files.push_back(filename);
                }
            }
        }

        closedir(dir);
        return files;
    }

    int64_t CountLines(const std::string &content)
    {
        int64_t count = 0;
        for (char c : content)
        {
            if (c == '\n')
                count++;
        }
        return count;
    }
};

void MonitorProgress(CoordinatorServer *server)
{
    int last_completed = 0;
    int total_tasks = server->GetTotalTasks();

    while (!server->IsComplete())
    {
        sleep(5);

        int completed = server->GetCompletedTasks();
        if (completed != last_completed)
        {
            std::cout << "Progress: " << completed << "/" << total_tasks
                      << " (" << (completed * 100 / total_tasks) << "%)" << std::endl;
            last_completed = completed;

            // print pending tasks
            std::cout << "Pending tasks: " << pending_tasks.size() << "\n";
        }
    }
}

void sendHeartbeatMessage(CoordinatorServer *server)
{
    while (!server->IsComplete())
    {
        sleep(5);

        // lock worker list so only thread can access it at a time
        std::lock_guard<std::mutex> lock(server->workers_mutex_);
        auto &workers = server->GetWorkers();
        // iterate through workers list,
        // send heartbeat, and delete if not responding
        for (auto iterator = workers.begin(); iterator != workers.end();)
        {
            int worker_crash_id = iterator->second.worker_id;
            workerClient client(iterator->second.worker_address);
            bool alive = client.areYouAlive();
            if (alive)
            {
                std::cout << "Worker #" << worker_crash_id << " alive!\n";
                ++iterator;
            }
            else
            {
                // delete worker from workers list
                std::cout << "Worker #" << iterator->second.worker_id << " crashed!\n";
                if (iterator->second.task_id > -1)
                {
                    std::lock_guard<std::mutex> task_lock(server->mutex_);

                    // get the task to revok
                    int task_id_to_reassign = iterator->second.task_id;
                    // move task to task queue
                    Task task = pending_tasks[task_id_to_reassign];
                    task.worker_id = -1;
                    server->task_queue_.push(task);
                    // remove from pending tasks list
                    pending_tasks.erase(task_id_to_reassign);

                    std::cout << "Task #" << task_id_to_reassign << " is reassigned!\n";
                }
                else
                {
                    std::cout << "Worker #" << iterator->second.worker_id << " does not have tasks. !\n";
                }
                iterator = workers.erase(iterator);
            }
        }
        sleep(5);
    }
}

void TakeSnapshot(CoordinatorServer *server)
/*
Take snapshot periodically until tasks are completed
*/
{
    while (!server->IsComplete())
    {
        sleep(25); // wait until worker start computing
        server->startSnapshot();
    }
}

int main(int argc, char *argv[])
{
    if (argc != 5)
    {
        std::cerr << "Usage: " << argv[0]
                  << " <coordinator_port> <afs_server_addresses> <input_dir> <restart>" << std::endl;
        std::cerr << "  afs_server_addresses: Comma-separated list (e.g., \"localhost:50051,localhost:50052,localhost:50053\")" << std::endl;
        std::cerr << "  restart: Use 'restart' to recover from snapshot, 'run' for new run" << std::endl;
        return 1;
    }

    std::string coordinator_port = argv[1];
    std::string afs_server_address = argv[2];
    std::string input_dir = argv[3];
    std::string restart = argv[4];

    bool recovery = "restart" == restart;

    clock_t start_time = clock();

    CoordinatorServer coordinator(afs_server_address);

    // if system is restarting, recover from snapshot
    // else, initialize new tasks
    if (recovery)
    {
        if (!coordinator.RecoverTaskQueue())
        {
            std::cerr << "Recovery failed." << std::endl;
            return 1;
        }
    }
    else
    {
        if (!coordinator.InitializeTaskQueue(input_dir))
        {
            std::cerr << "Initialization failed." << std::endl;
            return 1;
        }
    }

    // build coordinator AFS server connection
    std::string server_address = "0.0.0.0:" + coordinator_port;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&coordinator);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Coordinator listening on " << server_address << std::endl;

    // start monitoring thread
    std::thread monitor_thread(MonitorProgress, &coordinator);

    // start snaphsot thread
    std::thread snapshot_thread(TakeSnapshot, &coordinator);
    // start heartbeat thread
    std::thread haerbeat_thread(sendHeartbeatMessage, &coordinator);

    while (!coordinator.IsComplete())
    {
        sleep(1);
    }

    monitor_thread.join();
    snapshot_thread.join();
    haerbeat_thread.join();

    coordinator.SaveResults("primes_output.txt");
    coordinator.PrintStatistics();

    clock_t end_time = clock();
    double elapsed_time = (double)(end_time - start_time) / CLOCKS_PER_SEC;

    std::cout << "Completed in " << elapsed_time << "s" << std::endl;

    server->Shutdown();

    return 0;
}