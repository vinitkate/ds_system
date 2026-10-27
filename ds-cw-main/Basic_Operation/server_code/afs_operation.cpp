#include <cstdlib>
#include <ctime>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <filesystem> 

#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include "afs_operation.grpc.pb.h"
#include "afs_operation.pb.h"

using namespace afs_operation;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ClientContext;
using grpc::Channel;

struct FileInfo{
    std::string filename;
    std::string server_path;
    int64_t timestamp;
};

// Typedef for internal stubs for clarity
using InternalStubPtr = std::unique_ptr<InternalReplication::Stub>;

class FileSystem final : public operators::Service, public InternalReplication::Service {
private:
    // a constant for the directory where the files are stored on the server
    const std::string SERVER_INPUT_FILES_ROOT = "/tmp/input/";
    const std::string SERVER_OUTPUT_FILES_ROOT = "/tmp/output/";

    // New Primary-Backup state members
    std::atomic<bool> running_ = true; //  To control the loop
    std::condition_variable cv_; // To wake up the thread
    std::atomic<StatusResponse::ServerRole> server_role_ = StatusResponse::BACKUP;
    std::string server_address_;
    std::vector<std::string> all_server_addresses_;
    std::unordered_map<std::string, InternalStubPtr> peer_stubs_;
    std::thread heartbeat_thread_;
    std::atomic<int> current_term_ = 0; // The current election term (epoch)
    std::chrono::milliseconds heartbeat_timeout_ = std::chrono::milliseconds(5000);
    std::chrono::time_point<std::chrono::high_resolution_clock> last_heartbeat_time_;

    std::mutex file_mutex_; // To protect file operations
    std::mutex state_mutex_; // Protects role, term, voted_for, and last_commit_index_
    std::string voted_for_;  // Tracks who this server voted for in the current term

    int64_t last_commit_index_ = 0;
    
    // Primary-Backup Replication Helpers
    // -----------------------------------------------------
    void InitializePeers(const std::vector<std::string>& addresses){
        all_server_addresses_ = addresses;
        
        for (const auto& addr : addresses){
            if (addr != server_address_){
                // Convert 0.0.0.0 to localhost for connection
                std::string connect_addr = addr;
                if (connect_addr.find("0.0.0.0:") == 0){
                    connect_addr.replace(0, 7, "localhost");
                }
                // Set max message size for server-to-server replication (100MB to handle large files - temporary fix)
                grpc::ChannelArguments channel_args;
                channel_args.SetMaxReceiveMessageSize(100 * 1024 * 1024);
                channel_args.SetMaxSendMessageSize(100 * 1024 * 1024);
                auto channel = grpc::CreateCustomChannel(connect_addr, grpc::InsecureChannelCredentials(), channel_args);
                peer_stubs_[addr] = InternalReplication::NewStub(channel);
            }
        }
    }


// runs only on Primary
void HeartbeatLoop(){
    std::cout << "[PRIMARY] " << server_address_ << " started heartbeat loop (Term: " << current_term_ << ")" << std::endl;
    int heartbeat_count = 0;
    
    while(server_role_ == StatusResponse::PRIMARY){
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        heartbeat_count++;
        
        HeartbeatRequest request;
        request.set_primary_id(server_address_);
        request.set_term(current_term_);
        
        int success_count = 1;
        
        for(auto const& [addr, stub] : peer_stubs_){
            HeartbeatResponse response;
            ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
            
            Status status = stub->Heartbeat(&context, request, &response);
            
            if(status.ok() && response.success()){
                success_count++;
                // fencing check: if backup has higher term, step down
                if(response.term() > current_term_){
                    std::cerr << "[PRIMARY] " << server_address_ << " received higher term from Backup. Stepping down." << std::endl;
                    server_role_ = StatusResponse::BACKUP;
                    return;
                }
            }else if(!status.ok()){
                std::cerr << "[PRIMARY] " << server_address_ << " heartbeat failed to " << addr << ": " << status.error_message() << std::endl;
            }
        }
        
        // check quorum
        int quorum_size = all_server_addresses_.size() / 2 + 1;
        if(success_count < quorum_size){
            std::cerr << "[PRIMARY] " << server_address_ << " quorum lost! (success: " << success_count << "/" << quorum_size << ") Stepping down." << std::endl;
            server_role_ = StatusResponse::BACKUP;
            return;
        }
        
        // Print status every 10 heartbeats
        if(heartbeat_count % 10 == 0){
            std::cout << "[PRIMARY] " << server_address_ << " sending heartbeats (Term: " << current_term_ << ", Quorum: " << success_count << "/" << quorum_size << ")" << std::endl;
        }
    }
    
    std::cout << "[PRIMARY] " << server_address_ << " heartbeat loop ended." << std::endl;
}


// runs only on Backup/Candidate
void HeartbeatTimeoutCheck(){
    while(running_ && server_role_ != StatusResponse::PRIMARY){
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        if(server_role_ == StatusResponse::BACKUP || server_role_ == StatusResponse::CANDIDATE){
            auto now = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_time_);
            
            // random jitter to avoid simultaneous elections
            auto jittered_timeout = heartbeat_timeout_ + std::chrono::milliseconds(rand() % 500 + 150);
            
            if(elapsed > jittered_timeout){
                if(server_role_ == StatusResponse::BACKUP){
                    std::cout << "[BACKUP] " << server_address_ << " heartbeat timeout (" << elapsed.count() << "ms). Primary appears dead. Initiating election." << std::endl;
                    InitiateElection();
                }else if(server_role_ == StatusResponse::CANDIDATE){
                    std::cout << "[CANDIDATE] " << server_address_ << " election timeout. Resetting to BACKUP and retrying." << std::endl;
                    {
                        std::lock_guard<std::mutex> lock(state_mutex_);
                        server_role_ = StatusResponse::BACKUP;
                        last_heartbeat_time_ = std::chrono::high_resolution_clock::now();
                    }
                }
            }
        }
    }
}

void InitiateElection(){
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        server_role_ = StatusResponse::CANDIDATE;
        current_term_++;
        voted_for_ = server_address_;
    }
    
    std::cout << "[CANDIDATE] " << server_address_ << " initiating election (Term: " << current_term_ << ")" << std::endl;
    
    int votes_granted = 1;
    int quorum_size = all_server_addresses_.size() / 2 + 1;
    
    RequestVoteRequest req;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        req.set_term(current_term_);
        req.set_candidate_id(server_address_);
        req.set_last_commit_index(last_commit_index_);
    }

    for(auto const& [addr, stub] : peer_stubs_){
        RequestVoteResponse res;
        ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

        Status status = stub->RequestVote(&context, req, &res);

        if(status.ok()){
            std::lock_guard<std::mutex> lock(state_mutex_);
            if(res.term() > current_term_){
                server_role_ = StatusResponse::BACKUP;
                current_term_ = res.term();
                return;
            }
            if(res.vote_granted()){
                votes_granted++;
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if(votes_granted >= quorum_size){
            std::cout << "[ELECTION] " << server_address_ << " won election! Becoming PRIMARY (Term: " << current_term_ << ", Votes: " << votes_granted << "/" << quorum_size << ")" << std::endl;
            server_role_ = StatusResponse::PRIMARY;
        }else{
            std::cout << "[ELECTION] " << server_address_ << " lost election (Votes: " << votes_granted << "/" << quorum_size << "). Remaining BACKUP." << std::endl;
            server_role_ = StatusResponse::BACKUP;
            last_heartbeat_time_ = std::chrono::high_resolution_clock::now();
        }
    }
    
    if(votes_granted >= quorum_size){
        if(heartbeat_thread_.joinable()){
            heartbeat_thread_.detach();
        }
        heartbeat_thread_ = std::thread(&FileSystem::HeartbeatLoop, this);
    }else{
        std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 1000 + 500));
    }
}


// primary-side write replication
bool ReplicateWriteToBackups(const ReplicateWriteRequest& request){
    int success_count = 1;
    int quorum_size = all_server_addresses_.size() / 2 + 1;

    for(auto const& [addr, stub] : peer_stubs_){
        ReplicateWriteResponse response;
        ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        
        Status status = stub->ReplicateWrite(&context, request, &response);
        
        if(status.ok() && response.success()){
            success_count++;
        }else{
            std::cerr << "Warning: Replication failed to " << addr << ": " << status.error_message() << std::endl;
        }
    }
    
    return success_count >= quorum_size;
}

// check if server is Primary before processing client requests
bool IsPrimary(ServerContext* context){
    if(server_role_ != StatusResponse::PRIMARY){
        context->AddInitialMetadata("leader_status", "not_primary");
        return false;
    }
    return true;
}


public:

    FileSystem(const std::string& address, const std::vector<std::string>& all_addresses)
        : server_address_(address), all_server_addresses_(all_addresses) {
        
        server_role_ = StatusResponse::BACKUP; // Default state of a server is BACKUP
        last_heartbeat_time_ = std::chrono::high_resolution_clock::now();
        // Start the heartbeat timeout checker thread
        heartbeat_thread_ = std::thread(&FileSystem::HeartbeatTimeoutCheck, this);
    }


    ~FileSystem() {
        running_ = false; // 1. Signal the thread to stop
        cv_.notify_all(); //2. Wake up the thread if it's sleeping
        if (heartbeat_thread_.joinable()) {
            heartbeat_thread_.join(); // 3. Wait for it to finish
        }
    }


    // Internal Replication Service Implementation (Server-to-Server)
    // -----------------------------------------------------

    Status Heartbeat(ServerContext* context, const HeartbeatRequest* request, HeartbeatResponse* response) override {
        // Fencing logic: If we are the primary, but see another one, step down.
        if (server_role_ == StatusResponse::PRIMARY) {
            if (request->term() >= current_term_) {
                server_role_ = StatusResponse::BACKUP; // Stepping down
                std::cout << "[FENCING] " << server_address_ << " received heartbeat from newer Primary (" << request->primary_id() << ", Term: " << request->term() << "). Stepping down." << std::endl;
                response->set_success(true);
                response->set_term(current_term_);
                return Status::OK;
            }
        }

        // Standard Backup response logic
        if (server_role_ == StatusResponse::BACKUP || server_role_ == StatusResponse::CANDIDATE) {
            last_heartbeat_time_ = std::chrono::high_resolution_clock::now(); // Reset timer
            current_term_ = std::max(current_term_.load(), request->term()); // Adopt new term
            server_role_ = StatusResponse::BACKUP; // Always adopt Backup state on successful HB
            
            // Log heartbeat reception (but not too frequently to avoid spam)
            static int heartbeat_log_counter = 0;
            if(++heartbeat_log_counter % 20 == 0){
                std::cout << "[BACKUP] " << server_address_ << " received heartbeat from PRIMARY " << request->primary_id() << " (Term: " << request->term() << ")" << std::endl;
            }
            
            response->set_success(true);
            response->set_term(current_term_);
            return Status::OK;
        }

        response->set_success(false);
        response->set_term(current_term_);
        return Status::OK;
    }
    

    Status ReplicateWrite(ServerContext* context, const ReplicateWriteRequest* request, ReplicateWriteResponse* response) override {
        // [FIX] Renamed 'lock' to 'state_lock' to avoid shadowing
        std::lock_guard<std::mutex> state_lock(state_mutex_);

        if (request->term() < current_term_) {
            response->set_success(false);
            response->set_term(current_term_);
            return Status(grpc::StatusCode::ABORTED, "Stale Primary term.");
        }
       
        // Adopt Primary's term and reset heartbeat timer
        current_term_ = request->term();
        server_role_ = StatusResponse::BACKUP;
        last_heartbeat_time_ = std::chrono::high_resolution_clock::now();

        if (server_role_ != StatusResponse::BACKUP) {
            response->set_success(false);
            return Status(grpc::StatusCode::UNAVAILABLE, "Not a backup, cannot replicate.");
        }
        
        // Simplified replication logic: perform the write locally
        // [FIX] Renamed 'lock' to 'file_lock'
        std::lock_guard<std::mutex> file_lock(file_mutex_);
        
        // [FIX] Corrected inverted directory logic. 0=input, 1=output.
        std::string full_path = (request->path_select() == 0 ? SERVER_INPUT_FILES_ROOT : SERVER_OUTPUT_FILES_ROOT) + request->filename();
        
        try {
            std::ofstream outfile(full_path, std::ios::binary);
            if (!outfile.is_open()) {
                std::cerr << "Replication failed: Could not open file at " << full_path << std::endl;
                response->set_success(false);
                return Status(grpc::StatusCode::INTERNAL, "Failed to open file for replication.");
            }
            // Write bytes content directly
            outfile.write(request->content().data(), request->content().size());
            outfile.close();
            
            // Update timestamp (important for client cache consistency)
            std::filesystem::last_write_time(full_path, std::filesystem::file_time_type::clock::now());
            
            // The write was successful, NOW update the commit index
            // This is safe because we still hold the state_lock
            last_commit_index_ = request->commit_index();
            std::cout << "[Backup] Replicated write for " << request->filename() << ". New index: " << last_commit_index_ << std::endl;

            response->set_success(true);
            response->set_term(current_term_);
            return Status::OK;
            
        } catch (const std::exception& e) {
            std::cerr << "Replication exception: " << e.what() << std::endl;
            response->set_success(false);
            response->set_term(current_term_);
            return Status(grpc::StatusCode::INTERNAL, "Exception during replication.");
        }
    }

    
    Status RequestVote(ServerContext* context, const RequestVoteRequest* request, RequestVoteResponse* response) override {
        std::lock_guard<std::mutex> lock(state_mutex_);

        // Default vote response values
        response->set_term(current_term_);
        response->set_vote_granted(false);

        // Candidate has a stale term
        if (request->term() < current_term_) {
            return Status::OK;
        }

        // Candidate has a higher term
        if(request->term() > current_term_) {
            current_term_ = request->term();
            server_role_ = StatusResponse::BACKUP;
            voted_for_ = ""; // Clear vote on term change
            response->set_term(current_term_);
        }
        
        // If we haven't voted yet in this term, or voted for this candidate, grant vote
        if (voted_for_.empty() || voted_for_ == request->candidate_id()) {
           
           // Only vote if candidate's log is at least as up-to-date as local log
            if(request->last_commit_index() >= last_commit_index_) {
                
                voted_for_ = request->candidate_id();
                response->set_vote_granted(true);
            }
        }
        
        return Status::OK;
    }


    // operators Service Implementation (Client-to-Server)
    // -----------------------------------------------------

    // Client status discovery
    Status GetStatus(ServerContext* context, const StatusRequest* request, StatusResponse* response) override {
        response->set_role(server_role_);
        response->set_term(current_term_);
        return Status::OK;
    }


    // When client connects, it request directory for input/output paths 
    grpc::Status request_dir(ServerContext* context, const InitialiseRequest* request, InitialiseResponse* response)override{
        if(!IsPrimary(context)){
            return Status(grpc::StatusCode::UNAVAILABLE, "Not the primary server.");
        }
        
        if (request->code_to_initialise() == "I want input/output directory"){
            response->set_input_path(SERVER_INPUT_FILES_ROOT);
            response->set_output_path(SERVER_OUTPUT_FILES_ROOT);
            return grpc::Status::OK;
        }else{
            std::cerr << "There is an error while passing the input/output files directory"<<std::endl;
            return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "You need the correct code to retrieve requested data.");
        }
    }

    grpc::Status open(ServerContext* context, const FileRequest* request, grpc::ServerWriter<FileResponse>* writer)override{
        if (!IsPrimary(context)) {
            return Status(grpc::StatusCode::UNAVAILABLE, "Not the Primary.");
        }

        //get the file name
        std::string filename = request -> filename(); // gRPC generates getter methods

        //construct the full path
        std::string path;
        if (request->path_select() == 0){
            path = SERVER_INPUT_FILES_ROOT + filename;
        }else{
            path = SERVER_OUTPUT_FILES_ROOT + filename;
        }
        // have to read the file in binary mode to avoid line ending translation
        // linux uses /n but windows uses /r/n. Without binary mode the file content will be 
        // automatically changed if opened on different machines
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()){
            std::cerr << "file: " << path << " not found" << std::endl;
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "File not found on the server.");
        }

        const std::size_t chunk_size = 4096;
        char buffer[chunk_size];
        FileResponse fr;
        auto file_time = std::filesystem::last_write_time(path);
        auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
        file_time - std::filesystem::file_time_type::clock::now() + 
        std::chrono::system_clock::now());
        
        int64_t total_bytes = 0;
        while(true){
            file.read(buffer, chunk_size);
            std::streamsize len = file.gcount();
            
            if(len <= 0)break;
            fr.clear_content();
            fr.set_content(buffer, len);
            fr.set_length(static_cast<int32_t>(len));
            fr.set_timestamp(sctp.time_since_epoch().count());
            total_bytes += len;

            if(!writer->Write(fr)){
                std::cerr << "Error: Failed write " << std::endl;
                return grpc::Status(grpc::StatusCode::INTERNAL, "Server failed to create the file.");
            }
        }
        file.close();
        return grpc::Status::OK;
    }


    grpc::Status close(ServerContext* context, grpc::ServerReader<FileRequest>* reader, FileResponse* response) override {
        if (!IsPrimary(context)) {
            return Status(grpc::StatusCode::UNAVAILABLE, "Not the Primary.");
        }
        
        FileRequest request;
        std::string filename;
        std::string path;
        std::string content; // Collect all content for replication
        int32_t path_select = 1; // Default to output path

        std::ofstream outfile;

        while(reader->Read(&request)){
            if(filename.empty() || path.empty()){
                filename = request.filename();
                path_select = request.path_select();
                if (path_select == 0) {
                    path = SERVER_INPUT_FILES_ROOT + filename;
                }  else {
                    path = SERVER_OUTPUT_FILES_ROOT + filename;
                }
                outfile.open(path, std::ios::binary);
                if(!outfile.is_open()){
                    std::cerr << "failed to open file: " << path << std::endl;
                    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
                                        "cant open file to write");
                }
            }
            // Collect content for replication
            content += request.content();
            outfile.write(request.content().data(), request.content().size());
        }

        if(outfile.is_open())outfile.close();
        
        // Replicate to backups
        if (!content.empty()) {
            ReplicateWriteRequest rep_request;
            int64_t new_commit_index = 0;
            
            {
                std::lock_guard<std::mutex> state_lock(state_mutex_);
                last_commit_index_++;
                new_commit_index = last_commit_index_;
                
                rep_request.set_term(current_term_);
                rep_request.set_filename(filename);
                rep_request.set_content(content); // bytes content
                rep_request.set_path_select(path_select);
                rep_request.set_commit_index(new_commit_index);
                rep_request.set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
            }
            
            if (!ReplicateWriteToBackups(rep_request)) {
                // Roll back commit index
                std::lock_guard<std::mutex> state_lock(state_mutex_);
                if (last_commit_index_ == new_commit_index) {
                    last_commit_index_--; // Roll back
                }
                return Status(grpc::StatusCode::INTERNAL, "Write failed to reach Quorum.");
            }
        }
        
        auto file_time = std::filesystem::last_write_time(path);
        auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(file_time - std::filesystem::file_time_type::clock::now() + std::chrono::system_clock::now());
        response->set_timestamp(sctp.time_since_epoch().count());
        return grpc::Status::OK;
    }


    // compares the timestamp of the client and server
    grpc::Status compare(grpc::ServerContext* context, const FileRequest* request, grpc::ServerWriter<FileResponse>* writer) override {
        if (!IsPrimary(context)) {
            return Status(grpc::StatusCode::UNAVAILABLE, "Not the Primary.");
        }

        std::string filename = request->filename();
        int64_t timestamp = request -> timestamp();
        // since only output files can be created -> only output files have timestamps -> we only have to implement the timestamp compare logic for output files

        // Determine path based on path_select
        std::string path;
        if (request->path_select() == 0) {
            path = SERVER_INPUT_FILES_ROOT + filename;
        } else {
            path = SERVER_OUTPUT_FILES_ROOT + filename;
        }

        // Get the timestamp of the file.
        
        try {
            auto file_time = std::filesystem::last_write_time(path); // gets the file's modification time
            auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(file_time - std::filesystem::file_time_type::clock::now() + std::chrono::system_clock::now());
            int64_t timestamp_server = sctp.time_since_epoch().count(); //Unix timestamp
            // compare
            if (timestamp_server > timestamp){
                // this means the client version was an old version
                // then we get the content of the file
                std::ifstream new_content(path,std::ios::binary);
                
            
                // sstream can help reading the entire contents of the file
                const std::size_t chunk_size = 4096;
                char buffer[chunk_size];
                while(true){
                    new_content.read(buffer, chunk_size);
                    std::streamsize len = new_content.gcount();
                    int64_t timestamp_server = sctp.time_since_epoch().count(); 
                    if(len <= 0)break;
                    FileResponse response;
                    response.set_content(buffer, len);
                    response.set_timestamp(timestamp_server);
                    response.set_update_bit(1);
                    if (!writer->Write(response)) {
                        std::cerr << "Failed to write chunk " << " to server" << std::endl;
                        return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Failed to write chunk");
                    }
                }
                std::cout << "Cache for '" << filename << "' is stale. Sent update." << std::endl;
                return grpc::Status::OK;

            }else{
                FileResponse response;

                response.set_update_bit(0);
                response.set_timestamp(timestamp_server); // for consistency, still good to send back
                writer->Write(response);

                std::cout << "Cache for '" << filename << "' is valid." << std::endl;
                return grpc::Status::OK;
            }
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Warning: Could not get timestamp for the file: " << e.what() << std::endl;
            return grpc::Status(grpc::StatusCode::INTERNAL, "Server failed to get the timestamp of the file on the server.");
        }

    }


    void RunServer(const std::vector<std::string>& all_addresses) {
        std::string server_address = server_address_;

        InitializePeers(all_addresses);

        grpc::ServerBuilder builder;
        
        // Set max message size for server-to-server replication (100MB to handle large files - temporary fix)
        builder.SetMaxReceiveMessageSize(100 * 1024 * 1024);
        builder.SetMaxSendMessageSize(100 * 1024 * 1024);
        
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(static_cast<operators::Service*>(this));
        builder.RegisterService(static_cast<InternalReplication::Service*>(this)); // Register internal service
        
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        if (!server) {
            std::cerr << "Failed to start server on " << server_address_ << std::endl;
            return;
        }
        
        std::cout << "Server (" << server_address_ << ") listening as BACKUP." << std::endl;
        std::cout << "Waiting for other servers to start..." << std::endl;
        
        // Give other servers time to start before election (longer wait for stability)
        std::this_thread::sleep_for(std::chrono::seconds(5));
        
        server->Wait();
    }



};


int main(int argc, char* argv[]) {
    // Initialize random seed for election jitter
    srand(time(nullptr));
    
    if (argc < 3){
        std::cerr << "Usage: " << argv[0] << " <server_port> <peer_addr_1> [<peer_addr_2> ...]" << std::endl;
        return 1;
    }
    
    std::string server_port = argv[1];
    std::string my_address = "0.0.0.0:" + server_port;
    
    std::vector<std::string> all_addresses;
    all_addresses.push_back(my_address);
    for (int i = 2; i < argc; ++i) { 
        all_addresses.push_back(argv[i]);
    }

    FileSystem filesys(my_address, all_addresses);
    filesys.RunServer(all_addresses);
    
    return 0;
}