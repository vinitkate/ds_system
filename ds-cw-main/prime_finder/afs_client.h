#ifndef AFS_CLIENT_H
#define AFS_CLIENT_H

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "afs_operation.grpc.pb.h"
#include "afs_operation.pb.h"

// AFS Client for prime_finder
class AFSClient{
private:
    std::vector<std::string> server_addresses_;
    std::mutex stub_mutex_;
    std::unique_ptr<afs_operation::operators::Stub> stub_;
    std::string input_path_;
    std::string output_path_;
    
    afs_operation::operators::Stub* WaitAndGetStub();
    void InvalidateStub();
    
public:
    AFSClient(const std::vector<std::string>& server_addresses);
    
    // path_select: 0=input, 1=output
    bool read_file(const std::string& filename, std::string& content, int path_select = 0);
    
    bool write_file(const std::string& filename, const std::string& content, int path_select = 1);
    
    bool append_file(const std::string& filename, const std::string& content, int path_select = 1);
    
    const std::string& get_input_path() const{ return input_path_; }
    const std::string& get_output_path() const{ return output_path_; }
};

#endif