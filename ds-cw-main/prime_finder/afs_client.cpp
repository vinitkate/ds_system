#include "afs_client.h"
#include <iostream>

AFSClient::AFSClient(const std::vector<std::string>& server_addresses)
    : server_addresses_(server_addresses){
    // stub_ is implicitly nullptr, discover primary when needed
}

void AFSClient::InvalidateStub(){
    std::lock_guard<std::mutex> lock(stub_mutex_);
    stub_.reset();
}

afs_operation::operators::Stub* AFSClient::WaitAndGetStub(){
    std::lock_guard<std::mutex> lock(stub_mutex_);

    if(stub_){
        return stub_.get();
    }

    int attempt = 0;
    const int MAX_RECOVERY_SECONDS = 30;
    auto start_time = std::chrono::steady_clock::now();

    while(std::chrono::steady_clock::now() - start_time < std::chrono::seconds(MAX_RECOVERY_SECONDS)){
        for(const auto& address: server_addresses_){
            auto channel =  grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
            
            auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
            if(!channel->WaitForConnected(deadline)){
                continue;
            }
            
            auto temp_stub = afs_operation::operators::NewStub(channel);

            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));

            afs_operation::StatusRequest req;
            afs_operation::StatusResponse res;
            grpc::Status status = temp_stub -> GetStatus(&context, req, &res);

            if(!status.ok() || res.role() != afs_operation::StatusResponse::PRIMARY){
                continue;
            }

            // Found Primary
            grpc::ChannelArguments channel_args;
            channel_args.SetMaxReceiveMessageSize(4 * 1024 * 1024);
            channel_args.SetMaxSendMessageSize(4 * 1024 * 1024);

            auto final_channel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), channel_args);
            stub_ = afs_operation::operators::NewStub(final_channel);
            
            // fetch dir info
            afs_operation::InitialiseRequest init_request;
            init_request.set_code_to_initialise("I want input/output directory");
            afs_operation::InitialiseResponse init_response;
            grpc::ClientContext init_context;
            init_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
            
            grpc::Status init_status = stub_->request_dir(&init_context, init_request, &init_response);
            if(init_status.ok()){
                input_path_ = init_response.input_path();
                output_path_ = init_response.output_path();
                return stub_.get();
            }else{
                InvalidateStub();
                break;
            }
        }

        // no primary found, wait with backoff
        if(attempt == 0){
            std::cerr << "[AFSClient] No Primary found. Waiting for election/recovery..." << std::endl;
        }
        int sleep_time_sec = std::min(5, 1 << attempt);
        
        std::this_thread::sleep_for(std::chrono::seconds(sleep_time_sec));
        attempt++;
    }

    std::cerr << "[AFSClient] FATAL: Primary recovery failed after " << MAX_RECOVERY_SECONDS << " seconds." << std::endl;
    return nullptr;
}

bool AFSClient::read_file(const std::string& filename, std::string& content, int path_select){
    for(int retry = 0; retry < 3; ++retry){
        afs_operation::operators::Stub* stub = WaitAndGetStub();
        if(!stub) return false;
        
        // use open() RPC to fetch file (streaming)
        afs_operation::FileRequest request;
        request.set_filename(filename);
        request.set_path_select(path_select);
        
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        
        std::unique_ptr<grpc::ClientReader<afs_operation::FileResponse>> reader(
            stub->open(&context, request)
        );
        
        content.clear();
        afs_operation::FileResponse response_chunk;
        int total_bytes = 0;
        
        while(reader->Read(&response_chunk)){
            if(response_chunk.length() > 0){
                content += response_chunk.content();
                total_bytes += response_chunk.length();
            }
        }
        
        grpc::Status status = reader->Finish();
        
        if(status.ok()){
            return true;
        }else if(status.error_code() == grpc::StatusCode::UNAVAILABLE || 
                   status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED){
            InvalidateStub();
            continue;
        }else{
            std::cerr << "[AFSClient] Failed to open '" << filename 
                      << "': " << status.error_message() << std::endl;
            return false;
        }
    }
    return false;
}

bool AFSClient::write_file(const std::string& filename, const std::string& content, int path_select) 
{
    for(int retry = 0; retry < 3; ++retry){
        afs_operation::operators::Stub* stub = WaitAndGetStub();
        if(!stub) return false;
        
        // use close() RPC to write file (streaming)
        afs_operation::FileResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        
        std::unique_ptr<grpc::ClientWriter<afs_operation::FileRequest>> writer(
            stub->close(&context, &response)
        );
        
        const std::size_t chunk_size = 4096;
        size_t pos = 0;
        
        while(pos < content.length()){
            size_t chunk_len = std::min(chunk_size, content.length() - pos);
            
            afs_operation::FileRequest request;
            request.set_filename(filename);
            request.set_path_select(path_select);
            request.set_content(content.substr(pos, chunk_len));
            
            if(!writer->Write(request)){
                std::cerr << "[AFSClient] Failed to write chunk to server" << std::endl;
                writer->WritesDone();
                writer->Finish();
                break;
            }
            
            pos += chunk_len;
        }
        
        writer->WritesDone();
        grpc::Status status = writer->Finish();
        
        if(status.ok()){
            return true;
        }else if(status.error_code() == grpc::StatusCode::UNAVAILABLE || 
                   status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED){
            InvalidateStub();
            continue;
        }else{
            std::cerr << "[AFSClient] Failed to write '" << filename 
                      << "': " << status.error_message() << std::endl;
            return false;
        }
    }
    return false;
}

bool AFSClient::append_file(const std::string& filename, const std::string& content, int path_select){
    // read entire file, append locally, write back
    std::string existing_content;
    
    if(!read_file(filename, existing_content, path_select)){
        existing_content.clear();
    }
    
    std::string new_content = existing_content + content;
    
    return write_file(filename, new_content, path_select);
}