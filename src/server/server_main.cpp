#include "protocol.hpp"
#include "sql_engine.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace {

using flexql::QueryResult;
using flexql::SqlEngine;

void append_escaped_field(std::string& out, const std::string& value) {
    bool needs_escape = false;
    for (char c : value) {
        if (c == '\\' || c == '\t' || c == '\n') {
            needs_escape = true;
            break;
        }
    }

    if (!needs_escape) {
        out += value;
        return;
    }

    for (char c : value) {
        if (c == '\\') {
            out += "\\\\";
        } else if (c == '\t') {
            out += "\\t";
        } else if (c == '\n') {
            out += "\\n";
        } else {
            out.push_back(c);
        }
    }
}

void append_line_with_fields(std::string& out,
                             const char* prefix,
                             const std::vector<std::string>& fields) {
    out += prefix;
    for (const std::string& f : fields) {
        out.push_back('\t');
        append_escaped_field(out, f);
    }
    out.push_back('\n');
}

bool send_error(int fd, const std::string& message) {
    std::string escaped = flexql_proto::escape_field(message);
    std::string line = "ERR\t" + escaped + "\n";
    return flexql_proto::send_all(fd, line.data(), line.size());
}

bool send_result(int fd, const QueryResult& result) {
    constexpr std::size_t kChunkFlush = 64 * 1024;
    std::string chunk;
    chunk.reserve(kChunkFlush + 4096);

    auto flush = [&]() -> bool {
        if (chunk.empty()) {
            return true;
        }
        bool ok = flexql_proto::send_all(fd, chunk.data(), chunk.size());
        chunk.clear();
        return ok;
    };

    chunk += "OK " + std::to_string(result.columns.size()) + "\n";
    if (result.columns.empty()) {
        chunk += "END\n";
        return flush();
    }

    append_line_with_fields(chunk, "COL", result.columns);
    for (const auto& row : result.rows) {
        append_line_with_fields(chunk, "ROW", row);
        if (chunk.size() >= kChunkFlush) {
            if (!flush()) {
                return false;
            }
        }
    }

    chunk += "END\n";
    return flush();
}

void handle_client(int client_fd, SqlEngine* engine) {
    for (;;) {
        std::string header;
        if (!flexql_proto::recv_line(client_fd, header)) {
            break;
        }

        if (header.empty()) {
            continue;
        }

        if (header.rfind("Q ", 0) != 0) {
            if (!send_error(client_fd, "protocol error: expected query header")) {
                break;
            }
            continue;
        }

        std::size_t len = 0;
        try {
            std::size_t consumed = 0;
            len = static_cast<std::size_t>(std::stoull(header.substr(2), &consumed));
            if (consumed != header.substr(2).size()) {
                if (!send_error(client_fd, "protocol error: invalid query length")) {
                    break;
                }
                continue;
            }
        } catch (const std::exception&) {
            if (!send_error(client_fd, "protocol error: invalid query length")) {
                break;
            }
            continue;
        }

        std::string sql(len, '\0');
        if (!flexql_proto::recv_exact(client_fd, sql.data(), len)) {
            break;
        }

        QueryResult result;
        std::string error;
        if (!engine->execute(sql, result, error)) {
            if (!send_error(client_fd, error)) {
                break;
            }
            continue;
        }

        if (!send_result(client_fd, result)) {
            break;
        }
    }

    flexql_proto::clear_reader_state(client_fd);
    ::close(client_fd);
}

class ThreadPool {
public:
    explicit ThreadPool(std::size_t threads) {
        if (threads == 0) {
            threads = 4;
        }
        workers_.reserve(threads);
        for (std::size_t i = 0; i < threads; ++i) {
            workers_.emplace_back([this]() {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(mutex_);
                        cv_.wait(lock, [this]() {
                            return stopping_ || !tasks_.empty();
                        });
                        if (stopping_ && tasks_.empty()) {
                            return;
                        }
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            });
        }
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        cv_.notify_all();
        for (std::thread& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    void submit(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                return;
            }
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
    }

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_ = false;
};

}  // namespace

int main(int argc, char** argv) {
    std::signal(SIGPIPE, SIG_IGN);

    int port = 9000;
    if (argc >= 2) {
        port = std::atoi(argv[1]);
        if (port <= 0 || port > 65535) {
            std::cerr << "invalid port: " << argv[1] << "\n";
            return 1;
        }
    }

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::perror("socket");
        return 1;
    }

    int one = 1;
    if (::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
        std::perror("setsockopt");
        ::close(server_fd);
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::perror("bind");
        ::close(server_fd);
        return 1;
    }

    if (::listen(server_fd, 128) != 0) {
        std::perror("listen");
        ::close(server_fd);
        return 1;
    }

    SqlEngine engine(512);
    ThreadPool pool(std::thread::hardware_concurrency());
    std::cout << "FlexQL server listening on port " << port << "\n";

    for (;;) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = ::accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("accept");
            continue;
        }

        int one_tcp = 1;
        (void)::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one_tcp, sizeof(one_tcp));

        pool.submit([client_fd, &engine]() {
            handle_client(client_fd, &engine);
        });
    }

    ::close(server_fd);
    return 0;
}
