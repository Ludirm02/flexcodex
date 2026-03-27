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
#include <cstdint>
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

inline void append_u16_be(std::string& out, std::uint16_t v) {
    const std::uint16_t be = htons(v);
    out.append(reinterpret_cast<const char*>(&be), sizeof(be));
}

inline void append_u32_be(std::string& out, std::uint32_t v) {
    const std::uint32_t be = htonl(v);
    out.append(reinterpret_cast<const char*>(&be), sizeof(be));
}

bool send_error_binary(int fd, const std::string& message) {
    std::string wire;
    wire.reserve(1 + 4 + message.size());
    wire.push_back(static_cast<char>(0x02));  // ERROR frame
    append_u32_be(wire, static_cast<std::uint32_t>(message.size()));
    wire.append(message);
    return flexql_proto::send_all(fd, wire.data(), wire.size());
}

bool send_result_binary(int fd, const QueryResult& result) {
    std::string wire;
    wire.reserve(1 + 4 + 4 + result.columns.size() * 8 + result.rows.size() * 16);
    wire.push_back(static_cast<char>(0x01));  // OK frame
    append_u32_be(wire, static_cast<std::uint32_t>(result.columns.size()));
    append_u32_be(wire, static_cast<std::uint32_t>(result.rows.size()));

    for (const std::string& c : result.columns) {
        const std::size_t n = std::min<std::size_t>(c.size(), 0xFFFF);
        append_u16_be(wire, static_cast<std::uint16_t>(n));
        wire.append(c.data(), n);
    }
    for (const auto& row : result.rows) {
        for (const std::string& v : row) {
            append_u32_be(wire, static_cast<std::uint32_t>(v.size()));
            wire.append(v);
        }
    }

    return flexql_proto::send_all(fd, wire.data(), wire.size());
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

        bool want_binary = false;
        const char* num_start = nullptr;
        if (header.rfind("Q ", 0) == 0) {
            num_start = header.c_str() + 2;
        } else if (header.rfind("QB ", 0) == 0) {
            want_binary = true;
            num_start = header.c_str() + 3;
        } else {
            if (!send_error(client_fd, "protocol error: expected query header")) {
                break;
            }
            continue;
        }

        std::size_t len = 0;
        {
            char* num_end = nullptr;
            errno = 0;
            unsigned long long parsed = std::strtoull(num_start, &num_end, 10);
            if (errno != 0 || num_end == num_start || *num_end != '\0') {
                const bool ok = want_binary
                                    ? send_error_binary(client_fd, "protocol error: invalid query length")
                                    : send_error(client_fd, "protocol error: invalid query length");
                if (!ok) {
                    break;
                }
                continue;
            }
            len = static_cast<std::size_t>(parsed);
        }
        if (len > (1ULL << 31)) {
            const bool ok = want_binary
                                ? send_error_binary(client_fd, "protocol error: invalid query length")
                                : send_error(client_fd, "protocol error: invalid query length");
            if (!ok) {
                break;
            }
            continue;
        }

        std::string sql(len, '\0');
        if (!flexql_proto::recv_exact(client_fd, sql.data(), len)) {
            break;
        }

        QueryResult result;
        std::string cached_wire;
        std::string error;
        if (!engine->execute(sql, result, error, &cached_wire, want_binary)) {
            const bool ok = want_binary ? send_error_binary(client_fd, error) : send_error(client_fd, error);
            if (!ok) {
                break;
            }
            continue;
        }
        if (!cached_wire.empty()) {
            if (!flexql_proto::send_all(client_fd, cached_wire.data(), cached_wire.size())) {
                break;
            }
            continue;
        }
        if (want_binary) {
            if (!send_result_binary(client_fd, result)) {
                break;
            }
            continue;
        }

        // Stream result directly — no intermediate copy
        constexpr std::size_t kChunk = 256 * 1024;
        std::string buf;
        buf.reserve(kChunk + 4096);

        buf += "OK ";
        buf += std::to_string(result.columns.size());
        buf += "\n";

        if (result.columns.empty()) {
            buf += "END\n";
            if (!flexql_proto::send_all(client_fd, buf.data(), buf.size())) break;
            continue;
        }

        append_line_with_fields(buf, "COL", result.columns);

        bool send_ok = true;
        for (const auto& row : result.rows) {
            append_line_with_fields(buf, "ROW", row);
            if (buf.size() >= kChunk) {
                if (!flexql_proto::send_all(client_fd, buf.data(), buf.size())) {
                    send_ok = false;
                    break;
                }
                buf.clear();
            }
        }

        if (!send_ok) break;

        buf += "END\n";
        if (!flexql_proto::send_all(client_fd, buf.data(), buf.size())) break;
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
    int one2 = 1;
    (void)::setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &one2, sizeof(one2));

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
    const std::size_t hw = std::thread::hardware_concurrency();
    ThreadPool pool(hw > 0 ? hw * 2 : 16);
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
        int rcvbuf = 4 << 20;
        (void)::setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
        int sndbuf = 4 << 20;
        (void)::setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
        pool.submit([client_fd, &engine]() {
            handle_client(client_fd, &engine);
        });
    }

    ::close(server_fd);
    return 0;
}
