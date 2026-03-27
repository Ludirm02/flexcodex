#include "flexql.h"

#include "protocol.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <vector>

struct FlexQL {
    int fd;
};

namespace {

void set_errmsg(char** errmsg, const std::string& msg) {
    if (errmsg == nullptr) {
        return;
    }
    *errmsg = nullptr;

    char* mem = static_cast<char*>(std::malloc(msg.size() + 1));
    if (mem == nullptr) {
        return;
    }
    std::memcpy(mem, msg.data(), msg.size());
    mem[msg.size()] = '\0';
    *errmsg = mem;
}

bool read_u16_be(int fd, std::uint16_t& out) {
    std::uint16_t be = 0;
    if (!flexql_proto::recv_exact(fd, reinterpret_cast<char*>(&be), sizeof(be))) {
        return false;
    }
    out = ntohs(be);
    return true;
}

bool read_u32_be(int fd, std::uint32_t& out) {
    std::uint32_t be = 0;
    if (!flexql_proto::recv_exact(fd, reinterpret_cast<char*>(&be), sizeof(be))) {
        return false;
    }
    out = ntohl(be);
    return true;
}

bool read_line_with_prefix(int fd, char prefix, std::string& out) {
    std::string tail;
    if (!flexql_proto::recv_line(fd, tail)) {
        return false;
    }
    out.clear();
    out.reserve(1 + tail.size());
    out.push_back(prefix);
    out += tail;
    return true;
}

std::vector<std::string> parse_prefixed_fields_copy(const std::string& line,
                                                    const std::string& prefix,
                                                    bool& ok) {
    ok = false;
    if (line == prefix) {
        ok = true;
        return {};
    }

    if (line.size() < prefix.size() + 1 || line.rfind(prefix + "\t", 0) != 0) {
        return {};
    }

    std::string payload = line.substr(prefix.size() + 1);
    auto fields = flexql_proto::split_tab_escaped(payload, 0);
    ok = true;
    return fields;
}

bool parse_row_fields_inplace(std::string& line, int expected_columns, std::vector<char*>& out_ptrs) {
    out_ptrs.clear();
    if (line == "ROW") {
        return expected_columns == 0;
    }
    if (line.rfind("ROW\t", 0) != 0) {
        return false;
    }

    char* read = line.data() + 4;
    char* write = read;
    char* field_start = write;

    while (*read != '\0') {
        const char c = *read++;
        if (c == '\\') {
            const char esc = *read;
            if (esc == '\0') {
                *write++ = '\\';
                break;
            }
            ++read;
            if (esc == 't') {
                *write++ = '\t';
            } else if (esc == 'n') {
                *write++ = '\n';
            } else if (esc == '\\') {
                *write++ = '\\';
            } else {
                *write++ = esc;
            }
            continue;
        }

        if (c == '\t') {
            *write++ = '\0';
            out_ptrs.push_back(field_start);
            field_start = write;
            continue;
        }

        *write++ = c;
    }

    *write = '\0';
    out_ptrs.push_back(field_start);
    return static_cast<int>(out_ptrs.size()) == expected_columns;
}

int handle_text_response_with_first(FlexQL* db,
                                    char first,
                                    flexql_callback callback,
                                    void* arg,
                                    char** errmsg) {
    int columns = 0;
    std::string first_line;
    if (!read_line_with_prefix(db->fd, first, first_line)) {
        set_errmsg(errmsg, "failed to read server response");
        return FLEXQL_PROTOCOL_ERROR;
    }
    if (first_line.rfind("ERR\t", 0) == 0) {
        set_errmsg(errmsg, flexql_proto::unescape_field(first_line.substr(4)));
        return FLEXQL_SQL_ERROR;
    }
    if (first_line.rfind("OK ", 0) != 0) {
        set_errmsg(errmsg, "protocol error: expected OK header");
        return FLEXQL_PROTOCOL_ERROR;
    }
    try {
        std::size_t consumed = 0;
        columns = std::stoi(first_line.substr(3), &consumed);
        if (consumed != first_line.substr(3).size() || columns < 0) {
            set_errmsg(errmsg, "protocol error: invalid column count");
            return FLEXQL_PROTOCOL_ERROR;
        }
    } catch (const std::exception&) {
        set_errmsg(errmsg, "protocol error: invalid column count");
        return FLEXQL_PROTOCOL_ERROR;
    }

    std::vector<std::string> col_names;
    std::vector<char*> name_ptrs;
    std::vector<char*> value_ptrs;
    if (columns > 0) {
        std::string cols_line;
        if (!flexql_proto::recv_line(db->fd, cols_line)) {
            set_errmsg(errmsg, "failed to read result columns");
            return FLEXQL_PROTOCOL_ERROR;
        }

        bool ok = false;
        col_names = parse_prefixed_fields_copy(cols_line, "COL", ok);
        if (!ok || static_cast<int>(col_names.size()) != columns) {
            set_errmsg(errmsg, "protocol error: malformed column line");
            return FLEXQL_PROTOCOL_ERROR;
        }

        name_ptrs.resize(col_names.size());
        value_ptrs.resize(col_names.size());
        for (std::size_t i = 0; i < col_names.size(); ++i) {
            name_ptrs[i] = col_names[i].data();
        }
    }

    bool abort_requested = false;
    for (;;) {
        std::string line;
        if (!flexql_proto::recv_line(db->fd, line)) {
            set_errmsg(errmsg, "failed to read query result");
            return FLEXQL_PROTOCOL_ERROR;
        }

        if (line == "END") {
            break;
        }

        if (!parse_row_fields_inplace(line, columns, value_ptrs)) {
            set_errmsg(errmsg, "protocol error: malformed row line");
            return FLEXQL_PROTOCOL_ERROR;
        }

        if (callback != nullptr && !abort_requested) {
            int cb_rc = callback(arg, columns, value_ptrs.data(), name_ptrs.data());
            if (cb_rc == 1) {
                abort_requested = true;
            }
        }
    }

    return FLEXQL_OK;
}

int handle_binary_response(int fd, flexql_callback callback, void* arg, char** errmsg) {
    std::uint32_t columns = 0;
    std::uint32_t rows = 0;
    if (!read_u32_be(fd, columns) || !read_u32_be(fd, rows)) {
        set_errmsg(errmsg, "protocol error: truncated binary header");
        return FLEXQL_PROTOCOL_ERROR;
    }

    std::vector<std::string> col_names;
    col_names.resize(columns);
    std::vector<char*> name_ptrs(columns, nullptr);
    for (std::uint32_t i = 0; i < columns; ++i) {
        std::uint16_t len = 0;
        if (!read_u16_be(fd, len)) {
            set_errmsg(errmsg, "protocol error: truncated column metadata");
            return FLEXQL_PROTOCOL_ERROR;
        }
        if (len > 0) {
            col_names[i].resize(len);
            if (!flexql_proto::recv_exact(fd, col_names[i].data(), len)) {
                set_errmsg(errmsg, "protocol error: truncated column name");
                return FLEXQL_PROTOCOL_ERROR;
            }
        } else {
            col_names[i].clear();
        }
        name_ptrs[i] = col_names[i].data();
    }

    std::vector<std::string> row_values(columns);
    std::vector<char*> value_ptrs(columns, nullptr);
    bool abort_requested = false;
    for (std::uint32_t r = 0; r < rows; ++r) {
        for (std::uint32_t c = 0; c < columns; ++c) {
            std::uint32_t len = 0;
            if (!read_u32_be(fd, len)) {
                set_errmsg(errmsg, "protocol error: truncated row field length");
                return FLEXQL_PROTOCOL_ERROR;
            }
            if (len > 0) {
                row_values[c].resize(len);
                if (!flexql_proto::recv_exact(fd, row_values[c].data(), len)) {
                    set_errmsg(errmsg, "protocol error: truncated row field");
                    return FLEXQL_PROTOCOL_ERROR;
                }
            } else {
                row_values[c].clear();
            }
            value_ptrs[c] = row_values[c].data();
        }

        if (callback != nullptr && !abort_requested) {
            int cb_rc = callback(arg, static_cast<int>(columns), value_ptrs.data(), name_ptrs.data());
            if (cb_rc == 1) {
                abort_requested = true;
            }
        }
    }

    return FLEXQL_OK;
}

}  // namespace

extern "C" int flexql_open(const char* host, int port, FlexQL** db) {
    if (host == nullptr || db == nullptr || port <= 0 || port > 65535) {
        return FLEXQL_ERROR;
    }

    *db = nullptr;

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* result = nullptr;
    std::string port_str = std::to_string(port);
    int rc = ::getaddrinfo(host, port_str.c_str(), &hints, &result);
    if (rc != 0) {
        return FLEXQL_NETWORK_ERROR;
    }

    int fd = -1;
    for (addrinfo* p = result; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
            break;
        }
        ::close(fd);
        fd = -1;
    }

    ::freeaddrinfo(result);

    if (fd < 0) {
        return FLEXQL_NETWORK_ERROR;
    }

    int one = 1;
    (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    FlexQL* handle = new (std::nothrow) FlexQL;
    if (handle == nullptr) {
        ::close(fd);
        return FLEXQL_NOMEM;
    }

    handle->fd = fd;
    *db = handle;
    return FLEXQL_OK;
}

extern "C" int flexql_close(FlexQL* db) {
    if (db == nullptr) {
        return FLEXQL_ERROR;
    }

    if (db->fd >= 0) {
        flexql_proto::clear_reader_state(db->fd);
        ::close(db->fd);
        db->fd = -1;
    }
    delete db;
    return FLEXQL_OK;
}

extern "C" int flexql_exec(FlexQL* db,
                           const char* sql,
                           flexql_callback callback,
                           void* arg,
                           char** errmsg) {
    if (errmsg != nullptr) {
        *errmsg = nullptr;
    }

    if (db == nullptr || sql == nullptr) {
        set_errmsg(errmsg, "invalid database handle or SQL");
        return FLEXQL_ERROR;
    }

    std::string sql_text(sql);
    if (!flexql_proto::send_query(db->fd, sql_text, true)) {
        set_errmsg(errmsg, "failed to send query to server");
        return FLEXQL_NETWORK_ERROR;
    }

    char frame_type = '\0';
    if (!flexql_proto::recv_exact(db->fd, &frame_type, 1)) {
        set_errmsg(errmsg, "failed to read server response");
        return FLEXQL_PROTOCOL_ERROR;
    }

    if (static_cast<unsigned char>(frame_type) == 0x01U) {
        return handle_binary_response(db->fd, callback, arg, errmsg);
    }
    if (static_cast<unsigned char>(frame_type) == 0x02U) {
        std::uint32_t err_len = 0;
        if (!read_u32_be(db->fd, err_len)) {
            set_errmsg(errmsg, "protocol error: truncated binary error");
            return FLEXQL_PROTOCOL_ERROR;
        }
        std::string msg;
        msg.resize(err_len);
        if (err_len > 0 && !flexql_proto::recv_exact(db->fd, msg.data(), err_len)) {
            set_errmsg(errmsg, "protocol error: truncated binary error");
            return FLEXQL_PROTOCOL_ERROR;
        }
        set_errmsg(errmsg, msg);
        return FLEXQL_SQL_ERROR;
    }

    return handle_text_response_with_first(db, frame_type, callback, arg, errmsg);
}

extern "C" void flexql_free(void* ptr) {
    std::free(ptr);
}
