#include "protocol.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <unordered_map>
#include <vector>

namespace flexql_proto {

namespace {

struct ReaderState {
    std::vector<char> buffer;
    std::size_t start = 0;
    std::size_t end = 0;

    ReaderState() : buffer(65536) {}
};

thread_local std::unordered_map<int, ReaderState> g_reader_states;

bool fill_reader_state(int fd, ReaderState& state) {
    if (state.start > 0 && state.start < state.end) {
        std::memmove(state.buffer.data(), state.buffer.data() + state.start, state.end - state.start);
        state.end -= state.start;
        state.start = 0;
    } else if (state.start >= state.end) {
        state.start = 0;
        state.end = 0;
    }

    if (state.end == state.buffer.size()) {
        state.buffer.resize(state.buffer.size() * 2);
    }

    for (;;) {
        ssize_t n = ::recv(fd, state.buffer.data() + state.end, state.buffer.size() - state.end, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        state.end += static_cast<std::size_t>(n);
        return true;
    }
}

}  // namespace

bool send_all(int fd, const char* data, std::size_t len) {
    std::size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

bool recv_exact(int fd, char* data, std::size_t len) {
    ReaderState& state = g_reader_states[fd];
    std::size_t got = 0;
    while (got < len) {
        if (state.start == state.end) {
            if (!fill_reader_state(fd, state)) {
                return false;
            }
        }

        const std::size_t available = state.end - state.start;
        const std::size_t take = std::min(available, len - got);
        std::memcpy(data + got, state.buffer.data() + state.start, take);
        state.start += take;
        got += take;

        if (state.start == state.end) {
            state.start = 0;
            state.end = 0;
        }
    }
    return true;
}

bool recv_line(int fd, std::string& out) {
    ReaderState& state = g_reader_states[fd];
    out.clear();

    for (;;) {
        if (state.start < state.end) {
            const char* begin = state.buffer.data() + state.start;
            const std::size_t available = state.end - state.start;
            const void* nl = std::memchr(begin, '\n', available);
            if (nl != nullptr) {
                const char* nl_ptr = static_cast<const char*>(nl);
                out.append(begin, static_cast<std::size_t>(nl_ptr - begin));
                state.start += static_cast<std::size_t>(nl_ptr - begin) + 1;
                if (state.start == state.end) {
                    state.start = 0;
                    state.end = 0;
                }
                return true;
            }

            out.append(begin, available);
            state.start = 0;
            state.end = 0;
        }

        if (!fill_reader_state(fd, state)) {
            return false;
        }
    }
}

void clear_reader_state(int fd) {
    g_reader_states.erase(fd);
}

std::string escape_field(const std::string& in) {
    std::string out;
    out.reserve(in.size());
    for (char c : in) {
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
    return out;
}

std::string unescape_field(const std::string& in) {
    std::string out;
    out.reserve(in.size());
    for (std::size_t i = 0; i < in.size(); ++i) {
        if (in[i] == '\\' && i + 1 < in.size()) {
            char n = in[i + 1];
            if (n == '\\') {
                out.push_back('\\');
                ++i;
            } else if (n == 't') {
                out.push_back('\t');
                ++i;
            } else if (n == 'n') {
                out.push_back('\n');
                ++i;
            } else {
                out.push_back(in[i]);
            }
        } else {
            out.push_back(in[i]);
        }
    }
    return out;
}

std::string join_tab_escaped(const std::vector<std::string>& fields, std::size_t start_index) {
    std::string out;
    for (std::size_t i = start_index; i < fields.size(); ++i) {
        if (i > start_index) {
            out.push_back('\t');
        }
        out += escape_field(fields[i]);
    }
    return out;
}

std::vector<std::string> split_tab_escaped(const std::string& line, std::size_t start_index) {
    std::vector<std::string> out;
    std::string current;
    bool escaping = false;
    std::size_t field_index = 0;

    for (char c : line) {
        if (escaping) {
            if (c == 't') {
                current.push_back('\t');
            } else if (c == 'n') {
                current.push_back('\n');
            } else {
                current.push_back(c);
            }
            escaping = false;
            continue;
        }
        if (c == '\\') {
            escaping = true;
            continue;
        }
        if (c == '\t') {
            if (field_index >= start_index) {
                out.push_back(current);
            }
            current.clear();
            ++field_index;
        } else {
            current.push_back(c);
        }
    }

    if (field_index >= start_index) {
        out.push_back(current);
    }

    return out;
}

bool send_query(int fd, const std::string& sql, bool want_binary) {
    const std::string header = std::string(want_binary ? "QB " : "Q ") + std::to_string(sql.size()) + "\n";
    if (!send_all(fd, header.data(), header.size())) {
        return false;
    }
    if (sql.empty()) {
        return true;
    }
    return send_all(fd, sql.data(), sql.size());
}

}  // namespace flexql_proto
