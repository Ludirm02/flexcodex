#ifndef FLEXQL_PROTOCOL_HPP
#define FLEXQL_PROTOCOL_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace flexql_proto {

bool send_all(int fd, const char* data, std::size_t len);
bool recv_exact(int fd, char* data, std::size_t len);
bool recv_line(int fd, std::string& out);
void clear_reader_state(int fd);

std::string escape_field(const std::string& in);
std::string unescape_field(const std::string& in);

std::string join_tab_escaped(const std::vector<std::string>& fields, std::size_t start_index = 0);
std::vector<std::string> split_tab_escaped(const std::string& line, std::size_t start_index = 0);

bool send_query(int fd, const std::string& sql, bool want_binary = false);

}  // namespace flexql_proto

#endif
