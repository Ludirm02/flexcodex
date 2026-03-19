#ifndef FLEXQL_SQL_ENGINE_HPP
#define FLEXQL_SQL_ENGINE_HPP

#include <cstdint>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace flexql {

enum class DataType {
    kInt,
    kDecimal,
    kVarchar,
    kDatetime
};

struct QueryResult {
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

class SqlEngine {
public:
    explicit SqlEngine(std::size_t cache_capacity = 256);

    bool execute(const std::string& sql, QueryResult& out, std::string& error);

private:
    struct Column {
        std::string name;
        DataType type;
        int varchar_limit = 0;
        bool not_null = false;
        bool primary_key = false;
    };

    struct Row {
        std::vector<std::string> values;
        std::int64_t expires_at_unix = 0;
    };

    struct Table {
        struct NumericIndexEntry {
            long double value = 0.0L;
            std::size_t row_idx = 0;
        };

        std::string name;
        std::vector<Column> columns;
        std::unordered_map<std::string, std::size_t> column_index;
        int primary_key_col = -1;
        std::unordered_map<std::string, std::size_t> primary_index;
        std::vector<std::vector<NumericIndexEntry>> numeric_range_index;
        std::vector<std::uint8_t> numeric_range_sorted;
        std::vector<std::vector<long double>> numeric_column_values;
        std::vector<std::vector<std::uint8_t>> numeric_column_valid;
        std::vector<Row> rows;
        std::uint64_t version = 1;
    };

    struct Condition {
        std::string column;
        std::string op;
        std::string value;
        bool present = false;
    };

    struct SelectPlan {
        bool valid = false;
        std::string base_table;
        std::string join_table;
        bool has_join = false;
        std::string join_left;
        std::string join_right;
        std::vector<std::string> select_columns;
        bool select_star = false;
        Condition where;
        std::vector<std::string> touched_tables;
    };

    struct CacheEntry {
        QueryResult result;
        std::unordered_map<std::string, std::uint64_t> versions;
        std::size_t approx_bytes = 0;
    };

    class QueryCache {
    public:
        explicit QueryCache(std::size_t capacity);
        bool get(const std::string& key,
                 const std::unordered_map<std::string, std::uint64_t>& current_versions,
                 QueryResult& out);
        void put(const std::string& key,
                 const QueryResult& result,
                 const std::unordered_map<std::string, std::uint64_t>& versions);

    private:
        std::size_t capacity_;
        std::list<std::pair<std::string, CacheEntry>> entries_;
        std::unordered_map<std::string,
                           std::list<std::pair<std::string, CacheEntry>>::iterator>
            map_;
        std::mutex mutex_;
        std::size_t max_bytes_ = 0;
        std::size_t current_bytes_ = 0;
    };

    bool execute_create_table(const std::string& sql, std::string& error);
    bool execute_insert(const std::string& sql, std::string& error);
    bool execute_select(const std::string& sql, QueryResult& out, std::string& error);

    bool parse_create_table(const std::string& sql,
                            std::string& table_name,
                            std::vector<Column>& columns,
                            int& primary_col,
                            std::string& error) const;
    bool parse_insert(const std::string& sql,
                      std::string& table_name,
                      std::vector<std::vector<std::string>>& rows,
                      std::vector<std::int64_t>& expires_at,
                      std::string& error) const;
    bool parse_select(const std::string& sql, SelectPlan& plan, std::string& error) const;

    bool evaluate_where(const Table& table,
                        const Row& row,
                        const Condition& condition,
                        std::string* error) const;
    bool evaluate_where_join(const Table& left,
                             const Row& left_row,
                             const Table& right,
                             const Row& right_row,
                             const Condition& condition,
                             std::string* error) const;

    std::unordered_map<std::string, std::uint64_t>
    capture_versions(const std::vector<std::string>& table_names) const;

    static std::string trim(const std::string& s);
    static std::string to_upper(std::string s);
    static std::string to_lower(std::string s);
    static std::string normalize_sql_for_cache(const std::string& s);
    static bool is_identifier(const std::string& s);
    static std::vector<std::string> split_csv(const std::string& s);
    static std::string unquote_literal(const std::string& s);
    static std::int64_t now_unix();
    static bool parse_datetime_to_unix(const std::string& s, std::int64_t& out);
    static bool parse_numeric_literal(const std::string& s, DataType type, long double& out);
    static bool fast_parse_int64(const std::string& s, std::int64_t& out);
    static bool fast_parse_long_double(const std::string& s, long double& out);
    static bool is_null_literal_ci(const std::string& s);
    static bool eval_numeric_op(long double lhs, long double rhs, const std::string& op, bool& out);
    static bool compare_values(const std::string& lhs,
                               const std::string& rhs,
                               DataType type,
                               const std::string& op,
                               bool& out,
                               std::string& error);
    static bool parse_condition(const std::string& text, Condition& out, std::string& error);
    static bool split_qualified(const std::string& qualified,
                                std::string& table,
                                std::string& column);
    static bool starts_with_keyword(const std::string& sql_upper, const std::string& kw);
    static std::size_t find_keyword(const std::string& sql_upper,
                                    const std::string& kw,
                                    std::size_t start = 0);
    static bool row_alive(const Row& row, std::int64_t now_ts);
    static bool row_numeric_value(const Table& table,
                                  const Row& row,
                                  std::size_t col_idx,
                                  long double& out);

    bool validate_typed_value(const Column& col,
                              std::string& value,
                              std::string& error,
                              long double* numeric_out = nullptr,
                              bool* numeric_valid = nullptr) const;
    const Column* lookup_column(const Table& table,
                                const std::string& col_ref,
                                std::size_t& idx,
                                std::string& error) const;

    mutable std::shared_mutex db_mutex_;
    mutable std::mutex numeric_index_mutex_;
    std::unordered_map<std::string, Table> tables_;
    QueryCache cache_;
};

}  // namespace flexql

#endif
