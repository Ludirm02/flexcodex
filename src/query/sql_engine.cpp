#include "sql_engine.hpp"

#pragma GCC optimize("O3,unroll-loops")
#pragma GCC target("avx2,bmi,bmi2,popcnt")

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>

namespace flexql {

namespace {

constexpr const char* kCreateTableKw = "CREATE TABLE";
constexpr const char* kInsertIntoKw = "INSERT INTO";
constexpr const char* kSelectKw = "SELECT";

inline bool is_space_char(char c) {
    return std::isspace(static_cast<unsigned char>(c)) != 0;
}

void trim_in_place(std::string& s) {
    std::size_t b = 0;
    while (b < s.size() && is_space_char(s[b])) {
        ++b;
    }
    std::size_t e = s.size();
    while (e > b && is_space_char(s[e - 1])) {
        --e;
    }
    if (b == 0 && e == s.size()) {
        return;
    }
    if (b >= e) {
        s.clear();
        return;
    }
    s.erase(e);
    s.erase(0, b);
}

std::size_t estimate_query_result_bytes(const QueryResult& result) {
    std::size_t bytes = sizeof(QueryResult);
    bytes += result.columns.size() * sizeof(std::string);
    for (const std::string& c : result.columns) {
        bytes += c.size();
    }
    bytes += result.rows.size() * sizeof(std::vector<std::string>);
    for (const auto& row : result.rows) {
        bytes += row.size() * sizeof(std::string);
        for (const std::string& cell : row) {
            bytes += cell.size();
        }
    }
    return bytes;
}

}  // namespace

SqlEngine::QueryCache::QueryCache(std::size_t capacity)
    : capacity_(capacity == 0 ? 1 : capacity),
      max_bytes_(512ULL * 1024 * 1024) {}

bool SqlEngine::QueryCache::get(const std::string& key,
                                const std::unordered_map<std::string, std::uint64_t>& current_versions,
                                QueryResult& out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) {
        return false;
    }

    const CacheEntry& entry = it->second->second;
    if (entry.versions != current_versions) {
        current_bytes_ -= entry.approx_bytes;
        entries_.erase(it->second);
        map_.erase(it);
        return false;
    }

    out = entry.result;
    entries_.splice(entries_.begin(), entries_, it->second);
    return true;
}

void SqlEngine::QueryCache::put(const std::string& key,
                                const QueryResult& result,
                                const std::unordered_map<std::string, std::uint64_t>& versions) {
    const std::size_t approx_bytes = estimate_query_result_bytes(result);
    if (approx_bytes > 256ULL * 1024 * 1024) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) {
        current_bytes_ -= it->second->second.approx_bytes;
        it->second->second.result = result;
        it->second->second.versions = versions;
        it->second->second.approx_bytes = approx_bytes;
        current_bytes_ += approx_bytes;
        entries_.splice(entries_.begin(), entries_, it->second);
    } else {
        entries_.push_front({key, CacheEntry{result, versions, approx_bytes}});
        map_[key] = entries_.begin();
        current_bytes_ += approx_bytes;
    }

    while (!entries_.empty() && (entries_.size() > capacity_ || current_bytes_ > max_bytes_)) {
        auto tail = std::prev(entries_.end());
        current_bytes_ -= tail->second.approx_bytes;
        map_.erase(tail->first);
        entries_.pop_back();
    }
}

SqlEngine::SqlEngine(std::size_t cache_capacity) : cache_(cache_capacity) {}

bool SqlEngine::execute(const std::string& sql, QueryResult& out, std::string& error) {
    out = QueryResult{};
    error.clear();

    // Fast path: avoid full string copy for INSERT batches
    // Just check first non-space char and strip trailing semicolon in-place
    const char* p = sql.c_str();
    while (*p && is_space_char(*p)) ++p;
    if (!*p) { error = "empty SQL statement"; return false; }

    std::string normalized(p);
    // Strip trailing whitespace and semicolon
    while (!normalized.empty() && (is_space_char(normalized.back()) || normalized.back() == ';')) {
        normalized.pop_back();
    }
    if (normalized.empty()) {
        error = "empty SQL statement";
        return false;
    }

    // Only uppercase first 20 chars to detect command type
    const std::size_t cmd_scan = std::min<std::size_t>(normalized.size(), 20);
    std::string upper = to_upper(normalized.substr(0, cmd_scan));
    if (starts_with_keyword(upper, kCreateTableKw)) {
        std::unique_lock<std::shared_mutex> lock(db_mutex_);
        return execute_create_table(normalized, error);
    }
    if (starts_with_keyword(upper, kInsertIntoKw)) {
        std::unique_lock<std::shared_mutex> lock(db_mutex_);
        return execute_insert(normalized, error);
    }
    if (starts_with_keyword(upper, kSelectKw)) {
        return execute_select(normalized, out, error);
    }

    error = "unsupported SQL command";
    return false;
}

bool SqlEngine::execute_create_table(const std::string& sql, std::string& error) {
    std::string table_name;
    std::vector<Column> columns;
    int primary_col = -1;
    if (!parse_create_table(sql, table_name, columns, primary_col, error)) {
        return false;
    }

    if (tables_.count(table_name) != 0U) {
        error = "table already exists: " + table_name;
        return false;
    }

    Table t;
    t.name = table_name;
    t.columns = columns;
    t.primary_key_col = primary_col;
    t.rows.reserve(1024);
    table.expiry_flat.push_back(table.rows.back().expires_at_unix);
    t.column_index.reserve(t.columns.size());
   
    t.numeric_column_values.resize(t.columns.size());
    t.numeric_column_valid.resize(t.columns.size());
    if (primary_col >= 0) {
        if (columns[static_cast<std::size_t>(primary_col)].type == DataType::kInt) {
            t.pk_is_int = true;
            t.pk_robin_index.reserve(1024);
        } else {
            t.primary_index.reserve(1024);
        }
    }

    for (std::size_t i = 0; i < t.columns.size(); ++i) {
        t.column_index[t.columns[i].name] = i;
        if (t.columns[i].type == DataType::kInt || t.columns[i].type == DataType::kDecimal ||
            t.columns[i].type == DataType::kDatetime) {
            if (t.primary_key_col >= 0 && i == static_cast<std::size_t>(t.primary_key_col)) {
                continue;
            }
            t.numeric_column_values[i].reserve(1024);
            t.numeric_column_valid[i].reserve(1024);
        }
    }

    tables_[t.name] = std::move(t);
    return true;
}

bool SqlEngine::execute_insert(const std::string& sql, std::string& error) {
    std::string table_name;
    std::vector<std::vector<std::string>> rows_values;
    std::vector<std::int64_t> expires_at;
    if (!parse_insert(sql, table_name, rows_values, expires_at, error)) {
        return false;
    }

    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        error = "unknown table: " + table_name;
        return false;
    }

    Table& table = it->second;
    if (rows_values.empty()) {
        error = "INSERT must contain at least one VALUES tuple";
        return false;
    }

    auto reserve_numeric_aux = [&](std::size_t target) {
        for (std::size_t i = 0; i < table.columns.size(); ++i) {
            if (table.columns[i].type != DataType::kInt &&
                table.columns[i].type != DataType::kDecimal &&
                table.columns[i].type != DataType::kDatetime) {
                continue;
            }
            if (table.primary_key_col >= 0 && i == static_cast<std::size_t>(table.primary_key_col)) {
                continue;
            }
           
            if (table.numeric_column_values[i].capacity() < target) {
                table.numeric_column_values[i].reserve(target);
            }
            if (table.numeric_column_valid[i].capacity() < target) {
                table.numeric_column_valid[i].reserve(target);
            }
        }
    };

    if (table.rows.empty() && rows_values.size() >= 512) {
        std::size_t bulk_target = rows_values.size() * 4096;
        if (bulk_target < 1'000'000) {
            bulk_target = 1'000'000;
        }
        if (bulk_target > 12'000'000) {
            bulk_target = 12'000'000;
        }
        if (table.rows.capacity() < bulk_target) {
            table.rows.reserve(bulk_target);
            table.expiry_flat.reserve(bulk_target);
            table.expiry_col.reserve(bulk_target);
            reserve_numeric_aux(bulk_target);
            if (table.primary_key_col >= 0) {
                if (table.pk_is_int) {
                    table.pk_robin_index.reserve(bulk_target);
                } else {
                    table.primary_index.reserve(static_cast<std::size_t>(bulk_target / 0.70));
                }
            }
        }
    }

    const std::size_t needed_rows_capacity = table.rows.size() + rows_values.size();
    if (table.rows.capacity() < needed_rows_capacity) {
        std::size_t new_capacity = std::max<std::size_t>(1024, table.rows.capacity() * 2);
        while (new_capacity < needed_rows_capacity) {
            new_capacity *= 2;
        }
        table.rows.reserve(new_capacity);
        table.expiry_flat.reserve(new_capacity);
        table.expiry_col.reserve(new_capacity);
        reserve_numeric_aux(new_capacity);
    }

    if (table.primary_key_col >= 0) {
        if (!table.pk_is_int) {
            const std::size_t cur_size = table.primary_index.size();
            const std::size_t buckets = table.primary_index.bucket_count();
            const std::size_t projected = cur_size + rows_values.size();
            if (buckets == 0 || projected > static_cast<std::size_t>(static_cast<double>(buckets) * 0.70)) {
                std::size_t target = std::max<std::size_t>(1024, buckets == 0 ? projected * 2 : buckets * 2);
                if (target < projected * 2) target = projected * 2;
                table.primary_index.reserve(target);
            }
        }
    }

    std::vector<double> parsed_numeric(table.columns.size(), 0.0);
    std::vector<std::uint8_t> parsed_numeric_valid(table.columns.size(), 0U);

    const std::int64_t now_ts = now_unix();
    for (std::size_t r = 0; r < rows_values.size(); ++r) {
        Row row;
        row.values = std::move(rows_values[r]);
        row.expires_at_unix = expires_at[r];
        std::string pk_key;

        if (row.values.size() != table.columns.size()) {
            error = "column count mismatch in INSERT";
            return false;
        }

        std::fill(parsed_numeric_valid.begin(), parsed_numeric_valid.end(), 0U);
        for (std::size_t i = 0; i < row.values.size(); ++i) {
            double parsed = 0.0;
            bool numeric_valid = false;
            if (!validate_typed_value(table.columns[i], row.values[i], error, &parsed, &numeric_valid)) {
                return false;
            }
            if (numeric_valid) {
                parsed_numeric[i] = parsed;
                parsed_numeric_valid[i] = 1U;
            }
        }

        std::int64_t pk_int_val = 0;
        if (table.primary_key_col >= 0) {
            pk_key = row.values[static_cast<std::size_t>(table.primary_key_col)];
            if (table.pk_is_int) {
                fast_parse_int64(pk_key, pk_int_val);
                auto existing = table.pk_robin_index.lookup(pk_int_val);
                if (existing != RobinHoodIndex::kEmpty && existing < table.rows.size()
                    && row_alive(table.rows[existing], now_ts)) {
                    error = "duplicate primary key value: " + pk_key;
                    return false;
                }
            } else {
                auto idx_it = table.primary_index.find(pk_key);
                if (idx_it != table.primary_index.end()) {
                    std::size_t row_idx = idx_it->second;
                    if (row_idx < table.rows.size() && row_alive(table.rows[row_idx], now_ts)) {
                        error = "duplicate primary key value: " + pk_key;
                        return false;
                    }
                }
            }
        }

        table.rows.push_back(std::move(row));
        table.expiry_flat.push_back(table.rows.back().expires_at_unix);
        table.expiry_flat.push_back(table.rows.back().expires_at_unix);
        const std::size_t row_idx = table.rows.size() - 1;
        table.expiry_col.push_back(table.rows[row_idx].expires_at_unix);

        if (table.primary_key_col >= 0) {
            if (table.pk_is_int) {
                table.pk_robin_index.insert(pk_int_val, row_idx);
            } else {
                table.primary_index[pk_key] = row_idx;
            }
        }

        for (std::size_t i = 0; i < table.columns.size(); ++i) {
            if (table.columns[i].type != DataType::kInt &&
                table.columns[i].type != DataType::kDecimal &&
                table.columns[i].type != DataType::kDatetime) {
                continue;
            }
            if (table.primary_key_col >= 0 && i == static_cast<std::size_t>(table.primary_key_col)) {
                continue;
            }

            table.numeric_column_values[i].push_back(parsed_numeric[i]);
            table.numeric_column_valid[i].push_back(parsed_numeric_valid[i]);
        }
    }

    ++table.version;
    return true;
}

bool SqlEngine::execute_select(const std::string& sql, QueryResult& out, std::string& error) {
    SelectPlan plan;
    if (!parse_select(sql, plan, error)) {
        return false;
    }

    const std::int64_t now_ts = now_unix();

    std::shared_lock<std::shared_mutex> lock(db_mutex_);
    auto base_it = tables_.find(plan.base_table);
    if (base_it == tables_.end()) {
        error = "unknown table: " + plan.base_table;
        return false;
    }

    const Table& base = base_it->second;
    bool skip_cache = false;
    if (!plan.has_join && plan.where.present && plan.where.op == "=" && base.primary_key_col >= 0) {
        const std::string& pk_name = base.columns[static_cast<std::size_t>(base.primary_key_col)].name;
        if (plan.where.column == pk_name || plan.where.column == (base.name + "." + pk_name)) {
            skip_cache = true;
        }
    }

    std::string cache_key;
    std::unordered_map<std::string, std::uint64_t> versions;
    if (!skip_cache) {
        cache_key = normalize_sql_for_cache(sql);
        versions = capture_versions(plan.touched_tables);
        if (cache_.get(cache_key, versions, out)) {
            return true;
        }
    }

    if (!plan.has_join) {
        std::vector<std::size_t> selected_indexes;
        if (plan.select_star) {
            for (std::size_t i = 0; i < base.columns.size(); ++i) {
                out.columns.push_back(base.columns[i].name);
                selected_indexes.push_back(i);
            }
        } else {
            for (const std::string& c : plan.select_columns) {
                std::string ref_table;
                std::string ref_col;
                if (split_qualified(c, ref_table, ref_col) && !ref_table.empty() && ref_table != base.name) {
                    error = "column references unknown table: " + c;
                    return false;
                }
                std::size_t idx = 0;
                std::string lookup_error;
                if (lookup_column(base, ref_col.empty() ? c : ref_col, idx, lookup_error) == nullptr) {
                    error = lookup_error;
                    return false;
                }
                out.columns.push_back(base.columns[idx].name);
                selected_indexes.push_back(idx);
            }
        }

        struct WhereMeta {
            bool enabled = false;
            std::size_t idx = 0;
            DataType type = DataType::kVarchar;
            std::string op;
            char op0 = '\0';
            char op1 = '\0';
            std::string rhs_unquoted;
            bool rhs_numeric_valid = false;
            double rhs_numeric = 0.0;
        } where_meta;

        if (plan.where.present) {
            std::string cond_table;
            std::string cond_col;
            split_qualified(plan.where.column, cond_table, cond_col);
            std::string raw_col = cond_col.empty() ? plan.where.column : cond_col;
            if (!cond_table.empty() && cond_table != base.name) {
                error = "WHERE references unknown table: " + cond_table;
                return false;
            }

            std::string lookup_error;
            const Column* col = lookup_column(base, raw_col, where_meta.idx, lookup_error);
            if (col == nullptr) {
                error = lookup_error;
                return false;
            }

            where_meta.enabled = true;
            where_meta.type = col->type;
            where_meta.op = plan.where.op;
            where_meta.op0 = plan.where.op[0];
            where_meta.op1 = plan.where.op.size() > 1 ? plan.where.op[1] : '\0';
            where_meta.rhs_unquoted = unquote_literal(plan.where.value);
            where_meta.rhs_numeric_valid = parse_numeric_literal(where_meta.rhs_unquoted, where_meta.type,
                                                                 where_meta.rhs_numeric);
        }

        if (!where_meta.enabled) {
            out.rows.reserve(base.rows.size());
        } else if (where_meta.op == "=") {
            if (base.primary_key_col >= 0 && where_meta.idx == static_cast<std::size_t>(base.primary_key_col)) {
                out.rows.reserve(1);
            } else {
                out.rows.reserve(std::max<std::size_t>(128, base.rows.size() / 64));
            }
        } else if (where_meta.op == "!=") {
            out.rows.reserve(base.rows.size());
        } else {
            out.rows.reserve(std::max<std::size_t>(1024, base.rows.size() / 2));
        }

        const std::size_t proj_count = selected_indexes.size();
        std::vector<std::string> projected_buf(proj_count);

        auto emit_projected_row = [&](std::size_t row_idx) {
            const Row& row = base.rows[row_idx];
            if (plan.select_star) {
                out.rows.push_back(row.values);
            } else {
                for (std::size_t i = 0; i < proj_count; ++i) {
                    projected_buf[i] = row.values[selected_indexes[i]];
                }
                out.rows.push_back(projected_buf);
            }
        };

        auto fast_numeric_value = [&](std::size_t row_idx, std::size_t col_idx, double& out_num) -> bool {
            if (col_idx >= base.numeric_column_valid.size() || col_idx >= base.numeric_column_values.size()) {
                return false;
            }
            const auto& valid = base.numeric_column_valid[col_idx];
            const auto& values = base.numeric_column_values[col_idx];
            if (row_idx >= valid.size() || row_idx >= values.size() || valid[row_idx] == 0U) {
                return false;
            }
            out_num = values[row_idx];
            return true;
        };

        auto passes_where = [&](std::size_t row_idx, bool& pass) -> bool {
            if (!where_meta.enabled) {
                pass = true;
                return true;
            }

            double lhs_numeric = 0.0;
            if (where_meta.rhs_numeric_valid &&
                fast_numeric_value(row_idx, where_meta.idx, lhs_numeric) &&
                (where_meta.type == DataType::kInt || where_meta.type == DataType::kDecimal ||
                 where_meta.type == DataType::kDatetime)) {
                if (!eval_numeric_op(lhs_numeric, where_meta.rhs_numeric, where_meta.op, pass)) {
                    error = "unsupported operator in WHERE: " + where_meta.op;
                    return false;
                }
                return true;
            }

            std::string cmp_error;
            if (!compare_values(base.rows[row_idx].values[where_meta.idx],
                                where_meta.rhs_unquoted,
                                where_meta.type,
                                where_meta.op,
                                pass, cmp_error)) {
                error = cmp_error;
                return false;
            }
            return true;
        };

        auto maybe_pk_index_scan = [&]() -> bool {
            if (!where_meta.enabled || where_meta.op != "=" || base.primary_key_col < 0) {
                return false;
            }
            if (where_meta.idx != static_cast<std::size_t>(base.primary_key_col)) {
                return false;
            }

            if (base.pk_is_int && where_meta.rhs_numeric_valid) {
                std::int64_t pk_int = static_cast<std::int64_t>(where_meta.rhs_numeric);
                auto row_idx = base.pk_robin_index.lookup(pk_int);
                if (row_idx == RobinHoodIndex::kEmpty || row_idx >= base.rows.size()) {
                    return true;
                }
                if (!row_alive(base.rows[row_idx], now_ts)) return true;
                bool pass = false;
                if (!passes_where(row_idx, pass)) return true;
                if (pass) emit_projected_row(row_idx);
                return true;
            }
            const std::string& key = where_meta.rhs_unquoted;
            auto idx_it = base.primary_index.find(key);
            if (idx_it == base.primary_index.end() || idx_it->second >= base.rows.size()) {
                return true;
            }
            const std::size_t row_idx = idx_it->second;
            if (!row_alive(base.rows[row_idx], now_ts)) {
                return true;
            }
            bool pass = false;
            if (!passes_where(row_idx, pass)) {
                return true;
            }
            if (!pass) {
                return true;
            }
            emit_projected_row(row_idx);
            return true;
        };

        auto maybe_numeric_range_scan = [&]() -> bool {
            if (!where_meta.enabled || !where_meta.rhs_numeric_valid) {
                return false;
            }
            if (where_meta.type != DataType::kInt && where_meta.type != DataType::kDecimal &&
                where_meta.type != DataType::kDatetime) {
                return false;
            }
            if (where_meta.op == "!=") {
                return false;
            }
            if (where_meta.idx >= base.numeric_column_values.size()) {
                return false;
            }

            const auto& col_vals  = base.numeric_column_values[where_meta.idx];
            const auto& col_valid = base.numeric_column_valid[where_meta.idx];
            const std::size_t n   = base.rows.size();
            if (col_vals.empty()) {
                return true;
            }

            out.rows.reserve(std::max<std::size_t>(1024, n / 2));

            const double rhs = where_meta.rhs_numeric;
            const char op0   = where_meta.op0;
            const char op1   = where_meta.op1;
            const std::int64_t* __restrict__ expiry_ptr = base.expiry_flat.data();

            for (std::size_t i = 0; i < n; ++i) {
                if (col_valid[i] == 0U) continue;
                if (expiry_ptr[i] != 0 && expiry_ptr[i] <= now_ts) continue;
                const double lhs = col_vals[i];
                bool pass;
                if      (op0 == '=' && op1 == '\0') pass = (lhs == rhs);
                else if (op0 == '!')                pass = (lhs != rhs);
                else if (op0 == '>' && op1 == '\0') pass = (lhs >  rhs);
                else if (op0 == '>' && op1 == '=')  pass = (lhs >= rhs);
                else if (op0 == '<' && op1 == '\0') pass = (lhs <  rhs);
                else                                pass = (lhs <= rhs);
                if (!pass) continue;
                emit_projected_row(i);
            }
            return true;
        };

        if (!maybe_pk_index_scan()) {
            bool used_numeric_index = maybe_numeric_range_scan();
            if (!error.empty()) {
                return false;
            }
            if (!used_numeric_index) {
                for (std::size_t row_idx = 0; row_idx < base.rows.size(); ++row_idx) {
                    if (row_idx < base.expiry_col.size() && base.expiry_col[row_idx] != 0 && base.expiry_col[row_idx] <= now_ts) {
                        continue;
                    }
                    const Row& row = base.rows[row_idx];
                    bool pass = false;
                    if (!passes_where(row_idx, pass)) {
                        return false;
                    }
                    if (!pass) {
                        continue;
                    }
                    emit_projected_row(row_idx);
                }
            }
        }

        if (!skip_cache) {
            cache_.put(cache_key, out, versions);
        }
        return true;
    }

    auto join_it = tables_.find(plan.join_table);
    if (join_it == tables_.end()) {
        error = "unknown table: " + plan.join_table;
        return false;
    }
    const Table& right = join_it->second;

    std::string left_join_tbl;
    std::string left_join_col;
    if (!split_qualified(plan.join_left, left_join_tbl, left_join_col) || left_join_col.empty()) {
        error = "invalid join condition, expected table.column = table.column";
        return false;
    }
    std::string right_join_tbl;
    std::string right_join_col;
    if (!split_qualified(plan.join_right, right_join_tbl, right_join_col) || right_join_col.empty()) {
        error = "invalid join condition, expected table.column = table.column";
        return false;
    }

    const Table* join_left_table = nullptr;
    const Table* join_right_table = nullptr;

    if (left_join_tbl == base.name) {
        join_left_table = &base;
    } else if (left_join_tbl == right.name) {
        join_left_table = &right;
    }

    if (right_join_tbl == base.name) {
        join_right_table = &base;
    } else if (right_join_tbl == right.name) {
        join_right_table = &right;
    }

    if (join_left_table == nullptr || join_right_table == nullptr || join_left_table == join_right_table) {
        error = "join columns must reference one column from each table";
        return false;
    }

    std::size_t left_join_idx = 0;
    std::size_t right_join_idx = 0;
    std::string lookup_error;
    if (lookup_column(*join_left_table, left_join_col, left_join_idx, lookup_error) == nullptr) {
        error = lookup_error;
        return false;
    }
    if (lookup_column(*join_right_table, right_join_col, right_join_idx, lookup_error) == nullptr) {
        error = lookup_error;
        return false;
    }

    struct Projection {
        const Table* table;
        std::size_t index;
        std::string output_name;
    };
    std::vector<Projection> projections;

    if (plan.select_star) {
        for (std::size_t i = 0; i < base.columns.size(); ++i) {
            projections.push_back({&base, i, base.name + "." + base.columns[i].name});
        }
        for (std::size_t i = 0; i < right.columns.size(); ++i) {
            projections.push_back({&right, i, right.name + "." + right.columns[i].name});
        }
    } else {
        for (const std::string& ref : plan.select_columns) {
            std::string tbl;
            std::string col;
            split_qualified(ref, tbl, col);

            auto push_projection = [&](const Table* t, const std::string& c, const std::string& output) {
                std::size_t idx = 0;
                std::string err;
                if (lookup_column(*t, c, idx, err) == nullptr) {
                    error = err;
                    return false;
                }
                projections.push_back({t, idx, output});
                return true;
            };

            if (!tbl.empty()) {
                if (tbl == base.name) {
                    if (!push_projection(&base, col, base.name + "." + col)) {
                        return false;
                    }
                } else if (tbl == right.name) {
                    if (!push_projection(&right, col, right.name + "." + col)) {
                        return false;
                    }
                } else {
                    error = "unknown table in select list: " + tbl;
                    return false;
                }
                continue;
            }

            std::size_t base_idx = 0;
            std::size_t right_idx = 0;
            std::string err1;
            std::string err2;
            bool in_base = lookup_column(base, ref, base_idx, err1) != nullptr;
            bool in_right = lookup_column(right, ref, right_idx, err2) != nullptr;

            if (in_base && in_right) {
                error = "ambiguous column in select list: " + ref;
                return false;
            }
            if (!in_base && !in_right) {
                error = "unknown column in select list: " + ref;
                return false;
            }

            if (in_base) {
                projections.push_back({&base, base_idx, base.name + "." + ref});
            } else {
                projections.push_back({&right, right_idx, right.name + "." + ref});
            }
        }
    }

    out.columns.reserve(projections.size());
    for (const Projection& p : projections) {
        out.columns.push_back(p.output_name);
    }

    std::size_t base_join_idx = 0;
    std::size_t right_join_idx_for_table = 0;
    if (join_left_table == &base) {
        base_join_idx = left_join_idx;
        right_join_idx_for_table = right_join_idx;
    } else {
        base_join_idx = right_join_idx;
        right_join_idx_for_table = left_join_idx;
    }

    DataType join_cmp_type = base.columns[base_join_idx].type;
    if (base.columns[base_join_idx].type != right.columns[right_join_idx_for_table].type) {
        join_cmp_type = DataType::kVarchar;
    }

    auto make_join_key = [&](const Table& table, std::size_t row_idx, std::size_t idx, std::string& key_out) -> bool {
        double numeric_value = 0.0;
        if ((join_cmp_type == DataType::kInt || join_cmp_type == DataType::kDecimal ||
             join_cmp_type == DataType::kDatetime) && idx < table.numeric_column_valid.size() &&
            idx < table.numeric_column_values.size() &&
            row_idx < table.numeric_column_valid[idx].size() &&
            row_idx < table.numeric_column_values[idx].size() &&
            table.numeric_column_valid[idx][row_idx] != 0U) {
            numeric_value = table.numeric_column_values[idx][row_idx];
            if (join_cmp_type == DataType::kInt) {
                long long n = static_cast<long long>(numeric_value);
                key_out = "I:" + std::to_string(n);
                return true;
            }
            if (join_cmp_type == DataType::kDecimal) {
                std::ostringstream oss;
                oss << std::setprecision(std::numeric_limits<double>::max_digits10) << numeric_value;
                key_out = "D:" + oss.str();
                return true;
            }
            long long ts = static_cast<long long>(numeric_value);
            key_out = "T:" + std::to_string(ts);
            return true;
        }

        if (row_idx >= table.rows.size() || idx >= table.rows[row_idx].values.size()) {
            return false;
        }
        const std::string& raw = table.rows[row_idx].values[idx];
        const std::string v = trim(raw);
        if (is_null_literal_ci(v)) {
            key_out = "N:";
            return true;
        }

        if (join_cmp_type == DataType::kInt) {
            std::int64_t n = 0;
            if (!fast_parse_int64(v, n)) {
                key_out = "X:" + v;
                return true;
            }
            key_out = "I:" + std::to_string(n);
            return true;
        }
        if (join_cmp_type == DataType::kDecimal) {
            double d = 0.0;
            if (!fast_parse_double(v, d)) {
                key_out = "X:" + v;
                return true;
            }
            std::ostringstream oss;
            oss << std::setprecision(std::numeric_limits<double>::max_digits10) << d;
            key_out = "D:" + oss.str();
            return true;
        }
        if (join_cmp_type == DataType::kDatetime) {
            std::int64_t ts = 0;
            if (parse_datetime_to_unix(v, ts)) {
                key_out = "T:" + std::to_string(ts);
            } else {
                key_out = "S:" + v;
            }
            return true;
        }
        key_out = "S:" + v;
        return true;
    };

    const Table* hash_table = &base;
    const Table* probe_table = &right;
    std::size_t hash_idx = base_join_idx;
    std::size_t probe_idx = right_join_idx_for_table;
    bool hash_is_base = true;

    if (right.rows.size() < base.rows.size()) {
        hash_table = &right;
        probe_table = &base;
        hash_idx = right_join_idx_for_table;
        probe_idx = base_join_idx;
        hash_is_base = false;
    }

    // Use int64 hash map for INT joins only when both join columns are fully numeric-backed.
    std::unordered_map<std::int64_t, std::vector<std::size_t>> join_hash_int;
    std::unordered_map<std::string, std::vector<std::size_t>> join_hash_str;
    const bool join_use_int = (join_cmp_type == DataType::kInt);
    const bool hash_int_backed = join_use_int &&
        hash_idx < hash_table->numeric_column_valid.size() &&
        hash_idx < hash_table->numeric_column_values.size() &&
        hash_table->numeric_column_valid[hash_idx].size() == hash_table->rows.size() &&
        hash_table->numeric_column_values[hash_idx].size() == hash_table->rows.size();
    const bool probe_int_backed = join_use_int &&
        probe_idx < probe_table->numeric_column_valid.size() &&
        probe_idx < probe_table->numeric_column_values.size() &&
        probe_table->numeric_column_valid[probe_idx].size() == probe_table->rows.size() &&
        probe_table->numeric_column_values[probe_idx].size() == probe_table->rows.size();
    const bool join_use_int_fast = hash_int_backed && probe_int_backed;

    if (join_use_int_fast) {
        join_hash_int.reserve(hash_table->rows.size() > 0 ? hash_table->rows.size() : 1);
    } else {
        join_hash_str.reserve(hash_table->rows.size() > 0 ? hash_table->rows.size() : 1);
    }

    for (std::size_t hash_row_idx = 0; hash_row_idx < hash_table->rows.size(); ++hash_row_idx) {
        const Row& row = hash_table->rows[hash_row_idx];
        if (!row_alive(row, now_ts)) continue;
        if (join_use_int_fast) {
            if (hash_table->numeric_column_valid[hash_idx][hash_row_idx] == 0U) {
                continue;
            }
            std::int64_t k = static_cast<std::int64_t>(hash_table->numeric_column_values[hash_idx][hash_row_idx]);
            join_hash_int[k].push_back(hash_row_idx);
        } else {
            std::string key;
            if (!make_join_key(*hash_table, hash_row_idx, hash_idx, key)) {
                error = "failed to compute join key"; return false;
            }
            join_hash_str[key].push_back(hash_row_idx);
        }
    }

    for (std::size_t probe_row_idx = 0; probe_row_idx < probe_table->rows.size(); ++probe_row_idx) {
        const Row& probe_row = probe_table->rows[probe_row_idx];
        if (!row_alive(probe_row, now_ts)) {
            continue;
        }

        std::vector<std::size_t>* hit_rows = nullptr;
        if (join_use_int_fast) {
            if (probe_table->numeric_column_valid[probe_idx][probe_row_idx] == 0U) {
                continue;
            }
            std::int64_t k = static_cast<std::int64_t>(probe_table->numeric_column_values[probe_idx][probe_row_idx]);
            auto hit = join_hash_int.find(k);
            if (hit != join_hash_int.end()) hit_rows = &hit->second;
        } else {
            std::string key;
            if (!make_join_key(*probe_table, probe_row_idx, probe_idx, key)) {
                error = "failed to compute join key"; return false;
            }
            auto hit = join_hash_str.find(key);
            if (hit != join_hash_str.end()) hit_rows = &hit->second;
        }
        if (!hit_rows) continue;

        for (std::size_t matched_row_idx : *hit_rows) {
            const std::size_t base_row_idx = hash_is_base ? matched_row_idx : probe_row_idx;
            const std::size_t right_row_idx = hash_is_base ? probe_row_idx : matched_row_idx;
            const Row* base_row_ptr = &base.rows[base_row_idx];
            const Row* right_row_ptr = &right.rows[right_row_idx];

            std::string where_error;
            bool pass = evaluate_where_join(base,
                                            *base_row_ptr,
                                            base_row_idx,
                                            right,
                                            *right_row_ptr,
                                            right_row_idx,
                                            plan.where,
                                            &where_error);
            if (!where_error.empty()) {
                error = where_error;
                return false;
            }
            if (!pass) {
                continue;
            }

            std::vector<std::string> projected;
            projected.reserve(projections.size());
            for (const Projection& p : projections) {
                if (p.table == &base) {
                    projected.push_back(base.rows[base_row_idx].values[p.index]);
                } else {
                    projected.push_back(right.rows[right_row_idx].values[p.index]);
                }
            }
            out.rows.push_back(std::move(projected));
        }
    }

    if (!skip_cache) {
        cache_.put(cache_key, out, versions);
    }
    return true;
}

bool SqlEngine::parse_create_table(const std::string& sql,
                                   std::string& table_name,
                                   std::vector<Column>& columns,
                                   int& primary_col,
                                   std::string& error) const {
    // Avoid trimming/copying — sql is already trimmed by execute()
    const std::string& s = sql;
    std::string upper = to_upper(s);
    if (!starts_with_keyword(upper, kCreateTableKw)) {
        error = "expected CREATE TABLE";
        return false;
    }

    std::size_t open_paren = s.find('(');
    std::size_t close_paren = s.rfind(')');
    if (open_paren == std::string::npos || close_paren == std::string::npos || close_paren <= open_paren) {
        error = "invalid CREATE TABLE syntax";
        return false;
    }

    std::string table_part = trim(s.substr(std::strlen(kCreateTableKw), open_paren - std::strlen(kCreateTableKw)));
    if (!is_identifier(table_part)) {
        error = "invalid table name";
        return false;
    }
    table_name = to_lower(table_part);

    std::string columns_part = s.substr(open_paren + 1, close_paren - open_paren - 1);
    auto defs = split_csv(columns_part);
    if (defs.empty()) {
        error = "table must contain at least one column";
        return false;
    }

    std::string pk_from_constraint;
    for (std::string def : defs) {
        def = trim(def);
        if (def.empty()) {
            continue;
        }
        std::string def_upper = to_upper(def);

        if (starts_with_keyword(def_upper, "PRIMARY KEY")) {
            std::size_t l = def.find('(');
            std::size_t r = def.find(')');
            if (l == std::string::npos || r == std::string::npos || r <= l + 1) {
                error = "invalid PRIMARY KEY constraint";
                return false;
            }
            pk_from_constraint = to_lower(trim(def.substr(l + 1, r - l - 1)));
            continue;
        }

        std::istringstream iss(def);
        std::string col_name;
        std::string type_token;
        if (!(iss >> col_name >> type_token)) {
            error = "invalid column definition: " + def;
            return false;
        }

        if (!is_identifier(col_name)) {
            error = "invalid column name: " + col_name;
            return false;
        }

        Column col;
        col.name = to_lower(col_name);
        std::string type_upper = to_upper(type_token);

        if (type_upper == "INT" || type_upper == "INTEGER") {
            col.type = DataType::kInt;
        } else if (type_upper == "DECIMAL") {
            col.type = DataType::kDecimal;
        } else if (type_upper == "DATETIME") {
            col.type = DataType::kDatetime;
        } else if (type_upper.rfind("VARCHAR", 0) == 0 || type_upper == "TEXT") {
            col.type = DataType::kVarchar;
            std::size_t l = type_token.find('(');
            std::size_t r = type_token.find(')');
            if (l != std::string::npos && r != std::string::npos && r > l + 1) {
                try {
                    col.varchar_limit = std::stoi(type_token.substr(l + 1, r - l - 1));
                } catch (...) {
                    error = "invalid VARCHAR length in: " + type_token;
                    return false;
                }
                if (col.varchar_limit <= 0) {
                    error = "VARCHAR length must be positive";
                    return false;
                }
            }
        } else {
            error = "unsupported type in column definition: " + type_token;
            return false;
        }

        std::string remainder;
        std::getline(iss, remainder);
        remainder = to_upper(trim(remainder));
        if (!remainder.empty()) {
            std::istringstream attrs(remainder);
            std::vector<std::string> tokens;
            std::string tok;
            while (attrs >> tok) {
                tokens.push_back(tok);
            }

            std::size_t i = 0;
            while (i < tokens.size()) {
                if (tokens[i] == "PRIMARY") {
                    if (i + 1 >= tokens.size() || tokens[i + 1] != "KEY") {
                        error = "unsupported column attributes: " + remainder;
                        return false;
                    }
                    col.primary_key = true;
                    col.not_null = true;
                    i += 2;
                    continue;
                }
                if (tokens[i] == "NOT") {
                    if (i + 1 >= tokens.size() || tokens[i + 1] != "NULL") {
                        error = "unsupported column attributes: " + remainder;
                        return false;
                    }
                    col.not_null = true;
                    i += 2;
                    continue;
                }
                if (tokens[i] == "NULL") {
                    i += 1;
                    continue;
                }
                error = "unsupported column attributes: " + remainder;
                return false;
            }
        }

        columns.push_back(col);
    }

    if (columns.empty()) {
        error = "table must contain at least one concrete column";
        return false;
    }

    std::set<std::string> seen;
    for (const Column& c : columns) {
        if (!seen.insert(c.name).second) {
            error = "duplicate column name: " + c.name;
            return false;
        }
    }

    primary_col = -1;
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].primary_key) {
            if (primary_col != -1) {
                error = "multiple primary keys are not supported";
                return false;
            }
            primary_col = static_cast<int>(i);
        }
    }

    if (!pk_from_constraint.empty()) {
        if (primary_col != -1) {
            error = "primary key already specified inline";
            return false;
        }
        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].name == pk_from_constraint) {
                columns[i].primary_key = true;
                columns[i].not_null = true;
                primary_col = static_cast<int>(i);
                break;
            }
        }
        if (primary_col == -1) {
            error = "PRIMARY KEY references unknown column: " + pk_from_constraint;
            return false;
        }
    }

    return true;
}

bool SqlEngine::parse_insert(const std::string& sql,
                             std::string& table_name,
                             std::vector<std::vector<std::string>>& rows,
                             std::vector<std::int64_t>& expires_at,
                             std::string& error) const {
    const std::string& s = sql;
    // Only uppercase first 128 chars — INSERT INTO tablename VALUES always fits here
    // This avoids copying the entire 40KB batch INSERT string for case conversion
    const std::size_t header_scan = std::min<std::size_t>(s.size(), 128);
    std::string upper = to_upper(s.substr(0, header_scan));
    if (!starts_with_keyword(upper, kInsertIntoKw)) {
        error = "expected INSERT INTO";
        return false;
    }

    std::size_t values_kw = find_keyword(upper, "VALUES", 0);
    if (values_kw == std::string::npos) {
        upper = to_upper(s);
        values_kw = find_keyword(upper, "VALUES", 0);
    }
    if (values_kw == std::string::npos) {
        error = "INSERT must contain VALUES clause";
        return false;
    }

    std::string table_part = trim(s.substr(std::strlen(kInsertIntoKw), values_kw - std::strlen(kInsertIntoKw)));    if (!is_identifier(table_part)) {
        error = "invalid table name in INSERT";
        return false;
    }
    table_name = to_lower(table_part);

    std::size_t pos = values_kw + std::strlen("VALUES");
    rows.clear();
    expires_at.clear();

    auto skip_spaces = [&](std::size_t& p) {
        while (p < s.size() && std::isspace(static_cast<unsigned char>(s[p])) != 0) {
            ++p;
        }
    };

    skip_spaces(pos);
    if (pos >= s.size() || s[pos] != '(') {
        error = "INSERT VALUES missing opening parenthesis";
        return false;
    }

    while (pos < s.size() && s[pos] == '(') {
        int depth = 0;
        bool in_single = false;
        bool in_double = false;
        std::size_t close_paren = std::string::npos;
        for (std::size_t i = pos; i < s.size(); ++i) {
            char c = s[i];
            if (c == '\'' && !in_double) {
                if (i + 1 < s.size() && s[i + 1] == '\'') {
                    ++i;
                    continue;
                }
                in_single = !in_single;
                continue;
            }
            if (c == '"' && !in_single) {
                in_double = !in_double;
                continue;
            }
            if (in_single || in_double) {
                continue;
            }
            if (c == '(') {
                ++depth;
            } else if (c == ')') {
                --depth;
                if (depth == 0) {
                    close_paren = i;
                    break;
                }
            }
        }

        if (close_paren == std::string::npos) {
            error = "INSERT VALUES missing closing parenthesis";
            return false;
        }

        std::string values_part = s.substr(pos + 1, close_paren - pos - 1);
        auto raw_values = split_csv(values_part);
        std::vector<std::string> tuple_values;
        tuple_values.reserve(raw_values.size());
        for (const std::string& raw : raw_values) {
            tuple_values.push_back(unquote_literal(raw));
        }
        rows.push_back(std::move(tuple_values));

        pos = close_paren + 1;
        skip_spaces(pos);
        if (pos < s.size() && s[pos] == ',') {
            ++pos;
            skip_spaces(pos);
            if (pos >= s.size() || s[pos] != '(') {
                error = "INSERT VALUES has malformed tuple list";
                return false;
            }
            continue;
        }
        break;
    }

    if (rows.empty()) {
        error = "INSERT must provide at least one row";
        return false;
    }

    std::int64_t ttl_or_expiry = 0;
    std::string tail = trim(s.substr(pos));
    if (tail.empty()) {
        expires_at.assign(rows.size(), 0);
        return true;
    }

    std::string tail_upper = to_upper(tail);
    if (starts_with_keyword(tail_upper, "EXPIRES")) {
        std::string token = trim(tail.substr(std::strlen("EXPIRES")));
        token = unquote_literal(token);
        if (token.empty()) {
            error = "EXPIRES expects a unix timestamp or datetime";
            return false;
        }

        try {
            std::size_t consumed = 0;
            std::int64_t epoch = std::stoll(token, &consumed);
            if (consumed == token.size()) {
                ttl_or_expiry = epoch;
                expires_at.assign(rows.size(), ttl_or_expiry);
                return true;
            }
        } catch (...) {
        }

        std::int64_t parsed = 0;
        if (!parse_datetime_to_unix(token, parsed)) {
            error = "invalid EXPIRES value, expected unix timestamp or YYYY-MM-DD HH:MM:SS";
            return false;
        }
        ttl_or_expiry = parsed;
        expires_at.assign(rows.size(), ttl_or_expiry);
        return true;
    }

    if (starts_with_keyword(tail_upper, "TTL")) {
        std::string token = trim(tail.substr(std::strlen("TTL")));
        token = unquote_literal(token);
        if (token.empty()) {
            error = "TTL expects a number of seconds";
            return false;
        }

        std::int64_t ttl = 0;
        try {
            std::size_t consumed = 0;
            ttl = std::stoll(token, &consumed);
            if (consumed != token.size()) {
                error = "invalid TTL value";
                return false;
            }
        } catch (...) {
            error = "invalid TTL value";
            return false;
        }

        if (ttl < 0) {
            error = "TTL cannot be negative";
            return false;
        }
        ttl_or_expiry = now_unix() + ttl;
        expires_at.assign(rows.size(), ttl_or_expiry);
        return true;
    }

    error = "unsupported INSERT tail; use EXPIRES <unix|datetime> or TTL <seconds>";
    return false;
}

bool SqlEngine::parse_select(const std::string& sql, SelectPlan& plan, std::string& error) const {
    plan = SelectPlan{};

    const std::string& s = sql;
    std::string upper = to_upper(s);
    if (!starts_with_keyword(upper, kSelectKw)) {
        error = "expected SELECT statement";
        return false;
    }

    std::size_t from_pos = find_keyword(upper, "FROM", 0);
    if (from_pos == std::string::npos) {
        error = "SELECT missing FROM clause";
        return false;
    }

    std::string select_part = trim(s.substr(std::strlen(kSelectKw), from_pos - std::strlen(kSelectKw)));
    if (select_part.empty()) {
        error = "SELECT list cannot be empty";
        return false;
    }

    std::string from_remainder = trim(s.substr(from_pos + std::strlen("FROM")));
    if (from_remainder.empty()) {
        error = "SELECT missing table after FROM";
        return false;
    }

    std::string from_upper = to_upper(from_remainder);
    std::size_t join_pos = find_keyword(from_upper, "INNER JOIN", 0);
    std::size_t where_pos = find_keyword(from_upper, "WHERE", 0);

    std::string where_clause;

    if (join_pos == std::string::npos) {
        std::string table_part;
        if (where_pos == std::string::npos) {
            table_part = trim(from_remainder);
        } else {
            table_part = trim(from_remainder.substr(0, where_pos));
            where_clause = trim(from_remainder.substr(where_pos + std::strlen("WHERE")));
        }

        if (!is_identifier(table_part)) {
            error = "invalid table name in FROM";
            return false;
        }

        plan.base_table = to_lower(table_part);
        plan.has_join = false;
        plan.touched_tables = {plan.base_table};
    } else {
        std::string left_table_part = trim(from_remainder.substr(0, join_pos));
        if (!is_identifier(left_table_part)) {
            error = "invalid left table in INNER JOIN";
            return false;
        }
        plan.base_table = to_lower(left_table_part);
        plan.has_join = true;

        std::string join_remainder = trim(from_remainder.substr(join_pos + std::strlen("INNER JOIN")));
        std::string join_upper = to_upper(join_remainder);
        std::size_t on_pos = find_keyword(join_upper, "ON", 0);
        if (on_pos == std::string::npos) {
            error = "INNER JOIN missing ON clause";
            return false;
        }

        std::string right_table_part = trim(join_remainder.substr(0, on_pos));
        if (!is_identifier(right_table_part)) {
            error = "invalid right table in INNER JOIN";
            return false;
        }
        plan.join_table = to_lower(right_table_part);

        std::string on_remainder = trim(join_remainder.substr(on_pos + std::strlen("ON")));
        std::string on_upper = to_upper(on_remainder);
        std::size_t where_on_pos = find_keyword(on_upper, "WHERE", 0);
        std::string join_condition;
        if (where_on_pos == std::string::npos) {
            join_condition = trim(on_remainder);
        } else {
            join_condition = trim(on_remainder.substr(0, where_on_pos));
            where_clause = trim(on_remainder.substr(where_on_pos + std::strlen("WHERE")));
        }

        std::size_t eq = join_condition.find('=');
        if (eq == std::string::npos) {
            error = "join condition must contain '='";
            return false;
        }
        plan.join_left = trim(join_condition.substr(0, eq));
        plan.join_right = trim(join_condition.substr(eq + 1));
        if (plan.join_left.empty() || plan.join_right.empty()) {
            error = "invalid join condition";
            return false;
        }

        plan.touched_tables = {plan.base_table, plan.join_table};
    }

    if (select_part == "*") {
        plan.select_star = true;
    } else {
        plan.select_columns = split_csv(select_part);
        if (plan.select_columns.empty()) {
            error = "empty SELECT projection";
            return false;
        }
        for (std::string& c : plan.select_columns) {
            c = trim(c);
            if (c.empty()) {
                error = "invalid empty projection in SELECT";
                return false;
            }
            std::string tbl;
            std::string col;
            if (split_qualified(c, tbl, col)) {
                if (col.empty() || !is_identifier(col) || (!tbl.empty() && !is_identifier(tbl))) {
                    error = "invalid projected column: " + c;
                    return false;
                }
                c = tbl.empty() ? to_lower(col) : to_lower(tbl) + "." + to_lower(col);
            } else {
                if (!is_identifier(c)) {
                    error = "invalid projected column: " + c;
                    return false;
                }
                c = to_lower(c);
            }
        }
    }

    if (!where_clause.empty()) {
        Condition cond;
        if (!parse_condition(where_clause, cond, error)) {
            return false;
        }
        std::string tbl;
        std::string col;
        split_qualified(cond.column, tbl, col);
        cond.column = tbl.empty() ? to_lower(cond.column) : to_lower(tbl) + "." + to_lower(col);
        plan.where = cond;
    }

    plan.valid = true;
    return true;
}

bool SqlEngine::evaluate_where(const Table& table,
                               const Row& row,
                               std::size_t row_idx,
                               const Condition& condition,
                               std::string* error) const {
    if (!condition.present) {
        return true;
    }

    std::string cond_table;
    std::string cond_col;
    split_qualified(condition.column, cond_table, cond_col);
    std::string raw_col = cond_col.empty() ? condition.column : cond_col;

    if (!cond_table.empty() && cond_table != table.name) {
        if (error != nullptr) {
            *error = "WHERE references unknown table: " + cond_table;
        }
        return false;
    }

    std::size_t idx = 0;
    std::string lookup_error;
    const Column* col = lookup_column(table, raw_col, idx, lookup_error);
    if (col == nullptr) {
        if (error != nullptr) {
            *error = lookup_error;
        }
        return false;
    }

    bool result = false;
    const std::string rhs = unquote_literal(condition.value);
    double rhs_num = 0.0;
    double lhs_num = 0.0;
    if ((col->type == DataType::kInt || col->type == DataType::kDecimal || col->type == DataType::kDatetime) &&
        row_numeric_value(table, row_idx, idx, lhs_num) &&
        parse_numeric_literal(rhs, col->type, rhs_num)) {
        if (!eval_numeric_op(lhs_num, rhs_num, condition.op, result)) {
            if (error != nullptr) {
                *error = "unsupported operator in WHERE: " + condition.op;
            }
            return false;
        }
        return result;
    }

    std::string cmp_error;
    if (!compare_values(row.values[idx], rhs, col->type, condition.op, result, cmp_error)) {
        if (error != nullptr) {
            *error = cmp_error;
        }
        return false;
    }
    return result;
}

bool SqlEngine::evaluate_where_join(const Table& left,
                                    const Row& left_row,
                                    std::size_t left_row_idx,
                                    const Table& right,
                                    const Row& right_row,
                                    std::size_t right_row_idx,
                                    const Condition& condition,
                                    std::string* error) const {
    if (!condition.present) {
        return true;
    }

    std::string cond_table;
    std::string cond_col;
    split_qualified(condition.column, cond_table, cond_col);
    std::string raw_col = cond_col.empty() ? condition.column : cond_col;

    const Table* table = nullptr;
    const Row* row = nullptr;
    std::size_t row_idx = 0;

    if (cond_table.empty()) {
        std::size_t dummy = 0;
        std::string err1;
        std::string err2;
        bool in_left = lookup_column(left, raw_col, dummy, err1) != nullptr;
        bool in_right = lookup_column(right, raw_col, dummy, err2) != nullptr;
        if (in_left && in_right) {
            if (error != nullptr) {
                *error = "ambiguous column in WHERE: " + raw_col;
            }
            return false;
        }
        if (!in_left && !in_right) {
            if (error != nullptr) {
                *error = "unknown column in WHERE: " + raw_col;
            }
            return false;
        }
        if (in_left) {
            table = &left;
            row = &left_row;
            row_idx = left_row_idx;
        } else {
            table = &right;
            row = &right_row;
            row_idx = right_row_idx;
        }
    } else if (cond_table == left.name) {
        table = &left;
        row = &left_row;
        row_idx = left_row_idx;
    } else if (cond_table == right.name) {
        table = &right;
        row = &right_row;
        row_idx = right_row_idx;
    } else {
        if (error != nullptr) {
            *error = "WHERE references unknown table: " + cond_table;
        }
        return false;
    }

    std::size_t idx = 0;
    std::string lookup_error;
    const Column* col = lookup_column(*table, raw_col, idx, lookup_error);
    if (col == nullptr) {
        if (error != nullptr) {
            *error = lookup_error;
        }
        return false;
    }

    bool result = false;
    const std::string rhs = unquote_literal(condition.value);
    double rhs_num = 0.0;
    double lhs_num = 0.0;
    if ((col->type == DataType::kInt || col->type == DataType::kDecimal || col->type == DataType::kDatetime) &&
        row_numeric_value(*table, row_idx, idx, lhs_num) &&
        parse_numeric_literal(rhs, col->type, rhs_num)) {
        if (!eval_numeric_op(lhs_num, rhs_num, condition.op, result)) {
            if (error != nullptr) {
                *error = "unsupported operator in WHERE: " + condition.op;
            }
            return false;
        }
        return result;
    }

    std::string cmp_error;
    if (!compare_values(row->values[idx], rhs, col->type, condition.op, result, cmp_error)) {
        if (error != nullptr) {
            *error = cmp_error;
        }
        return false;
    }

    return result;
}

std::unordered_map<std::string, std::uint64_t>
SqlEngine::capture_versions(const std::vector<std::string>& table_names) const {
    std::unordered_map<std::string, std::uint64_t> out;
    out.reserve(table_names.size());
    for (const std::string& t : table_names) {
        auto it = tables_.find(t);
        if (it != tables_.end()) {
            out[t] = it->second.version;
        }
    }
    return out;
}

std::string SqlEngine::trim(const std::string& s) {
    std::size_t b = 0;
    while (b < s.size() && std::isspace(static_cast<unsigned char>(s[b]))) {
        ++b;
    }
    std::size_t e = s.size();
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) {
        --e;
    }
    return s.substr(b, e - b);
}

std::string SqlEngine::to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    return s;
}

std::string SqlEngine::to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return s;
}

std::string SqlEngine::normalize_sql_for_cache(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    bool prev_space = false;
    bool in_single = false;
    bool in_double = false;

    for (char c : s) {
        if (c == '\'' && !in_double) {
            in_single = !in_single;
            out.push_back(c);
            prev_space = false;
            continue;
        }
        if (c == '"' && !in_single) {
            in_double = !in_double;
            out.push_back(c);
            prev_space = false;
            continue;
        }

        if (!in_single && !in_double && std::isspace(static_cast<unsigned char>(c))) {
            if (!prev_space) {
                out.push_back(' ');
                prev_space = true;
            }
            continue;
        }
        prev_space = false;

        if (!in_single && !in_double) {
            out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
        } else {
            out.push_back(c);
        }
    }

    out = trim(out);
    if (!out.empty() && out.back() == ';') {
        out.pop_back();
        out = trim(out);
    }
    return out;
}

bool SqlEngine::is_identifier(const std::string& s) {
    if (s.empty()) {
        return false;
    }
    if (!(std::isalpha(static_cast<unsigned char>(s[0])) || s[0] == '_')) {
        return false;
    }
    for (char c : s) {
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) {
            return false;
        }
    }
    return true;
}

std::vector<std::string> SqlEngine::split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::string token;
    int depth = 0;
    bool in_single = false;
    bool in_double = false;

    for (std::size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '\'' && !in_double) {
            if (i + 1 < s.size() && s[i + 1] == '\'') {
                token.push_back(c);
                token.push_back(s[++i]);
                continue;
            }
            in_single = !in_single;
            token.push_back(c);
            continue;
        }
        if (c == '"' && !in_single) {
            in_double = !in_double;
            token.push_back(c);
            continue;
        }

        if (!in_single && !in_double) {
            if (c == '(') {
                ++depth;
            } else if (c == ')') {
                --depth;
            } else if (c == ',' && depth == 0) {
                out.push_back(trim(token));
                token.clear();
                continue;
            }
        }
        token.push_back(c);
    }

    if (!token.empty() || !s.empty()) {
        out.push_back(trim(token));
    }

    return out;
}

std::string SqlEngine::unquote_literal(const std::string& s) {
    std::size_t b = 0;
    while (b < s.size() && is_space_char(s[b])) ++b;
    std::size_t e = s.size();
    while (e > b && is_space_char(s[e - 1])) --e;

    if (e - b < 2) {
        return std::string(s.data() + b, e - b);
    }

    const char front = s[b];
    const char back  = s[e - 1];
    if ((front == '\'' && back == '\'') || (front == '"' && back == '"')) {
        char q = front;
        std::string out;
        out.reserve(e - b - 2);
        for (std::size_t i = b + 1; i < e - 1; ++i) {
            if (s[i] == q && i + 1 < e - 1 && s[i + 1] == q) {
                out.push_back(q);
                ++i;
                continue;
            }
            out.push_back(s[i]);
        }
        return out;
    }

    return std::string(s.data() + b, e - b);
}

std::int64_t SqlEngine::now_unix() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

bool SqlEngine::parse_datetime_to_unix(const std::string& s, std::int64_t& out) {
    std::tm tm{};
    std::istringstream iss(s);
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (iss.fail()) {
        iss.clear();
        iss.str(s);
        iss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
        if (iss.fail()) {
            return false;
        }
    }

    tm.tm_isdst = -1;
    std::time_t tt = std::mktime(&tm);
    if (tt == static_cast<std::time_t>(-1)) {
        return false;
    }

    out = static_cast<std::int64_t>(tt);
    return true;
}

bool SqlEngine::fast_parse_int64(const std::string& s, std::int64_t& out) {
    if (s.empty()) {
        return false;
    }
    const char* begin = s.data();
    const char* end = s.data() + s.size();
    while (begin < end && is_space_char(*begin)) {
        ++begin;
    }
    while (end > begin && is_space_char(*(end - 1))) {
        --end;
    }
    if (begin == end) {
        return false;
    }
    auto parsed = std::from_chars(begin, end, out, 10);
    return parsed.ec == std::errc{} && parsed.ptr == end;
}

bool SqlEngine::fast_parse_double(const std::string& s, double& out) {
    if (s.empty()) {
        return false;
    }

    const char* begin = s.data();
    const char* endp = s.data() + s.size();
    while (begin < endp && is_space_char(*begin)) {
        ++begin;
    }
    while (endp > begin && is_space_char(*(endp - 1))) {
        --endp;
    }
    if (begin == endp) {
        return false;
    }

    auto parsed_fast = std::from_chars(begin, endp, out, std::chars_format::general);
    if (parsed_fast.ec == std::errc{} && parsed_fast.ptr == endp) {
        return true;
    }

    std::string tmp(begin, endp);
    char* end = nullptr;
    errno = 0;
    double parsed = std::strtod(tmp.c_str(), &end);
    if (errno == ERANGE || end == tmp.c_str() || *end != '\0') {
        return false;
    }
    out = parsed;
    return true;
}

bool SqlEngine::is_null_literal_ci(const std::string& s) {
    std::size_t b = 0;
    std::size_t e = s.size();
    while (b < e && is_space_char(s[b])) {
        ++b;
    }
    while (e > b && is_space_char(s[e - 1])) {
        --e;
    }
    if (e - b != 4) {
        return false;
    }
    return std::toupper(static_cast<unsigned char>(s[b])) == 'N' &&
           std::toupper(static_cast<unsigned char>(s[b + 1])) == 'U' &&
           std::toupper(static_cast<unsigned char>(s[b + 2])) == 'L' &&
           std::toupper(static_cast<unsigned char>(s[b + 3])) == 'L';
}

bool SqlEngine::parse_numeric_literal(const std::string& s, DataType type, double& out) {
    if (is_null_literal_ci(s)) {
        return false;
    }

    if (type == DataType::kInt) {
        std::int64_t n = 0;
        if (!fast_parse_int64(s, n)) {
            return false;
        }
        out = static_cast<double>(n);
        return true;
    }
    if (type == DataType::kDecimal) {
        return fast_parse_double(s, out);
    }
    if (type == DataType::kDatetime) {
        std::int64_t ts = 0;
        if (!parse_datetime_to_unix(trim(s), ts)) {
            return false;
        }
        out = static_cast<double>(ts);
        return true;
    }
    return false;
}

bool SqlEngine::eval_numeric_op(double lhs, double rhs, const std::string& op, bool& out) {
    if (op == "=") {
        out = (lhs == rhs);
        return true;
    }
    if (op == "!=") {
        out = (lhs != rhs);
        return true;
    }
    if (op == "<") {
        out = (lhs < rhs);
        return true;
    }
    if (op == "<=") {
        out = (lhs <= rhs);
        return true;
    }
    if (op == ">") {
        out = (lhs > rhs);
        return true;
    }
    if (op == ">=") {
        out = (lhs >= rhs);
        return true;
    }
    return false;
}

bool SqlEngine::compare_values(const std::string& lhs,
                               const std::string& rhs,
                               DataType type,
                               const std::string& op,
                               bool& out,
                               std::string& error) {
    const bool lhs_is_null = is_null_literal_ci(lhs);
    const bool rhs_is_null = is_null_literal_ci(rhs);
    if (lhs_is_null || rhs_is_null) {
        if (op == "=") {
            out = lhs_is_null && rhs_is_null;
            return true;
        }
        if (op == "!=") {
            out = lhs_is_null != rhs_is_null;
            return true;
        }
        out = false;
        return true;
    }

    auto apply_op = [&](auto a, auto b) {
        if (op == "=") {
            out = (a == b);
        } else if (op == "!=") {
            out = (a != b);
        } else if (op == "<") {
            out = (a < b);
        } else if (op == "<=") {
            out = (a <= b);
        } else if (op == ">") {
            out = (a > b);
        } else if (op == ">=") {
            out = (a >= b);
        } else {
            error = "unsupported operator in WHERE: " + op;
            return false;
        }
        return true;
    };

    if (type == DataType::kInt) {
        std::int64_t a = 0;
        std::int64_t b = 0;
        if (!fast_parse_int64(lhs, a) || !fast_parse_int64(rhs, b)) {
            error = "invalid integer comparison value";
            return false;
        }
        return apply_op(a, b);
    }

    if (type == DataType::kDecimal) {
        double a = 0.0;
        double b = 0.0;
        if (!fast_parse_double(lhs, a) || !fast_parse_double(rhs, b)) {
            error = "invalid decimal comparison value";
            return false;
        }
        return apply_op(a, b);
    }

    if (type == DataType::kDatetime) {
        std::int64_t a = 0;
        std::int64_t b = 0;
        if (!parse_datetime_to_unix(lhs, a) || !parse_datetime_to_unix(rhs, b)) {
            return apply_op(lhs, rhs);
        }
        return apply_op(a, b);
    }

    return apply_op(lhs, rhs);
}

bool SqlEngine::parse_condition(const std::string& text, Condition& out, std::string& error) {
    std::string s = trim(text);
    if (s.empty()) {
        error = "invalid WHERE clause; expected a single condition";
        return false;
    }

    bool in_single = false;
    bool in_double = false;
    std::size_t op_pos = std::string::npos;
    std::string op;

    for (std::size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '\'' && !in_double) {
            in_single = !in_single;
            continue;
        }
        if (c == '"' && !in_single) {
            in_double = !in_double;
            continue;
        }
        if (in_single || in_double) {
            continue;
        }

        if (i + 1 < s.size()) {
            if (s[i] == '!' && s[i + 1] == '=') {
                op_pos = i;
                op = "!=";
                break;
            }
            if (s[i] == '<' && s[i + 1] == '=') {
                op_pos = i;
                op = "<=";
                break;
            }
            if (s[i] == '>' && s[i + 1] == '=') {
                op_pos = i;
                op = ">=";
                break;
            }
        }

        if (c == '=' || c == '<' || c == '>') {
            op_pos = i;
            op.assign(1, c);
            break;
        }
    }

    if (op_pos == std::string::npos) {
        error = "invalid WHERE clause; expected a single condition";
        return false;
    }

    std::string lhs = trim(s.substr(0, op_pos));
    std::string rhs = trim(s.substr(op_pos + op.size()));
    if (lhs.empty() || rhs.empty()) {
        error = "invalid WHERE clause; expected a single condition";
        return false;
    }

    std::string tbl;
    std::string col;
    if (!split_qualified(lhs, tbl, col)) {
        error = "invalid WHERE clause; expected a valid column reference";
        return false;
    }
    const std::string raw_col = col.empty() ? lhs : col;
    if (!is_identifier(raw_col) || (!tbl.empty() && !is_identifier(tbl))) {
        error = "invalid WHERE clause; expected a valid column reference";
        return false;
    }

    out.present = true;
    out.column = lhs;
    out.op = op;
    out.value = rhs;
    return true;
}

bool SqlEngine::split_qualified(const std::string& qualified,
                                std::string& table,
                                std::string& column) {
    table.clear();
    column.clear();
    std::size_t dot = qualified.find('.');
    if (dot == std::string::npos) {
        column = qualified;
        return true;
    }

    if (qualified.find('.', dot + 1) != std::string::npos) {
        return false;
    }

    table = trim(qualified.substr(0, dot));
    column = trim(qualified.substr(dot + 1));
    return !table.empty() && !column.empty();
}

bool SqlEngine::starts_with_keyword(const std::string& sql_upper, const std::string& kw) {
    if (sql_upper.rfind(kw, 0) != 0) {
        return false;
    }
    if (sql_upper.size() == kw.size()) {
        return true;
    }
    return std::isspace(static_cast<unsigned char>(sql_upper[kw.size()])) != 0;
}

std::size_t SqlEngine::find_keyword(const std::string& sql_upper,
                                    const std::string& kw,
                                    std::size_t start) {
    bool in_single = false;
    bool in_double = false;

    for (std::size_t i = start; i + kw.size() <= sql_upper.size(); ++i) {
        char c = sql_upper[i];
        if (c == '\'' && !in_double) {
            in_single = !in_single;
            continue;
        }
        if (c == '"' && !in_single) {
            in_double = !in_double;
            continue;
        }
        if (in_single || in_double) {
            continue;
        }
        if (sql_upper.compare(i, kw.size(), kw) != 0) {
            continue;
        }

        bool left_ok = (i == 0) || std::isspace(static_cast<unsigned char>(sql_upper[i - 1])) ||
                       sql_upper[i - 1] == ')';
        bool right_ok = (i + kw.size() == sql_upper.size()) ||
                        std::isspace(static_cast<unsigned char>(sql_upper[i + kw.size()])) ||
                        sql_upper[i + kw.size()] == '(';
        if (left_ok && right_ok) {
            return i;
        }
    }

    return std::string::npos;
}

bool SqlEngine::row_alive(const Row& row, std::int64_t now_ts) {
    return row.expires_at_unix == 0 || row.expires_at_unix > now_ts;
}

bool SqlEngine::row_numeric_value(const Table& table,
                                  std::size_t row_idx,
                                  std::size_t col_idx,
                                  double& out) {
    if (col_idx >= table.numeric_column_valid.size() || col_idx >= table.numeric_column_values.size()) {
        return false;
    }
    const auto& valid = table.numeric_column_valid[col_idx];
    const auto& values = table.numeric_column_values[col_idx];
    if (row_idx >= valid.size() || row_idx >= values.size() || valid[row_idx] == 0U) {
        return false;
    }

    out = values[row_idx];
    return true;
}

bool SqlEngine::validate_typed_value(const Column& col,
                                     std::string& value,
                                     std::string& error,
                                     double* numeric_out,
                                     bool* numeric_valid) const {
    if (numeric_valid != nullptr) {
        *numeric_valid = false;
    }
    trim_in_place(value);
    if (is_null_literal_ci(value)) {
        if (col.not_null || col.primary_key) {
            error = "NULL is not allowed for column " + col.name;
            return false;
        }
        return true;
    }

    if (col.type == DataType::kInt) {
        std::int64_t parsed = 0;
        if (!fast_parse_int64(value, parsed)) {
            error = "invalid INT value for column " + col.name;
            return false;
        }
        if (numeric_out != nullptr) {
            *numeric_out = static_cast<double>(parsed);
        }
        if (numeric_valid != nullptr) {
            *numeric_valid = true;
        }
        return true;
    }

    if (col.type == DataType::kDecimal) {
        double parsed = 0.0;
        if (!fast_parse_double(value, parsed)) {
            error = "invalid DECIMAL value for column " + col.name;
            return false;
        }
        if (numeric_out != nullptr) {
            *numeric_out = parsed;
        }
        if (numeric_valid != nullptr) {
            *numeric_valid = true;
        }
        return true;
    }

    if (col.type == DataType::kDatetime) {
        std::int64_t parsed = 0;
        if (!parse_datetime_to_unix(value, parsed)) {
            error = "invalid DATETIME for column " + col.name + " (use YYYY-MM-DD HH:MM:SS)";
            return false;
        }
        if (numeric_out != nullptr) {
            *numeric_out = static_cast<double>(parsed);
        }
        if (numeric_valid != nullptr) {
            *numeric_valid = true;
        }
        return true;
    }

    if (col.varchar_limit > 0 && static_cast<int>(value.size()) > col.varchar_limit) {
        error = "VARCHAR length exceeded for column " + col.name;
        return false;
    }

    return true;
}

const SqlEngine::Column* SqlEngine::lookup_column(const Table& table,
                                                   const std::string& col_ref,
                                                   std::size_t& idx,
                                                   std::string& error) const {
    // Fast path: check if already lowercase (common case)
    auto it = table.column_index.find(col_ref);
    if (it == table.column_index.end()) {
        std::string key = to_lower(col_ref);
        it = table.column_index.find(key);
        if (it == table.column_index.end()) {
            error = "unknown column " + col_ref + " in table " + table.name;
            return nullptr;
        }
    }
    idx = it->second;
    return &table.columns[idx];
}

}  // namespace flexql
