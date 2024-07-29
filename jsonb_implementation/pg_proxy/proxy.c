#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <stdint.h>
#include <ev.h>
#include "postgres.h"
#include "postmaster/bgworker.h"
#include "fmgr.h"
#include "postgresql/libpq-fe.h"
#include <json-c/json.h>
#include <libbson-1.0/bson.h>
#include "catalog/pg_type.h"
#include "coro.h"
#include "config.h"


#ifdef PG_MODULE_MAGIC

PG_MODULE_MAGIC;
#endif

#define MONGO_PORT 3391
#define BUFFER_SIZE 1024 * 3
#define OP_QUERY 2004
#define OP_REPLY 1
#define OP_MSG 2013
#define PING_ENDSESSIONS_REPLY_LEN 38
#define INSERT_DELETE_REPLY_LEN 45
#define UPDATE_REPLY_LEN 60
#define PG_CONNINFO "dbname=postgres user=user1 password=passwd host=localhost port=5433"


#define BODY_MSG_SECTION_TYPE 0
#define DOC_MSG_SECTION_TYPE 1

#define MAX_BSON_OBJECTS 10

#define STACK_SIZE (1024 * 1024)  // 1 MB


PGDLLEXPORT int main_proxy(void);

void accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);

void read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);

void write_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);

//void process_message(uint32_t response_to, unsigned char *buffer, char *json_metadata, char *json_data_array, int *flag);

void
process_message(uint32_t response_to, unsigned char *buffer, char *json_metadata, char *json_data_array,
                int *flag, struct json_object **results, char **dbname, char **collection, int *changed_count);

void parse_mongodb_packet(char *buffer, char **query_string, char **parameter_string);

int parse_message(char *buffer, char **query_string, char **parameter_string);

int parse_bson_object(char *my_data, bson_t **my_bson);

ssize_t get_str_len_from_doc_seq(char *doc_seq);

bool check_and_create_database(PGconn *conn, const char *dbname);

bool check_and_create_table(PGconn *conn, const char *table_name);

bool column_exists(PGconn *conn, const char *table_name, const char *column_name);

const char *get_json_value_as_string(struct json_object *field_value);

bool execute_insert_queries(PGconn *conn, const char *table_name, struct json_object *data_array, int *inserted_count);

bool execute_query_insert_to_postgres(const char *json_metadata, const char *json_data_array, int *inserted_count);

bool execute_delete_queries(PGconn *conn, const char *table_name, struct json_object *delete_array, int *deleted_count);

bool execute_query_delete_to_postgres(const char *json_metadata, const char *json_data_array, int *deleted_count);

bool execute_update_queries(PGconn *conn, const char *table_name, struct json_object *update_array, int *updated_count);

bool execute_query_update_to_postgres(const char *json_metadata, const char *json_data_array, int *updated_count);

//bool execute_query_find_to_postgres(const char *json_metadata, struct json_object **results);
bool execute_query_find_to_postgres(const char *json_metadata, struct json_object **results, const char **collection,
                                    const char **dbname);

void cleanup_and_exit(struct ev_loop *loop, int server_sd);

static void handle_sigterm(SIGNAL_ARGS, int server_sd);

void random_new_req_id(unsigned char *buffer);

void modify_insert_delete_reply(unsigned char *reply, u_int32_t response_to, int n);

void modify_ping_endsessions_reply(unsigned char *reply, u_int32_t response_to);

void modify_update_reply(unsigned char *reply, u_int32_t response_to, int nmodified);

int get_type_of_value(struct json_object *field_value);

int generate_subelemets_string(struct json_object *single_json, char *reply);

int generate_first_batch_element_i(struct json_object *single_json, char *reply, int number_of_el);

int generate_first_batch(struct json_object *data_array, char *reply);

void random_new_req_id(unsigned char *buffer);

int generate_find_reply_packet(struct json_object *data_array, char *reply, uint32_t response_to, char *db_name,
                               char *table_name);

int generate_cursor(struct json_object *data_array, char *reply, char *db_name, char *table_name);

int generate_ns_element(char *reply, char *db_name, char *table_name);

static void handle_sigterm(SIGNAL_ARGS, int server_sd) {
    if (server_sd >= 0) {
        close(server_sd);
    }
    exit(0);
}

bool check_and_create_database(PGconn *conn, const char *dbname) {
    char query[BUFFER_SIZE];
    snprintf(query, sizeof(query), "SELECT 1 FROM pg_database WHERE datname='%s'", dbname);

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return false;
    }

    //ALTER USER user1 CREATEDB;
    if (PQntuples(res) == 0) {
        PQclear(res);
        snprintf(query, sizeof(query), "CREATE DATABASE %s", dbname);
        res = PQexec(conn, query);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            PQclear(res);
            return false;
        }
    }

    PQclear(res);
    return true;
}

bool check_and_create_table(PGconn *conn, const char *table_name) {
    char query[BUFFER_SIZE];
    snprintf(query, sizeof(query),
             "CREATE TABLE IF NOT EXISTS %s ("
             "_id SERIAL PRIMARY KEY, "
             "data JSONB)",
             table_name);

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Table creation failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}

const char *get_json_value_as_string(struct json_object *field_value) {
    if (json_object_is_type(field_value, json_type_string)) {
        return json_object_get_string(field_value);
    } else if (json_object_is_type(field_value, json_type_int)) {
        return json_object_to_json_string(field_value);
    } else if (json_object_is_type(field_value, json_type_boolean)) {
        return json_object_to_json_string(field_value);
    } else if (json_object_is_type(field_value, json_type_double)) {
        return json_object_to_json_string(field_value);
    }
    return NULL;
}

bool execute_insert_queries(PGconn *conn, const char *table_name, struct json_object *data_array, int *inserted_count) {
    int array_length = json_object_array_length(data_array);
    *inserted_count = 0;

    for (int i = 0; i < array_length; i++) {
        struct json_object *data_json = json_object_array_get_idx(data_array, i);

        // Remove _id field if present
        json_object_object_del(data_json, "_id");

        // Convert JSON object to string
        const char *json_str = json_object_to_json_string(data_json);

        // Construct SQL query for insertion into jsonb column
        char query[BUFFER_SIZE];
        snprintf(query, sizeof(query), "INSERT INTO %s (data) VALUES ('%s'::jsonb) RETURNING _id", table_name,
                 json_str);


        // Send the query asynchronously
        if (!PQsendQuery(conn, query)) {
            fprintf(stderr, "Failed to send query: %s\n", PQerrorMessage(conn));
            return false;
        }

        // Wait for the query to complete
        while (PQisBusy(conn)) {
            if (!PQconsumeInput(conn)) {
                fprintf(stderr, "Failed to consume input: %s\n", PQerrorMessage(conn));
                return false;
            }
        }

        // Get the result
        PGresult *res = PQgetResult(conn);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "UPDATE command failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            return false;
        }

        (*inserted_count)++;
        PQclear(res);
    }
    return true;
}


bool execute_query_insert_to_postgres(const char *json_metadata, const char *json_data_array, int *inserted_count) {
    // Connect to database
    PGconn *conn = PQconnectdb(PG_CONNINFO);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }

    // Parse metadata JSON
    struct json_object *metadata_json = json_tokener_parse(json_metadata);
    if (!metadata_json) {
        fprintf(stderr, "Failed to parse metadata JSON\n");
        PQfinish(conn);
        return false;
    }

    struct json_object *insert_obj, *db_obj;
    if (!json_object_object_get_ex(metadata_json, "insert", &insert_obj) ||
        !json_object_object_get_ex(metadata_json, "$db", &db_obj)) {
        fprintf(stderr, "Invalid metadata JSON format\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    const char *collection = json_object_get_string(insert_obj);
    const char *dbname = json_object_get_string(db_obj);

    // Check and create database if it does not exist
    if (!check_and_create_database(conn, dbname)) {
        fprintf(stderr, "Failed to create or check database\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    PQfinish(conn);

    // Connect to specified database
    char conninfo[BUFFER_SIZE];
    snprintf(conninfo, sizeof(conninfo), "dbname=%s user=user1 password=passwd port=5433", dbname);
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database %s failed: %s", dbname, PQerrorMessage(conn));
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    // Check and create table if it does not exist
    if (!check_and_create_table(conn, collection)) {
        fprintf(stderr, "Failed to create or check table\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    // Parse data JSON array
    struct json_object *data_array = json_tokener_parse(json_data_array);
    if (!data_array || json_object_get_type(data_array) != json_type_array) {
        fprintf(stderr, "Failed to parse data JSON array\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    // Execute insert queries
    if (!execute_insert_queries(conn, collection, data_array, inserted_count)) {
        fprintf(stderr, "Failed to execute insert queries\n");
        json_object_put(metadata_json);
        json_object_put(data_array);
        PQfinish(conn);
        return false;
    }

    // Clean up
    json_object_put(metadata_json);
    json_object_put(data_array);
    PQfinish(conn);

    return true;
}

bool column_exists(PGconn *conn, const char *table_name, const char *column_name) {
    char query[BUFFER_SIZE];
    snprintf(query, sizeof(query),
             "SELECT column_name FROM information_schema.columns WHERE table_name='%s' AND column_name='%s'",
             table_name, column_name);

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return false;
    }

    bool exists = PQntuples(res) > 0;
    PQclear(res);
    return exists;
}

bool
execute_delete_queries(PGconn *conn, const char *table_name, struct json_object *delete_array, int *deleted_count) {
    int array_length = json_object_array_length(delete_array);
    *deleted_count = 0;

    for (int i = 0; i < array_length; i++) {
        struct json_object *delete_json = json_object_array_get_idx(delete_array, i);
        struct json_object *q_json, *limit_json;

        if (!json_object_object_get_ex(delete_json, "q", &q_json) ||
            !json_object_object_get_ex(delete_json, "limit", &limit_json)) {
            fprintf(stderr, "Invalid delete JSON format\n");
            return false;
        }

        const char *q_str = json_object_to_json_string_ext(q_json, JSON_C_TO_STRING_PLAIN);
        int limit = json_object_get_int(limit_json);

        // Construct the JSONPath condition dynamically
        char jsonpath_condition[BUFFER_SIZE] = "$.** ? (";
        json_object_object_foreach(q_json, key, val)
        {
            char condition_part[BUFFER_SIZE];
            snprintf(condition_part, sizeof(condition_part), "@.%s == \"%s\" && ", key, json_object_get_string(val));
            strncat(jsonpath_condition, condition_part, sizeof(jsonpath_condition) - strlen(jsonpath_condition) - 1);
        }

        // Remove the trailing " && " and close the condition
        jsonpath_condition[strlen(jsonpath_condition) - 4] = '\0';
        strncat(jsonpath_condition, ")", sizeof(jsonpath_condition) - strlen(jsonpath_condition) - 1);

        // Construct the full query string
        char query[BUFFER_SIZE];
        if (limit == 0) {
            snprintf(query, sizeof(query), "DELETE FROM %s WHERE jsonb_path_exists(data, '%s')", table_name,
                     jsonpath_condition);
        } else {
            snprintf(query, sizeof(query),
                     "WITH del AS (SELECT ctid FROM %s WHERE jsonb_path_exists(data, '%s') LIMIT %d) "
                     "DELETE FROM %s WHERE ctid IN (SELECT ctid FROM del)",
                     table_name, jsonpath_condition, limit, table_name);
        }

        // Send the query asynchronously
        if (!PQsendQuery(conn, query)) {
            fprintf(stderr, "Failed to send query: %s\n", PQerrorMessage(conn));
            return false;
        }

        // Wait for the query to complete
        while (PQisBusy(conn)) {
            if (!PQconsumeInput(conn)) {
                fprintf(stderr, "Failed to consume input: %s\n", PQerrorMessage(conn));
                return false;
            }
        }

        // Get the result
        PGresult *res = PQgetResult(conn);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "UPDATE command failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            return false;
        }

        *deleted_count += atoi(PQcmdTuples(res));
        PQclear(res);
    }
    return true;
}

bool execute_query_delete_to_postgres(const char *json_metadata, const char *json_data_array, int *deleted_count) {
    PGconn *conn = PQconnectdb(PG_CONNINFO);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }

    struct json_object *metadata_json = json_tokener_parse(json_metadata);
    if (!metadata_json) {
        fprintf(stderr, "Failed to parse metadata JSON\n");
        PQfinish(conn);
        return false;
    }

    struct json_object *delete_obj, *db_obj;
    if (!json_object_object_get_ex(metadata_json, "delete", &delete_obj) ||
        !json_object_object_get_ex(metadata_json, "$db", &db_obj)) {
        fprintf(stderr, "Invalid metadata JSON format\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    const char *collection = json_object_get_string(delete_obj);
    const char *dbname = json_object_get_string(db_obj);

    if (!check_and_create_database(conn, dbname)) {
        fprintf(stderr, "Failed to create or check database\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    PQfinish(conn);

    char conninfo[BUFFER_SIZE];
    snprintf(conninfo, sizeof(conninfo), "dbname=%s user=user1 password=passwd port=5433", dbname);
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database %s failed: %s", dbname, PQerrorMessage(conn));
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    if (!check_and_create_table(conn, collection)) {
        fprintf(stderr, "Failed to create or check table\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    struct json_object *delete_array = json_tokener_parse(json_data_array);
    if (!delete_array || json_object_get_type(delete_array) != json_type_array) {
        fprintf(stderr, "Failed to parse delete JSON array\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    if (!execute_delete_queries(conn, collection, delete_array, deleted_count)) {
        fprintf(stderr, "Failed to execute delete queries\n");
        json_object_put(metadata_json);
        json_object_put(delete_array);
        PQfinish(conn);
        return false;
    }

    json_object_put(metadata_json);
    json_object_put(delete_array);
    PQfinish(conn);

    return true;
}

bool
execute_update_queries(PGconn *conn, const char *table_name, struct json_object *update_array, int *updated_count) {
    int array_length = json_object_array_length(update_array);
    *updated_count = 0;

    for (int i = 0; i < array_length; i++) {
        struct json_object *update_json = json_object_array_get_idx(update_array, i);
        struct json_object *q_json, *u_json, *multi_json;

        if (!json_object_object_get_ex(update_json, "q", &q_json) ||
            !json_object_object_get_ex(update_json, "u", &u_json)) {
            fprintf(stderr, "Invalid update JSON format\n");
            return false;
        }

        const char *q_str = json_object_to_json_string(q_json);
        struct json_object *set_json;
        if (!json_object_object_get_ex(u_json, "$set", &set_json)) {
            fprintf(stderr, "Invalid update JSON format\n");
            return false;
        }

        char set_clause[BUFFER_SIZE] = "data";
        struct json_object_iterator it = json_object_iter_begin(set_json);
        struct json_object_iterator it_end = json_object_iter_end(set_json);

        char jsonb_set_clause[BUFFER_SIZE] = "";
        while (!json_object_iter_equal(&it, &it_end)) {
            const char *field_name = json_object_iter_peek_name(&it);
            struct json_object *field_value = json_object_iter_peek_value(&it);

            strcat(jsonb_set_clause, "jsonb_set(");
            strcat(jsonb_set_clause, set_clause);
            strcat(jsonb_set_clause, ", '{");
            strcat(jsonb_set_clause, field_name);
            strcat(jsonb_set_clause, "}', '");
            strcat(jsonb_set_clause, json_object_to_json_string(field_value));
            strcat(jsonb_set_clause, "'::jsonb), ");

            json_object_iter_next(&it);
        }

        // Remove the last ", "
        jsonb_set_clause[strlen(jsonb_set_clause) - 2] = '\0';

        char query[BUFFER_SIZE];
        if (json_object_object_get_ex(update_json, "multi", &multi_json) && json_object_get_boolean(multi_json)) {
            snprintf(query, sizeof(query),
                     "UPDATE %s SET data = %s WHERE data @> '%s'::jsonb",
                     table_name, jsonb_set_clause, q_str);
        } else {
            snprintf(query, sizeof(query),
                     "UPDATE %s SET data = %s WHERE ctid IN (SELECT ctid FROM %s WHERE data @> '%s'::jsonb LIMIT 1)",
                     table_name, jsonb_set_clause, table_name, q_str);
        }


        // Send the query asynchronously
        if (!PQsendQuery(conn, query)) {
            fprintf(stderr, "Failed to send query: %s\n", PQerrorMessage(conn));
            return false;
        }

        // Wait for the query to complete
        while (PQisBusy(conn)) {
            if (!PQconsumeInput(conn)) {
                fprintf(stderr, "Failed to consume input: %s\n", PQerrorMessage(conn));
                return false;
            }
        }

        // Get the result
        PGresult *res = PQgetResult(conn);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "UPDATE command failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            return false;
        }

        *updated_count += atoi(PQcmdTuples(res));
        PQclear(res);
    }
    return true;
}

bool execute_query_update_to_postgres(const char *json_metadata, const char *json_data_array, int *updated_count) {
    PGconn *conn = PQconnectdb(PG_CONNINFO);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }

    struct json_object *metadata_json = json_tokener_parse(json_metadata);
    if (!metadata_json) {
        fprintf(stderr, "Failed to parse metadata JSON\n");
        PQfinish(conn);
        return false;
    }

    struct json_object *update_obj, *db_obj;
    if (!json_object_object_get_ex(metadata_json, "update", &update_obj) ||
        !json_object_object_get_ex(metadata_json, "$db", &db_obj)) {
        fprintf(stderr, "Invalid metadata JSON format\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    const char *collection = json_object_get_string(update_obj);
    const char *dbname = json_object_get_string(db_obj);

    if (!check_and_create_database(conn, dbname)) {
        fprintf(stderr, "Failed to create or check database\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    PQfinish(conn);

    char conninfo[BUFFER_SIZE];
    snprintf(conninfo, sizeof(conninfo), "dbname=%s user=user1 password=passwd port=5433", dbname);
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database %s failed: %s", dbname, PQerrorMessage(conn));
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    if (!check_and_create_table(conn, collection)) {
        fprintf(stderr, "Failed to create or check table\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    struct json_object *update_array = json_tokener_parse(json_data_array);
    if (!update_array || json_object_get_type(update_array) != json_type_array) {
        fprintf(stderr, "Failed to parse update JSON array\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    if (!execute_update_queries(conn, collection, update_array, updated_count)) {
        fprintf(stderr, "Failed to execute update queries\n");
        json_object_put(metadata_json);
        json_object_put(update_array);
        PQfinish(conn);
        return false;
    }

    json_object_put(metadata_json);
    json_object_put(update_array);
    PQfinish(conn);

    return true;
}

bool
execute_find_query(PGconn *conn, const char *table_name, struct json_object *find_json, struct json_object **results) {
    struct json_object *filter_json, *limit_json;
    char condition[BUFFER_SIZE] = "";
    int limit = -1;
    bool has_nested_field = false;

    // Parse filter conditions from the JSON
    if (json_object_object_get_ex(find_json, "filter", &filter_json)) {
        struct json_object_iterator it = json_object_iter_begin(filter_json);
        struct json_object_iterator it_end = json_object_iter_end(filter_json);

        // Check if there are any nested fields
        while (!json_object_iter_equal(&it, &it_end)) {
            const char *field_name = json_object_iter_peek_name(&it);
            if (strchr(field_name, '.')) {
                has_nested_field = true;
                break;
            }
            json_object_iter_next(&it);
        }

        it = json_object_iter_begin(filter_json);
        if (!has_nested_field) {
            // Simple fields logic
            while (!json_object_iter_equal(&it, &it_end)) {
                const char *field_name = json_object_iter_peek_name(&it);
                struct json_object *field_value = json_object_iter_peek_value(&it);
                const char *value_str = json_object_get_string(field_value);

                strcat(condition, "data->>");
                strcat(condition, "'");
                strcat(condition, field_name);
                strcat(condition, "'");
                strcat(condition, " = '");
                strcat(condition, value_str);
                strcat(condition, "' AND ");

                json_object_iter_next(&it);
            }

            // Remove the last " AND "
            if (strlen(condition) > 0) {
                condition[strlen(condition) - 5] = '\0';
            }
        } else {
            // Nested fields logic
            while (!json_object_iter_equal(&it, &it_end)) {
                const char *field_name = json_object_iter_peek_name(&it);
                struct json_object *field_value = json_object_iter_peek_value(&it);
                const char *value_str = json_object_to_json_string_ext(field_value, JSON_C_TO_STRING_PLAIN);

                char nested_condition[BUFFER_SIZE];
                snprintf(nested_condition, sizeof(nested_condition),
                         "jsonb_path_exists(data, '$.%s ? (@ == %s)'::jsonpath)",
                         field_name, value_str);
                strcat(condition, nested_condition);
                strcat(condition, " AND ");

                json_object_iter_next(&it);
            }

            // Remove the last " AND "
            if (strlen(condition) > 0) {
                condition[strlen(condition) - 5] = '\0';
            }
        }
    }

    // Parse limit from the JSON
    if (json_object_object_get_ex(find_json, "limit", &limit_json)) {
        limit = json_object_get_int(limit_json);
    }

    // Construct the SQL query
    char query[BUFFER_SIZE];
    if (strlen(condition) > 0) {
        if (limit > 0) {
            snprintf(query, sizeof(query), "SELECT * FROM %s WHERE %s LIMIT %d", table_name, condition, limit);
        } else {
            snprintf(query, sizeof(query), "SELECT * FROM %s WHERE %s", table_name, condition);
        }
    } else {
        if (limit > 0) {
            snprintf(query, sizeof(query), "SELECT * FROM %s LIMIT %d", table_name, limit);
        } else {
            snprintf(query, sizeof(query), "SELECT * FROM %s", table_name);
        }
    }



    // Send the query asynchronously
    if (!PQsendQuery(conn, query)) {
        fprintf(stderr, "Failed to send query: %s\n", PQerrorMessage(conn));
        return false;
    }

    // Wait for the query to complete
    while (PQisBusy(conn)) {
        if (!PQconsumeInput(conn)) {
            fprintf(stderr, "Failed to consume input: %s\n", PQerrorMessage(conn));
            return false;
        }
    }

    // Get the result
    PGresult *res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "UPDATE command failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    // Process the query results
    int rows = PQntuples(res);
    *results = json_object_new_array();

    for (int i = 0; i < rows; i++) {
        struct json_object *row_obj = json_object_new_object();
        int n_fields = PQnfields(res);

        for (int j = 0; j < n_fields; j++) {
            const char *field_name = PQfname(res, j);
            const char *field_value = PQgetvalue(res, i, j);

            // Assume all fields are JSONB and parse accordingly
            struct json_object *json_value = json_tokener_parse(field_value);
            json_object_object_add(row_obj, field_name, json_value);
        }
        json_object_array_add(*results, row_obj);
    }

    PQclear(res);
    return true;
}

bool execute_query_find_to_postgres(const char *json_metadata, struct json_object **results, const char **collection,
                                    const char **dbname) {
    PGconn *conn = PQconnectdb(PG_CONNINFO);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }

    struct json_object *metadata_json = json_tokener_parse(json_metadata);
    if (!metadata_json) {
        fprintf(stderr, "Failed to parse metadata JSON\n");
        PQfinish(conn);
        return false;
    }

    struct json_object *find_obj, *db_obj;
    if (!json_object_object_get_ex(metadata_json, "find", &find_obj) ||
        !json_object_object_get_ex(metadata_json, "$db", &db_obj)) {
        fprintf(stderr, "Invalid metadata JSON format\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }


    strcpy(*collection, json_object_get_string(find_obj));
    strcpy(*dbname, json_object_get_string(db_obj));
    elog(WARNING, "EXECUTE_QUERY_FIND_TO_POSTGRES: line: %d dbname: %s collection: %s", __LINE__, *dbname, *collection);

    if (!check_and_create_database(conn, *dbname)) {
        fprintf(stderr, "Failed to create or check database\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    PQfinish(conn);

    char conninfo[BUFFER_SIZE];
    snprintf(conninfo, sizeof(conninfo), "dbname=%s user=user1 password=passwd port=5433", *dbname);
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database %s failed: %s", *dbname, PQerrorMessage(conn));
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    if (!check_and_create_table(conn, *collection)) {
        fprintf(stderr, "Failed to create or check table\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    struct json_object *find_json = json_tokener_parse(json_metadata);
    if (!find_json) {
        fprintf(stderr, "Failed to parse find JSON\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    if (!execute_find_query(conn, *collection, find_json, results)) {
        fprintf(stderr, "Failed to execute find query\n");
        json_object_put(metadata_json);
        json_object_put(find_json);
        PQfinish(conn);
        return false;
    }

    int array_length = json_object_array_length(*results);
    int inserted_count = 0;

    json_object_put(metadata_json);
    json_object_put(find_json);
    PQfinish(conn);

    return true;
}


void
process_message(uint32_t response_to,
                unsigned char *buffer,
                char *json_metadata,
                char *json_data_array,
                int *flag,
                struct json_object **results,
                char **dbname,
                char **collection,
                int *changed_count) {

    if (buffer[18] == 1) {
        *flag = 1;
        elog(WARNING, "ignore");
        memset(buffer, 0, BUFFER_SIZE);
        return;
    }

    if (buffer[26] == 'h') {
        *flag = 1;
        elog(WARNING, "ignore");
        memset(buffer, 0, BUFFER_SIZE);
        return;
    }

    elog(WARNING, "before ping check");
    if (buffer[26] == 'p') {
        *flag = 2;
        elog(WARNING, "ping");
        memset(buffer, 0, BUFFER_SIZE);
        return;
    }

    if (buffer[26] == 'e') {
        *flag = 5;
        elog(WARNING, "end session");
        memset(buffer, 0, BUFFER_SIZE);
        return;
    }

    if (buffer[26] == 'i') {
        int inserted_count = 0;
        if (execute_query_insert_to_postgres(json_metadata, json_data_array, &inserted_count)) {
            elog(WARNING, "Insert to PostgreSQL successful %d", inserted_count);
            *flag = 3;
            memset(buffer, 0, BUFFER_SIZE);
            *changed_count = inserted_count;
            return;
        } else {
            elog(WARNING, "Insert to PostgreSQL failed");
            *flag = 4;
            memset(buffer, 0, BUFFER_SIZE);
            return;
        }
    }
    if (buffer[26] == 'd') {
        int deleted_count = 0;
        if (execute_query_delete_to_postgres(json_metadata, json_data_array, &deleted_count)) {
            elog(WARNING, "Delete from PostgreSQL successful %d", deleted_count);
            *flag = 6;
            memset(buffer, 0, BUFFER_SIZE);
            *changed_count = deleted_count;
            return;
        } else {
            elog(WARNING, "Delete from PostgreSQL failed");
            *flag = 7;
            memset(buffer, 0, BUFFER_SIZE);
            return;
        }
    }
    if (buffer[26] == 'u') {
        int updated_count = 0;
        if (execute_query_update_to_postgres(json_metadata, json_data_array, &updated_count)) {
            elog(WARNING, "Update from PostgreSQL successful %d", updated_count);
            *flag = 8;
            memset(buffer, 0, BUFFER_SIZE);
            *changed_count = updated_count;
            return;
        } else {
            elog(WARNING, "Update from PostgreSQL failed");
            *flag = 9;
            memset(buffer, 0, BUFFER_SIZE);
            return;
        }
    }
    if (buffer[26] == 'f') {
        //struct json_object *results;
        if (execute_query_find_to_postgres(json_metadata, results, collection, dbname)) {
            elog(WARNING, "PROCESS_MESSAGE: line: %d dbname: %s collection: %s", __LINE__, *dbname, *collection);
            *flag = 10;
            fprintf(stderr, "Find query executed successfully. Results:\n%s\n",
                    json_object_to_json_string_ext(*results, JSON_C_TO_STRING_PRETTY));
        } else {
            fprintf(stderr, "Failed to execute find query\n");
        }
        memset(buffer, 0, BUFFER_SIZE);
        //json_object_put(results);
        return;
    }
}


typedef struct {
    struct ev_io io;
    int fd;
    coro_context ctx;
    coro_context main_ctx;
    char stack[STACK_SIZE];
} client_t;


void coroutine_entry_point(void *arg) {
    client_t *client = (client_t *) arg;
    struct ev_loop *loop = ev_default_loop(0);

    ev_io_init(&client->io, read_cb, client->fd, EV_READ);
    ev_io_start(loop, &client->io);

    // Передаем управление обратно в главный контекст
    coro_transfer(&client->ctx, &client->main_ctx);

    // Запускаем цикл событий для обработки клиента
    ev_run(loop, 0);

    close(client->fd);
    free(client);
}

void accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    if (revents & EV_ERROR) {
        perror("got invalid event");
        return;
    }

    int client_sd = accept(watcher->fd, NULL, NULL);
    if (client_sd < 0) {
        perror("accept error");
        return;
    }

    client_t *client = (client_t *) malloc(sizeof(client_t));
    client->fd = client_sd;

    coro_create(&client->ctx, coroutine_entry_point, client, client->stack, STACK_SIZE);
    coro_transfer(&client->main_ctx, &client->ctx);
}


int main_proxy(void) {
    struct ev_loop *loop = ev_default_loop(0);
    int reuseaddr = 1;
    struct sockaddr_in addr;
    struct ev_io w_accept;
    int server_sd = -1;
    if ((server_sd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket error");
        return -1;
    }

    if (setsockopt(server_sd, SOL_SOCKET, SO_REUSEADDR, &reuseaddr, sizeof(reuseaddr)) < 0) {
        perror("setsockopt error");
        close(server_sd);
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(MONGO_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_sd, (struct sockaddr *) &addr, sizeof(addr)) != 0) {
        perror("bind error");
        close(server_sd);
        return -1;
    }

    if (listen(server_sd, 128) < 0) {
        perror("listen error");
        close(server_sd);
        return -1;
    }

    ev_io_init(&w_accept, accept_cb, server_sd, EV_READ);
    ev_io_start(loop, &w_accept);
    //ev_signal_init(&signal_watcher, handle_sigterm, SIGTERM);
    //ev_signal_start(loop, &signal_watcher);

    ev_loop(loop, 0);

    cleanup_and_exit(loop, server_sd);
    return 0;
}

void cleanup_and_exit(struct ev_loop *loop, int server_sd) {
    if (server_sd >= 0) {
        close(server_sd);
    }
    ev_loop_destroy(loop);
    exit(0);
}

/*
void handle_sigterm(struct ev_loop *loop, ev_signal *w, int revents) {
    elog(WARNING, "Received SIGTERM, shutting down...");
    ev_break(loop, EVBREAK_ALL);
}
*/

void read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    unsigned char buffer[BUFFER_SIZE];
    ssize_t read;

    char **query_string;
    char **parameter_string;
    u_int32_t msg_length = 0;
    u_int32_t request_id = 0;
    u_int32_t response_to = 0;
    u_int32_t op_code = 0;
    int flag = 0;

    unsigned char insert_delete_ok[INSERT_DELETE_REPLY_LEN] = "-\000\000\000\213\003\000\000\t\000\000\000\335\a\000"
                                                              "\000\000\000\000\000\000\030\000\000\000\020n\000\002\000\000\000\001ok\000\000\000\000\000\000\000\360?\000";

    unsigned char ping_endsessions_ok[PING_ENDSESSIONS_REPLY_LEN] = "&\000\000\000\216\003\000\000\f\000\000\000"
                                                                    "\335\a\000\000\000\000\000\000\000\021\000\000\000\001ok\000\000\000\000\000\000\000\360?\000";

    unsigned char update_ok[UPDATE_REPLY_LEN] = "<\000\000\000\376\000\000\000\f\000\000\000\335\a\000\000\000\000"
                                                "\000\000\000'\000\000\000\020n\000\001\000\000\000\020nModified\000\001\000\000\000\001ok\000\000\000\000\000"
                                                "\000\000\360?\000";

    unsigned char response[] = "I\001\000\000~\001\000\000\003\000\000\000\001\000\000\000\b\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\001\000\000\000%\001\000\000\bhelloOk\000\001\bismaster\000\001\003topologyVersion\000-\000\000\000\aprocessId\000f\225\335\246B(\\C\202\2468\351\022counter\000\000\000\000\000\000\000\000\000\000\020maxBsonObjectSize\000\000\000\000\001\020maxMessageSizeBytes\000\000l\334\002\020maxWriteBatchSize\000\240\206\001\000\tlocalTime\000\032T\246\271\220\001\000\000\020logicalSessionTimeoutMinutes\000\036\000\000\000\020connectionId\000*\000\000\000\020minWireVersion\000\000\000\000\000\020maxWireVersion\000\025\000\000\000\breadOnly\000\000\001ok\000\000\000\000\000\000\000\360?";
    unsigned char msg_response[] = "&\000\000\000\006\000\000\000\001\000\000\000\335\a\000\000\000\000\000\000\000\021\000\000\000\001ok\000\000\000\000\000\000\000\360?";
    unsigned char ok_query_response[] = "-\000\000\000\a\000\000\000\t\000\000\000\335\a\000\000\000\000\000\000\000\030\000\000\000\020n\000\001\000\000\000\001ok\000\000\000\000\000\000\000\360?";


    if (EV_ERROR & revents) {
        perror("got invalid event");
        return;
    }


    read = recv(watcher->fd, buffer, BUFFER_SIZE, 0);

    if (read < 0) {
        perror("read error");
        return;
    }

    if (read == 0) {
        ev_io_stop(loop, watcher);
        close(watcher->fd);
        free(watcher);
        return;
    }


    msg_length = ((u_int32_t *) buffer)[0];
    request_id = ((u_int32_t *) buffer)[1];
    response_to = ((u_int32_t *) buffer)[2];
    op_code = ((u_int32_t *) buffer)[3];


    switch (op_code) {
        case OP_QUERY:
            //parse_query(buffer);
            elog(WARNING, "send reply");
            random_new_req_id(response);
            send(watcher->fd, response, sizeof(response), 0);
            break;
        case OP_MSG:

            query_string = (char **) malloc(sizeof(char *));
            parameter_string = (char **) malloc(sizeof(char *));
            *query_string = NULL;
            *parameter_string = NULL;

            parse_message(buffer, query_string, parameter_string);

            struct json_object *results;
            char **dbname = (char **) malloc(sizeof(char *));
            *dbname = (char *) malloc(256);
            memset(*dbname, 0, 256);
            char **collection = (char **) malloc(sizeof(char *));
            *collection = (char *) malloc(256);
            memset(*collection, 0, 256);
            int changed_count = 0;
            process_message(request_id, buffer, *query_string, *parameter_string, &flag, &results, dbname, collection,
                            &changed_count);
            if (flag == 2) {
                //REPLY MODIFIED
                elog(WARNING, "send ping");
                modify_ping_endsessions_reply(ping_endsessions_ok, request_id);
                //send(watcher->fd, msg_response, sizeof(msg_response), 0);
                send(watcher->fd, ping_endsessions_ok, PING_ENDSESSIONS_REPLY_LEN, 0);
            }
            if (flag == 3) {
                //REPLY MODIFIED
                elog(WARNING, "send insert");
                modify_insert_delete_reply(insert_delete_ok, request_id, changed_count);
                send(watcher->fd, insert_delete_ok, INSERT_DELETE_REPLY_LEN, 0);
            }
            if (flag == 6) {
                //REPLY MODIFIED
                elog(WARNING, "send delete");
                modify_insert_delete_reply(insert_delete_ok, request_id, changed_count);
                send(watcher->fd, insert_delete_ok, INSERT_DELETE_REPLY_LEN, 0);
            }
            if (flag == 8) {
                //REPLY MODIFIED
                elog(WARNING, "send update");
                modify_update_reply(update_ok, response_to, changed_count);
                send(watcher->fd, update_ok, UPDATE_REPLY_LEN, 0);
            }
            if (flag == 10) {
                //REPLY MODIFIED
                elog(WARNING, "send find");
                unsigned char find_reply[BUFFER_SIZE];
                memset(find_reply, 0, BUFFER_SIZE);
                int find_reply_len = generate_find_reply_packet(results, find_reply, 0x06, *dbname, *collection);
                if (find_reply_len == -1) {
                    elog(WARNING, "generate_find_reply_packet got an error");
                }
                send(watcher->fd, find_reply, find_reply_len, 0);
                json_object_put(results);
            }
            if (flag == 5) {
                //REPLY MODIFIED
                elog(WARNING, "terminate session");
                modify_ping_endsessions_reply(ping_endsessions_ok, request_id);
                send(watcher->fd, ping_endsessions_ok, PING_ENDSESSIONS_REPLY_LEN, 0);
            }

            if (*query_string != NULL) {
                free(*query_string);
            }
            if (*parameter_string != NULL) {
                free(*parameter_string);
            }
            if (query_string != NULL) {
                free(query_string);
            }
            if (parameter_string != NULL) {
                free(parameter_string);
            }

            free(*dbname);
            free(*collection);
            free(dbname);
            free(collection);
            break;


        case OP_REPLY:
            /* code */
            break;
        default:
            perror("UNKNOWN OP_CODE\n");
            return;
    }

    memset(buffer, 0, BUFFER_SIZE);


}

void write_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    ev_io_stop(loop, watcher);
    close(watcher->fd);
    free(watcher);
}

void _PG_init(void) {
    BackgroundWorker worker;

    pqsignal(SIGTERM, handle_sigterm);
    pqsignal(SIGINT, handle_sigterm);
    BackgroundWorkerUnblockSignals();
    memset(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_proxy");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "main_proxy");
    snprintf(worker.bgw_name, BGW_MAXLEN, "Proxy");
    snprintf(worker.bgw_type, BGW_MAXLEN, "Proxy");

    RegisterBackgroundWorker(&worker);
}

/**
 * return 0 if everything is successful
 * return -1 if not (for example, if smth with length of char *buffer)
 */
int parse_message(char *buffer, char **query_string, char **parameter_string) {
    u_int32_t flags = ((u_int32_t *) buffer)[4];
    int overall_sections_start_bit = 20; //because of mongodb protocol
    int overall_sections_end_bit = ((u_int32_t *) buffer)[0]; //msg_length
    u_int32_t msg_length = ((u_int32_t *) buffer)[0]; //msg_length
    u_int32_t checksum = 0;
    char section_kind = 0;
    bson_t *b[MAX_BSON_OBJECTS]; //array of *bson_t formed by parsing OP_MSG message type
    int next_bson_to_get = 0; //an index of b to be filled next
    int section_start = 0;
    //for converting strings to query_string, parameter_string
    int par_string_len;
    int len_of_par_strings = 0;
    char **jsons;

    if (flags && (1 << 7)) {
        overall_sections_end_bit -= 4; //it means there is a checksum in the end of the packet
        ///////////later : add buffersize <?> msg_length check
        checksum = ((u_int32_t *) buffer)[msg_length / sizeof(u_int32_t) - 1]; //the checksum
    }


    section_start = overall_sections_start_bit;

    for (; section_start < overall_sections_end_bit;) {

        int add_to_section_start = 0;
        section_kind = buffer[section_start];
        section_start++; //bc we got section kind

        switch (section_kind) {
            case BODY_MSG_SECTION_TYPE:
                /**
                 * A body section is encoded as a single BSON object.
                 * The size in the BSON object also serves as the size of the section.
                 * */
                int flag_local = parse_bson_object((buffer + section_start), &(b[next_bson_to_get]));
                if (flag_local < 0) {
                    return -1; //it means smth bad with parsing bson
                }

                next_bson_to_get++;
                add_to_section_start += ((u_int32_t * )(buffer + section_start))[0];
                break;
            case DOC_MSG_SECTION_TYPE:
                /**
                 * A Document Sequence section contains:
                 * int32 - size of the section
                 * cstring - Document sequence identifier.
                 * Zero or more BSON objects
                 * */
                u_int32_t size_section = ((u_int32_t * )(buffer + section_start))[0];
                // YES, it starts with the section start, not by cstring start - look at func realization
                ssize_t doc_string_len = get_str_len_from_doc_seq(buffer + section_start);
                if (doc_string_len < 0) {
                    elog(ERROR, "UNKNOWN PROBLEM WITH STRING");
                    return -1; //it means smth wrong with string => smth wrong with buffer/packet
                }

                //this is wwhere bsons located
                add_to_section_start += doc_string_len + 4; //size of the string + size of the section
                int j = 0;
                for (j = section_start + add_to_section_start; j < section_start + size_section;) {
                    u_int32_t my_bson_size = ((u_int32_t * )(buffer + j))[0];
                    int flag_local = parse_bson_object((char *) (buffer + j), &(b[next_bson_to_get]));
                    if (flag_local < 0) {
                        elog(ERROR, "PROBLEMS WITH PARSING BSON\n");
                        return -1; //it means smth bad with parsing bson
                    }

                    next_bson_to_get++;
                    j += my_bson_size;
                }
                add_to_section_start = j - section_start;
                break;

            default:
                elog(ERROR, "UNKNOWN SECTION KIND: %d\n", section_kind);
                return -1;
                break;
        }


        section_start += add_to_section_start;

    }

    //it  means there were no bsons
    if (next_bson_to_get == 0) {
        return -1;
    }

    size_t len_of_string = 0;
    char *str = bson_as_relaxed_extended_json(b[0], &len_of_string);
    *query_string = (char *) malloc(len_of_string + 1);
    memset(*query_string, 0, len_of_string + 1);
    memcpy(*query_string, str, len_of_string);
    bson_free(str);


    jsons = (char **) malloc((next_bson_to_get - 1) * sizeof(char *));


    for (int i = 1; i < next_bson_to_get; ++i) {
        size_t len_of_string = 0;
        jsons[i] = bson_as_relaxed_extended_json(b[i], &len_of_string);

        len_of_par_strings += len_of_string;
    }

    if (next_bson_to_get != 1) {

        par_string_len = (len_of_par_strings + 2 * (next_bson_to_get - 1) + 1) * sizeof(char *);

        *parameter_string = (char *) malloc(par_string_len);
        int now = 0;

        memset(*parameter_string, 0, par_string_len);
        memcpy(*parameter_string, "[", 1);
        now += 1;

        for (int i = 1; i < next_bson_to_get; ++i) {
            if (i + 1 != next_bson_to_get) {

                memcpy((char *) (*parameter_string + now), jsons[i], strlen(jsons[i]));
                now += strlen(jsons[i]);
                memcpy((char *) (*parameter_string + now), ", ", 2);
                now += 2;
            } else {
                memcpy((char *) (*parameter_string + now), jsons[i], strlen(jsons[i]));
                now += strlen(jsons[i]);
                memcpy((char *) (*parameter_string + now), "]", 1);
                now += 1;
            }
        }
    }


    for (int i = 1; i < next_bson_to_get; ++i) {
        bson_free(jsons[i]);
    }

    free(jsons);
    return 0;
}


/**
 * <just a light wrapper on bson_new_from_data(const uint8_t *data, size_t length) from libbson.h
 * may be removed later>
 * return 0 if everything is ok
 * return -1 if not (espesially if smth with length of char *my_data)
 */
int parse_bson_object(char *my_data, bson_t **my_bson) {
    size_t my_data_len = ((u_int32_t *) my_data)[0];
    *my_bson = bson_new_from_data(my_data, my_data_len);
    if (!my_bson) {
        return -1;
    }
    return 0;
}


/**
 * if string is valid (ends with '\0'), return (strlen(string in doc seq) + 1)
 * else return -1
 */
ssize_t get_str_len_from_doc_seq(char *doc_seq) {
    u_int32_t section_size = ((u_int32_t *) doc_seq)[0];
    ssize_t count = 0;

    for (int i = 4; i < (int) section_size; ++i) {
        if (doc_seq[i] == '\0') {
            count++;
            return count;
        }
        count++;
    }

    return -1;
}


void random_new_req_id(unsigned char *buffer) {
    static u_int32_t a = 911;
    ((u_int32_t *) buffer)[1] = a;
    a += 2;
}

void modify_insert_delete_reply(unsigned char *reply, u_int32_t response_to, int n) {
    random_new_req_id(reply);
    ((u_int32_t *) reply)[2] = response_to;
    ((u_int32_t *) reply)[7] = (u_int32_t) n; // 7 because of mongodb protocol

}

void modify_ping_endsessions_reply(unsigned char *reply, u_int32_t response_to) {
    random_new_req_id(reply);
    ((u_int32_t *) reply)[2] = response_to;
}

void modify_update_reply(unsigned char *reply, u_int32_t response_to, int nmodified) {
    random_new_req_id(reply);
    ((u_int32_t *) reply)[2] = response_to;
    ((u_int32_t * )(reply + 3))[(43 - 3) / 4] = nmodified;
}


int generate_find_reply_packet(struct json_object *data_array, char *reply, uint32_t response_to, char *db_name,
                               char *table_name) {
    /**
     * structure of find_reply_packet:
     * [0 - 3] message length
     * [4 - 7] request_id
     * [8 - 11] response_to
     * [12 - 15] opcode
     * [16 - 19] message flags
     * //start of the section
     * [20] = 0 kind: Body
     * //start of the BodyDocument
     * [21 - 24] Document lenght = 4 (the doclength itself)
     *                              + size of Element: cursor
     *                              + size of Element: ok
     *                              + 1 (= 0 :  end of the BodyDocument)
     * [25 - ...] Element: cursor
     * [... + 1 - ...] ok
     * [... + 1] = 0 end of the BodyDocument
     * //end of the Section
     */
    int message_lenght = 0;
    random_new_req_id(reply); //[4 - 7] request_id
    ((uint32_t * )(reply))[2] = response_to;
    memcpy(reply + 12, "\335\a\000\000", 4); //opcode
    memcpy(reply + 19, "\000\000\000\000", 4);
    reply[20] = 0;


    int doc_size = 4; // for 

    char cursor_buffer[BUFFER_SIZE];
    memset(cursor_buffer, 0, BUFFER_SIZE);
    int cursor_size = generate_cursor(data_array, cursor_buffer, db_name, table_name);
    if (cursor_size == -1) {
        return -1;
    }

    memcpy(reply + 21 + doc_size, cursor_buffer, cursor_size);
    doc_size += cursor_size;

    memcpy(reply + 21 + doc_size, "\001ok\000\000\000\000\000\000\000\360?", 12);
    doc_size += 12;
    reply[21 + doc_size] = 0; //end of the BodyDocument
    doc_size++;


    //setting the document size
    ((uint32_t * )(reply + 21))[0] = doc_size;


    //setting the message length
    message_lenght += doc_size + 21;
    ((uint32_t * )(reply))[0] = message_lenght;

    return message_lenght;
}


/**
 * return reply_size if everything is good
 * return -1 if something went wrong
 */
int generate_cursor(struct json_object *data_array, char *reply, char *db_name, char *table_name) {
    /**
     * struct of element: cursor
     * [0] = 0x03 type: Document 
     * [1-6] = "cursor"
     * [7] = 0
     * //then goes Document
     * [8-11] Document length
     * [12 - ...] element : firstBatch
     * [... - ...] element: id
     * [... - ...] element: ns
     * [... + 1] = 0 - end of the Document
     */

    int now_to_put = 0;
    reply[0] = 0x03;
    memcpy(reply + 1, "cursor", 6);
    reply[7] = 0;


    char first_batch_reply[BUFFER_SIZE];
    memset(first_batch_reply, 0, BUFFER_SIZE);
    int first_batch_size = generate_first_batch(data_array, first_batch_reply);
    if (first_batch_size == -1) {
        return -1;
    }
    now_to_put = 12;
    memcpy(reply + now_to_put, first_batch_reply, first_batch_size);
    now_to_put += first_batch_size;

    char id[] = "\022id\000\000\000\000\000\000\000\000\000";
    memcpy(reply + now_to_put, id, 12);
    now_to_put += 12;


    char ns_reply[BUFFER_SIZE];
    memset(ns_reply, 0, BUFFER_SIZE);
    int ns_size = generate_ns_element(ns_reply, db_name, table_name);
    if (ns_size == -1) {
        return -1;
    }
    memcpy(reply + now_to_put, ns_reply, ns_size);
    now_to_put += ns_size;
    reply[now_to_put] = 0; //end of the Document
    now_to_put++;


    //setting the Document length
    ((uint32_t * )(reply + 8))[0] = now_to_put - 8;
    return now_to_put;
}


/**
 * return reply_size if everything is good
 * return -1 if something went wrong
 */
int generate_ns_element(char *reply, char *db_name, char *table_name) {
    /**
     * structure of ns
     * [0] = 0x02 type: string
     * [1-2] = "ns"
     * [3] = 0 (like end of string)
     * [4 - 7] = length of string
     * [8 - ...] = string (db_name.table_name) (it includes!!! 0 in the end of the string)
     * 
     */
    int value_size = 0;
    reply[0] = 0x02;
    reply[1] = 0x6e;
    reply[2] = 0x73;
    reply[3] = 0;


    memcpy(reply + 8, db_name, strlen(db_name));
    value_size += strlen(db_name);
    reply[8 + value_size] = 0x2e; //= '.'
    value_size++;
    memcpy(reply + 8 + value_size, table_name, strlen(table_name));
    value_size += strlen(table_name);
    reply[8 + value_size] = 0; // 0 in the end of the string
    //bc 0 int hte end of the string must be included to the lenght of the string
    value_size++;
    //setting length of element
    ((uint32_t * )(reply))[1] = value_size;
    return value_size + 8;
}

/**
 * return reply_size if everything is good
 * return -1 if something went wrong
 */
int generate_first_batch(struct json_object *data_array, char *reply) {
    /**
     * struct of firstBatch
     * [0] = 0x04 type: array
     * [1-10] name of element: firstBatch
     * [11] = 0
     * //here Document starts
     * [12-15] Document lenght
     * //[16] = 0x03 type: Document 
     * element 0: (from generate_first_batch_element_i)
     * ...
     * element n-1:
     * [last] = 0
     */
    int now_to_put = 0;
    reply[0] = 0x04;
    char firstBatch[] = "firstBatch";
    memcpy(reply + 1, firstBatch, 10);
    reply[11] = 0;
    now_to_put = 16;

    char el_reply[BUFFER_SIZE];


    int array_length = json_object_array_length(data_array);

    /**
     * we go throw data_array (thow all jsons we have), process them and add result to our reply
     */
    if (array_length > 0) {
        for (int i = 0; i < array_length; i++) {
            struct json_object *data_json = json_object_array_get_idx(data_array, i);

            memset(el_reply, 0, BUFFER_SIZE);
            int el_i_size = generate_first_batch_element_i(data_json, el_reply, i);
            if (el_i_size == -1) {
                //everything is bad
                return -1;
            }
            memcpy(reply + now_to_put, el_reply, el_i_size);
            now_to_put += el_i_size;

        }
    }


    reply[now_to_put] = 0;
    //this was added for testing answers for empty find. for not empty find that wasn't needed
    now_to_put++;

    //setting the Document lenght
    ((uint32_t * )(reply + 12))[0] = now_to_put - 12;

    return now_to_put;
}

/**
 * return reply_size if everything is good
 * return -1 if something went wrong
 */
int generate_first_batch_element_i(struct json_object *single_json, char *reply, int number_of_el) {
    /**
     * structure of element: i
     * [0](byte) type: Document (0x03)
     * [1](byte) Element: 30 + number_of_el - bc i found it in traces
     * [2]byte = 0
     * [3-6]4 bytes : document lenght
     * [7-...]then elements we got from generate_subelemets_string()
     * [... + 1]byte = 0
     */
    int place_for_answer = 7;
    int now_to_put = 0;
    reply[now_to_put] = 0x03;
    now_to_put++;
    //reply[1] = (char)(0x30 + number_of_el);
    int ne_copy = number_of_el;
    int digits_number = 0;
    if (number_of_el == 0) {
        digits_number = 1;
    } else {
        while (ne_copy != 0) {
            ne_copy /= 10;
            digits_number++;
        }
    }


    ne_copy = number_of_el;
    for (int i = now_to_put + digits_number - 1; i >= now_to_put; i--) {
        reply[i] = (char) (0x30 + ne_copy % 10);
        ne_copy /= 10;
    }
    now_to_put += digits_number;
    reply[now_to_put] = 0;
    now_to_put++;
    //reply[1] = (char)(0x30 + number_of_el); 

    //reply[2] = 0;

    char answer[BUFFER_SIZE];
    memset(answer, 0, BUFFER_SIZE);
    int ans_size = generate_subelemets_string(single_json, answer);
    if (ans_size < 0) {
        return -1;
    }

    memcpy((reply + 6 + digits_number), answer, ans_size);
    reply[6 + digits_number + ans_size] = 0;

    //counting reply_size
    int reply_size = 6 + digits_number + ans_size + 1;
    //setting the Document length (it is = reply_size - 3(at the start of the document))
    ((uint32_t * )(reply + 2 + digits_number))[0] = reply_size - 2 - digits_number;
    return reply_size;

}


/**
 * return size of reply (bytes) if everything is fine
 * return -1 if something went wrong
 */
int generate_subelemets_string(struct json_object *single_json, char *reply) {
    /**
     * structure
     * type: 0x07 [0]
     * element: _id [1-3]
     * byte = 0 [4]
     * ObjectID: ... [5-16]
     * THIS PART WOULD ALWAYS BE THE SAME!!!
     */
    // char element_id[] = "\a_idf\237\037\246\266\037\020\2324\356\275\262";

    char element_id[18];
    element_id[0] = 0x07;
    element_id[1] = 0x5f;
    element_id[2] = 0x69;
    element_id[3] = 0x64;
    element_id[4] = 0x00;
    element_id[5] = 0x66;
    element_id[6] = 0x9f;
    element_id[7] = 0x1f;
    element_id[8] = 0xa6;
    element_id[9] = 0xb6;
    element_id[10] = 0x1f;
    element_id[11] = 0x10;
    element_id[12] = 0x9a;
    element_id[13] = 0x34;
    element_id[14] = 0xee;
    element_id[15] = 0xbd;
    element_id[16] = 0xb2;

    int size_of_reply = 0;

    memcpy(reply, element_id, 17);

    size_of_reply += 17;

    struct json_object_iterator it = json_object_iter_begin(single_json);
    struct json_object_iterator it_end = json_object_iter_end(single_json);

    /**
     * meaningful element structure:
     * type: type of value - 1 byte
     * name of the field: - how much needed (it DOES end with '\0')
     * lenght: 4 byte (ONLY if type is string)
     * value:
     */

    while (!json_object_iter_equal(&it, &it_end)) {
        char el[BUFFER_SIZE];
        memset(el, 0, BUFFER_SIZE);
        int real_size = 0;
        int now_to_put = 0;

        const char *field_str = json_object_iter_peek_name(&it);
        struct json_object *value_json = json_object_iter_peek_value(&it);
        const char *value_str = get_json_value_as_string(value_json);

        //if there is an empty value, we do not send it
        if (strcmp(value_str, "") == 0) {
            json_object_iter_next(&it);
            continue;
        }

        //here we go type
        int type = get_type_of_value(value_json);
        if (type == -1) {
            return -1;
        }

        //here we put type
        el[now_to_put] = type;
        now_to_put++;
        //here we put name of the field
        memcpy((el + now_to_put), field_str, strlen(field_str) + 1); // copy field name to el
        now_to_put += strlen(field_str) + 1;

        //here we put size of the field (if needed) and field value
        switch (type) {
            case 2: //string
                //here we put size of the field
                char lilbuf[4];
                memset(lilbuf, 0, 4);
                int my_len = strlen(value_str) + 1;
                int num_16_digits = 0;
                while (my_len != 0) {
                    my_len /= 256;
                    num_16_digits++;
                }

                my_len = strlen(value_str) + 1;

                for (int i = 0; i < num_16_digits; i++) {
                    lilbuf[i] = my_len % 256;
                    my_len /= 256;
                }
                //memcpy(el + now_to_put, lilbuf, 4);
                for (int i = 0; i < 4; ++i) {
                    if (lilbuf[i]) {
                        el[now_to_put + i] = lilbuf[i]; //- '0';
                    } else {
                        el[now_to_put + i] = lilbuf[i];
                    }

                }
                now_to_put += 4;
                //here we put field value
                memcpy((el + now_to_put), value_str, strlen(value_str));
                now_to_put += strlen(value_str);
                el[now_to_put] = 0;
                now_to_put++;
                break;
            case 16: //int32
                //here we put field value
                *(int32_t * )(&el[0] + now_to_put) = (int32_t) atoi(value_str);
                now_to_put += 4;
                break;
            case 8: //boolean
                //here we put field value
                *(u_int8_t * )(&el[0] + now_to_put) = (u_int8_t) atoi(value_str) > 0 ? 1 : 0;
                now_to_put += 1;
                break;
            case 1: //double
                //here we put field value
                *(int64_t * )(&el[0] + now_to_put) = (int64_t) atoi(value_str);
                now_to_put += 8;
                break;
            default:
                elog(WARNING, "UNCKOWN TYPE IN JSON: %d\n", type);
                return -1;
        }
        //here we think that el(element string) is built
        real_size = now_to_put;

        memcpy((reply + size_of_reply), el, real_size);
        size_of_reply += real_size;


        json_object_iter_next(&it);
    }

    return size_of_reply;
}


/**
 * return type of json_value
 * currently supports: int32, string, boolean, double
 * return
 * string: 2 = 0x02
 * int32: 16 = 0x10
 * boolean: 8 = 0x08
 * double: 1 = 0x01
 * else: -1
 */
int get_type_of_value(struct json_object *field_value) {
    if (json_object_is_type(field_value, json_type_string)) {
        return 0x02;
    } else if (json_object_is_type(field_value, json_type_int)) {
        return 0x10;
    } else if (json_object_is_type(field_value, json_type_boolean)) {
        return 0x08;
    } else if (json_object_is_type(field_value, json_type_double)) {
        return 0x01;
    }
    return -1;
}


