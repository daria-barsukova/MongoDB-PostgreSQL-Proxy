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

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define MONGO_PORT 5011
#define BUFFER_SIZE 1024
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


PGDLLEXPORT int main_proxy(void);

void accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
void read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
void write_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
void process_message(uint32_t response_to, unsigned char *buffer, char *json_metadata,char *json_data_array, int *flag);
void parse_mongodb_packet(char *buffer, char **query_string, char **parameter_string);
int parse_message(char *buffer, char **query_string, char **parameter_string);
int parse_bson_object(char *my_data, bson_t **my_bson);
ssize_t get_str_len_from_doc_seq(char *doc_seq);
bool check_and_create_database(PGconn *conn, const char *dbname);
bool check_and_create_table(PGconn *conn, const char *table_name);
bool check_and_create_columns(PGconn *conn, const char *table_name, struct json_object *data_json);
const char *get_json_value_as_string(struct json_object *field_value);
bool execute_insert_queries(PGconn *conn, const char *table_name, struct json_object *data_array, int *inserted_count);
bool execute_query_insert_to_postgres(const char *json_metadata, const char *json_data_array, int *inserted_count);
bool execute_delete_queries(PGconn *conn, const char *table_name, struct json_object *delete_array, int* deleted_count);
bool execute_query_delete_to_postgres(const char *json_metadata, const char *json_data_array, int* deleted_count);
bool execute_update_queries(PGconn *conn, const char *table_name, struct json_object *update_array, int* updated_count);
bool execute_query_update_to_postgres(const char *json_metadata, const char *json_data_array, int* updated_count);
void cleanup_and_exit(struct ev_loop *loop);
static void handle_sigterm(SIGNAL_ARGS);
void random_new_req_id(unsigned char *buffer);
void modify_insert_delete_reply(unsigned char *reply, u_int32_t response_to, int n);
void modify_ping_endsessions_reply(unsigned char *reply, u_int32_t response_to);
void modify_update_reply(unsigned char *reply, u_int32_t response_to, int nmodified);


//int flag = 0;
int server_sd = -1;

static void handle_sigterm(SIGNAL_ARGS) {
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
    snprintf(query, sizeof(query), "SELECT 1 FROM information_schema.tables WHERE table_name='%s'", table_name);

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return false;
    }

    if (PQntuples(res) == 0) {
        PQclear(res);
        snprintf(query, sizeof(query), "CREATE TABLE %s (id SERIAL PRIMARY KEY)", table_name);
        res = PQexec(conn, query);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            PQclear(res);
            return false;
        }
    }

    PQclear(res);
    return true;
}

bool check_and_create_columns(PGconn *conn, const char *table_name, struct json_object *data_json) {
    struct json_object_iterator it = json_object_iter_begin(data_json);
    struct json_object_iterator it_end = json_object_iter_end(data_json);

    while (!json_object_iter_equal(&it, &it_end)) {
        const char *field_name = json_object_iter_peek_name(&it);
        // Skip "_id" field
        if (strcmp(field_name, "_id") == 0) {
            json_object_iter_next(&it);
            continue;
        }
        if (strcmp(field_name, "q") != 0 && strcmp(field_name, "u") != 0 && strcmp(field_name, "multi") != 0) {

            char query[BUFFER_SIZE];
            snprintf(query, sizeof(query),
                     "SELECT 1 FROM information_schema.columns WHERE table_name='%s' AND column_name='%s'", table_name,
                     field_name);

            PGresult *res = PQexec(conn, query);
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                PQclear(res);
                return false;
            }

            if (PQntuples(res) == 0) {
                PQclear(res);
                // Determine type of field from JSON object
                struct json_object *field_value = json_object_iter_peek_value(&it);
                const char *field_type = "TEXT";
                if (json_object_is_type(field_value, json_type_int)) {
                    field_type = "INT";
                } else if (json_object_is_type(field_value, json_type_boolean)) {
                    field_type = "BOOLEAN";
                } else if (json_object_is_type(field_value, json_type_double)) {
                    field_type = "DOUBLE PRECISION";
                }

                snprintf(query, sizeof(query), "ALTER TABLE %s ADD COLUMN %s %s", table_name, field_name, field_type);
                res = PQexec(conn, query);
                if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                    PQclear(res);
                    return false;
                }
            }

            PQclear(res);
        }
        json_object_iter_next(&it);
    }

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

        if (!check_and_create_columns(conn, table_name, data_json)) {
            fprintf(stderr, "Failed to check and create columns\n");
            return false;
        }

        // Construct SQL query for insertion
        struct json_object_iterator it = json_object_iter_begin(data_json);
        struct json_object_iterator it_end = json_object_iter_end(data_json);
        char columns[BUFFER_SIZE] = "";
        char values[BUFFER_SIZE] = "";

        while (!json_object_iter_equal(&it, &it_end)) {
            const char *field_name = json_object_iter_peek_name(&it);
            // Skip "_id" field
            if (strcmp(field_name, "_id") == 0) {
                json_object_iter_next(&it);
                continue;
            }

            struct json_object *field_value = json_object_iter_peek_value(&it);

            strcat(columns, field_name);
            strcat(columns, ",");

            const char *value_str = get_json_value_as_string(field_value);
            if (value_str) {
                strcat(values, "'");
                strcat(values, value_str);
                strcat(values, "'");
            } else {
                strcat(values, "NULL");
            }
            strcat(values, ",");

            json_object_iter_next(&it);
        }

        // Remove trailing commas
        columns[strlen(columns) - 1] = '\0';
        values[strlen(values) - 1] = '\0';

        char query[BUFFER_SIZE];
        snprintf(query, sizeof(query), "INSERT INTO %s (%s) VALUES (%s)", table_name, columns, values);

        PGresult *res = PQexec(conn, query);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "INSERT command failed: %s", PQerrorMessage(conn));
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

bool execute_delete_queries(PGconn *conn, const char *table_name, struct json_object *delete_array, int* deleted_count) {
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

        struct json_object_iterator it = json_object_iter_begin(q_json);
        struct json_object_iterator it_end = json_object_iter_end(q_json);
        char condition[BUFFER_SIZE] = "";

        while (!json_object_iter_equal(&it, &it_end)) {
            const char *field_name = json_object_iter_peek_name(&it);
            struct json_object *field_value = json_object_iter_peek_value(&it);

            if (!column_exists(conn, table_name, field_name)) {
                fprintf(stderr, "Column '%s' does not exist in table '%s'\n", field_name, table_name);
                return true;  // Return 0 deleted rows
            }

            const char *value_str = json_object_get_string(field_value);

            strcat(condition, field_name);
            strcat(condition, "='");
            strcat(condition, value_str);
            strcat(condition, "' AND ");

            json_object_iter_next(&it);
        }

        // Remove the last " AND "
        condition[strlen(condition) - 5] = '\0';

        char query[BUFFER_SIZE];
        if (json_object_get_int(limit_json) == 0) {
            snprintf(query, sizeof(query), "DELETE FROM %s WHERE %s", table_name, condition);
        } else {
            snprintf(query, sizeof(query),
                     "WITH del AS (SELECT ctid FROM %s WHERE %s LIMIT 1) "
                     "DELETE FROM %s WHERE ctid IN (SELECT ctid FROM del)",
                     table_name, condition, table_name);
        }

        PGresult *res = PQexec(conn, query);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "DELETE command failed: %s", PQerrorMessage(conn));
            PQclear(res);
            return false;
        }
        *deleted_count += atoi(PQcmdTuples(res));
        PQclear(res);
    }
    return true;
}

bool execute_query_delete_to_postgres(const char *json_metadata, const char *json_data_array, int* deleted_count) {
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

bool execute_update_queries(PGconn *conn, const char *table_name, struct json_object *update_array, int* updated_count) {
    int array_length = json_object_array_length(update_array);
    *updated_count = 0;
    for (int i = 0; i < array_length; i++) {
        struct json_object *update_json = json_object_array_get_idx(update_array, i);
        struct json_object *q_json, *u_json, *multi_json;

        if (!check_and_create_columns(conn, table_name, update_json)) {
            fprintf(stderr, "Failed to check and create columns\n");
            return false;
        }

        if (!json_object_object_get_ex(update_json, "q", &q_json) ||
            !json_object_object_get_ex(update_json, "u", &u_json)) {
            fprintf(stderr, "Invalid update JSON format\n");
            return false;
        }

        struct json_object_iterator it = json_object_iter_begin(q_json);
        struct json_object_iterator it_end = json_object_iter_end(q_json);
        char condition[BUFFER_SIZE] = "";

        while (!json_object_iter_equal(&it, &it_end)) {
            const char *field_name = json_object_iter_peek_name(&it);
            struct json_object *field_value = json_object_iter_peek_value(&it);
            const char *value_str = json_object_get_string(field_value);

            strcat(condition, field_name);
            strcat(condition, "='");
            strcat(condition, value_str);
            strcat(condition, "' AND ");

            json_object_iter_next(&it);
        }

        // Remove the last " AND "
        condition[strlen(condition) - 5] = '\0';

        struct json_object *set_json;
        if (!json_object_object_get_ex(u_json, "$set", &set_json)) {
            fprintf(stderr, "Invalid update JSON format\n");
            return false;
        }
        if (!check_and_create_columns(conn, table_name, set_json)) {
            fprintf(stderr, "Failed to check or create columns for update\n");
            return false;
        }
        it = json_object_iter_begin(set_json);
        it_end = json_object_iter_end(set_json);
        char set_clause[BUFFER_SIZE] = "";

        while (!json_object_iter_equal(&it, &it_end)) {
            const char *field_name = json_object_iter_peek_name(&it);
            struct json_object *field_value = json_object_iter_peek_value(&it);
            const char *value_str = json_object_get_string(field_value);

            strcat(set_clause, field_name);
            strcat(set_clause, "='");
            strcat(set_clause, value_str);
            strcat(set_clause, "', ");

            json_object_iter_next(&it);
        }

        // Remove the last ", "
        set_clause[strlen(set_clause) - 2] = '\0';

        char query[BUFFER_SIZE];
        if (json_object_object_get_ex(update_json, "multi", &multi_json) && json_object_get_boolean(multi_json)) {
            snprintf(query, sizeof(query), "UPDATE %s SET %s WHERE %s", table_name, set_clause, condition);
        } else {
            snprintf(query, sizeof(query),
                     "UPDATE %s SET %s WHERE ctid IN (SELECT ctid FROM %s WHERE %s LIMIT 1)",
                     table_name, set_clause, table_name, condition);
        }

        PGresult *res = PQexec(conn, query);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "UPDATE command failed: %s", PQerrorMessage(conn));
            PQclear(res);
            return false;
        }
        *updated_count += atoi(PQcmdTuples(res));
        PQclear(res);
    }
    return true;
}

bool execute_query_update_to_postgres(const char *json_metadata, const char *json_data_array,int* updated_count) {
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
bool execute_find_query(PGconn *conn, const char *table_name, struct json_object *find_json, struct json_object **results) {
    struct json_object *filter_json, *limit_json, *single_batch_json;
    char condition[BUFFER_SIZE] = "";
    int limit = -1;
    bool single_batch = false;

    if (json_object_object_get_ex(find_json, "filter", &filter_json)) {
        struct json_object_iterator it = json_object_iter_begin(filter_json);
        struct json_object_iterator it_end = json_object_iter_end(filter_json);

        while (!json_object_iter_equal(&it, &it_end)) {
            const char *field_name = json_object_iter_peek_name(&it);
            struct json_object *field_value = json_object_iter_peek_value(&it);
            const char *value_str = json_object_get_string(field_value);

            strcat(condition, field_name);
            strcat(condition, "='");
            strcat(condition, value_str);
            strcat(condition, "' AND ");

            json_object_iter_next(&it);
        }

        // Remove the last " AND "
        if (strlen(condition) > 0) {
            condition[strlen(condition) - 5] = '\0';
        }
    }

    if (json_object_object_get_ex(find_json, "limit", &limit_json)) {
        limit = json_object_get_int(limit_json);
    }

    if (json_object_object_get_ex(find_json, "singleBatch", &single_batch_json)) {
        single_batch = json_object_get_boolean(single_batch_json);
    }

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

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "SELECT command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    int rows = PQntuples(res);
    *results = json_object_new_array();

    for (int i = 0; i < rows; i++) {
        struct json_object *row_obj = json_object_new_object();
        int n_fields = PQnfields(res);

        for (int j = 0; j < n_fields; j++) {
            const char *field_name = PQfname(res, j);
            const char *field_value = PQgetvalue(res, i, j);
            json_object_object_add(row_obj, field_name, json_object_new_string(field_value));
        }
        json_object_array_add(*results, row_obj);
    }

    PQclear(res);
    return true;
}

bool execute_query_find_to_postgres(const char *json_metadata, struct json_object **results) {
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

    const char *collection = json_object_get_string(find_obj);
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

    struct json_object *find_json = json_tokener_parse(json_metadata);
    if (!find_json) {
        fprintf(stderr, "Failed to parse find JSON\n");
        json_object_put(metadata_json);
        PQfinish(conn);
        return false;
    }

    if (!execute_find_query(conn, collection, find_json, results)) {
        fprintf(stderr, "Failed to execute find query\n");
        json_object_put(metadata_json);
        json_object_put(find_json);
        PQfinish(conn);
        return false;
    }

    json_object_put(metadata_json);
    json_object_put(find_json);
    PQfinish(conn);

    return true;
}


void process_message(uint32_t response_to, unsigned char *buffer,  char *json_metadata, char *json_data_array, int *flag) {

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
            return;
        } else {
            elog(WARNING, "Update from PostgreSQL failed");
            *flag = 9;
            memset(buffer, 0, BUFFER_SIZE);
            return;
        }
    }
    if (buffer[26] == 'f') {
        struct json_object *results;
        if (execute_query_find_to_postgres(json_metadata, &results)) {
            *flag = 10;
            fprintf(stderr, "Find query executed successfully. Results:\n%s\n", json_object_to_json_string_ext(results, JSON_C_TO_STRING_PRETTY));
        } else {
            fprintf(stderr, "Failed to execute find query\n");
        }

        json_object_put(results);
        return;
    }
}

int main_proxy(void) {
    struct ev_loop *loop = ev_default_loop(0);
    int reuseaddr = 1;
    struct sockaddr_in addr;
    struct ev_io w_accept;

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

    if (listen(server_sd, 2) < 0) {
        perror("listen error");
        close(server_sd);
        return -1;
    }

    ev_io_init(&w_accept, accept_cb, server_sd, EV_READ);
    ev_io_start(loop, &w_accept);
    //ev_signal_init(&signal_watcher, handle_sigterm, SIGTERM);
    //ev_signal_start(loop, &signal_watcher);

    ev_loop(loop, 0);

    cleanup_and_exit(loop);
    return 0;
}

void accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    int client_sd;
    struct ev_io *w_client;

    if (EV_ERROR & revents) {
        perror("got invalid event");
        return;
    }

    client_sd = accept(watcher->fd, NULL, NULL);
    if (client_sd < 0) {
        perror("accept error");
        return;
    }

    w_client = (struct ev_io *) malloc(sizeof(struct ev_io));
    if (!w_client) {
        perror("malloc error");
        close(client_sd);
        return;
    }

    ev_io_init(w_client, read_cb, client_sd, EV_READ);
    ev_io_start(loop, w_client);

}

void cleanup_and_exit(struct ev_loop *loop) {
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

    query_string = (char **) malloc(sizeof(char *));
    parameter_string = (char **) malloc(sizeof(char *));
    *query_string = NULL;
    *parameter_string = NULL;


    msg_length = ((u_int32_t *) buffer)[0];
    request_id = ((u_int32_t *) buffer)[1];
    response_to = ((u_int32_t *) buffer)[2];
    op_code = ((u_int32_t *) buffer)[3];

    unsigned char response[] = "I\001\000\000~\001\000\000\003\000\000\000\001\000\000\000\b\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\001\000\000\000%\001\000\000\bhelloOk\000\001\bismaster\000\001\003topologyVersion\000-\000\000\000\aprocessId\000f\225\335\246B(\\C\202\2468\351\022counter\000\000\000\000\000\000\000\000\000\000\020maxBsonObjectSize\000\000\000\000\001\020maxMessageSizeBytes\000\000l\334\002\020maxWriteBatchSize\000\240\206\001\000\tlocalTime\000\032T\246\271\220\001\000\000\020logicalSessionTimeoutMinutes\000\036\000\000\000\020connectionId\000*\000\000\000\020minWireVersion\000\000\000\000\000\020maxWireVersion\000\025\000\000\000\breadOnly\000\000\001ok\000\000\000\000\000\000\000\360?";
    unsigned char msg_response[] = "&\000\000\000\006\000\000\000\001\000\000\000\335\a\000\000\000\000\000\000\000\021\000\000\000\001ok\000\000\000\000\000\000\000\360?";
    unsigned char ok_query_response[] = "-\000\000\000\a\000\000\000\t\000\000\000\335\a\000\000\000\000\000\000\000\030\000\000\000\020n\000\001\000\000\000\001ok\000\000\000\000\000\000\000\360?";

    switch (op_code) {
        case OP_QUERY:
            //parse_query(buffer);
            elog(WARNING, "send reply");
            send(watcher->fd, response, sizeof(response), 0);
            break;
        case OP_MSG:
            parse_message(buffer, query_string, parameter_string);
            process_message(request_id, buffer, *query_string, *parameter_string, &flag);
            if (flag == 2) {
                elog(WARNING, "send ping");
                //REPLY MODIFIED
                modify_ping_endsessions_reply(&ping_endsessions_ok, request_id);
                //send(watcher->fd, msg_response, sizeof(msg_response), 0);
                send(watcher->fd, ping_endsessions_ok, PING_ENDSESSIONS_REPLY_LEN, 0);
            }
            if (flag == 3) {
                elog(WARNING, "send insert");
                send(watcher->fd, ok_query_response, sizeof(ok_query_response), 0);
            }
            if (flag == 6) {
                elog(WARNING, "send delete");
                send(watcher->fd, ok_query_response, sizeof(ok_query_response), 0);
            }
            if (flag == 8) {
                elog(WARNING, "send update");
                //modify_update_reply(&update_ok[0], response_to, )
                send(watcher->fd, ok_query_response, sizeof(ok_query_response), 0);
            }
            if (flag == 10) {
                elog(WARNING, "send find");
                send(watcher->fd, ok_query_response, sizeof(ok_query_response), 0);
            }
            if (flag == 5) {
                elog(WARNING, "terminate session");
                //REPLY MODIFIED
                modify_ping_endsessions_reply(&ping_endsessions_ok, request_id);
                //send(watcher->fd, msg_response, sizeof(msg_response), 0);
                send(watcher->fd, ping_endsessions_ok, PING_ENDSESSIONS_REPLY_LEN, 0);
            }
            break;
        case OP_REPLY:
            /* code */
            break;
        default:
            perror("UNKNOWN OP_CODE\n");
            return;
    }

    memset(buffer, 0, BUFFER_SIZE);
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
    ((u_int32_t*)buffer)[1] = a;
    a +=2;
}

void modify_insert_delete_reply(unsigned char *reply, u_int32_t response_to, int n) {
    random_new_req_id(reply);
    ((u_int32_t*)reply)[2] = response_to;
    ((u_int32_t*)reply)[7] = (u_int32_t) n; // 7 because of mongodb protocol

}

void modify_ping_endsessions_reply(unsigned char *reply, u_int32_t response_to) {
    random_new_req_id(reply);
    ((u_int32_t*)reply)[2] = response_to;
}

void modify_update_reply(unsigned char *reply, u_int32_t response_to, int nmodified) {
    random_new_req_id(reply);
    ((u_int32_t*)reply)[2] = response_to;
    ((u_int32_t*)(reply + 3))[(43 - 3) / 4] = nmodified;
}


