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

#define MONGO_PORT 5039
#define BUFFER_SIZE 1024
#define OP_QUERY 2004
#define OP_REPLY 1
#define OP_MSG 2013
#define PG_CONNINFO "dbname=postgres user=user1 password=passwd host=localhost port=5432"


PGDLLEXPORT int main_proxy(void);

void accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
void read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
void write_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
bool insert_to_postgres(const char *json_query);
void process_message(uint32_t response_to, unsigned char *buffer, unsigned char response[BUFFER_SIZE]);

int flag = 0;
int server_sd = -1;

static void handle_sigterm(SIGNAL_ARGS) {
    if (server_sd >= 0) {
        close(server_sd);
    }
    exit(0);
}

bool execute_query_to_postgres(const char *json_query) {

    struct json_object *parsed_json = json_tokener_parse(json_query);
    if (parsed_json == NULL) {
        elog(WARNING, "Failed to parse JSON query");
        return false;
    }

    struct json_object *collection_obj, *data_obj;
    if (!json_object_object_get_ex(parsed_json, "collection", &collection_obj) ||
        !json_object_object_get_ex(parsed_json, "data", &data_obj)) {
        elog(WARNING, "Invalid JSON format");
        json_object_put(parsed_json); // Free memory
        return false;
    }

    const char *collection = json_object_get_string(collection_obj);
    struct json_object_iterator it = json_object_iter_begin(data_obj);
    struct json_object_iterator itEnd = json_object_iter_end(data_obj);

    char sql[BUFFER_SIZE] = {0};
    snprintf(sql, BUFFER_SIZE, "INSERT INTO %s (", collection);

    char columns[BUFFER_SIZE] = {0};
    char values[BUFFER_SIZE] = {0};

    while (!json_object_iter_equal(&it, &itEnd)) {
        const char *key = json_object_iter_peek_name(&it);
        struct json_object *val = json_object_iter_peek_value(&it);

        snprintf(columns + strlen(columns), BUFFER_SIZE - strlen(columns), "%s, ", key);
        snprintf(values + strlen(values), BUFFER_SIZE - strlen(values), "'%s', ", json_object_get_string(val));

        json_object_iter_next(&it);
    }

    columns[strlen(columns) - 2] = '\0';
    values[strlen(values) - 2] = '\0';

    snprintf(sql + strlen(sql), BUFFER_SIZE - strlen(sql), "%s) VALUES (%s);", columns, values);

    PGconn *conn = PQconnectdb(PG_CONNINFO);
    if (PQstatus(conn) != CONNECTION_OK) {
        elog(WARNING, "Connection to database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        json_object_put(parsed_json); // Free memory
        return false;
    }

    PGresult *res = PQexec(conn, sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        elog(WARNING, "SQL command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        json_object_put(parsed_json); // Free memory
        return false;
    }

    PQclear(res);
    PQfinish(conn);
    json_object_put(parsed_json);
    return true;
}

void process_message(uint32_t response_to, unsigned char *buffer, unsigned char response[BUFFER_SIZE]) {

    if (buffer[18] == 1) {
        flag = 1;
        elog(WARNING, "ignore");
        memset(buffer, 0, BUFFER_SIZE);
        return;
    }

    if (buffer[26] == 'h') {
        flag = 1;
        elog(WARNING, "ignore");
        memset(buffer, 0, BUFFER_SIZE);
        return;
    }

    elog(WARNING, "before ping check");
    if (buffer[26] == 'p') {
        flag = 2;
        elog(WARNING, "ping");
        memset(buffer, 0, BUFFER_SIZE);
        return;
    }
    const char *json_query = "{\"collection\": \"testcollection\", \"data\": {\"name\": \"example\"}}";

    if (buffer[26] == 'i') {
        if (execute_query_to_postgres((const char *) json_query)) {
            elog(WARNING, "Insert to PostgreSQL successful");
            flag = 3;
            memset(buffer, 0, BUFFER_SIZE);
            return;
        } else {
            elog(WARNING, "Insert to PostgreSQL failed");
            flag = 4;
            memset(buffer, 0, BUFFER_SIZE);
            return;
        }
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

    ev_loop(loop, 0);

    if (loop) {
        ev_break(loop, EVBREAK_ALL);
    }
    if (server_sd >= 0) {
        close(server_sd);
    }
    ev_loop_destroy(loop);
    return 0;
}

void accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    if (EV_ERROR & revents) {
        perror("got invalid event");
        return;
    }

    int client_sd = accept(watcher->fd, NULL, NULL);
    if (client_sd < 0) {
        perror("accept error");
        return;
    }

    struct ev_io *w_client = (struct ev_io *) malloc(sizeof(struct ev_io));
    if (!w_client) {
        perror("malloc error");
        close(client_sd);
        return;
    }

    ev_io_init(w_client, read_cb, client_sd, EV_READ);
    ev_io_start(loop, w_client);
}

void read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    if (EV_ERROR & revents) {
        perror("got invalid event");
        return;
    }

    unsigned char buffer[BUFFER_SIZE];
    ssize_t read = recv(watcher->fd, buffer, BUFFER_SIZE, 0);

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

    uint32_t message_length = ((uint32_t *) buffer)[0];
    uint32_t request_id = ((uint32_t *) buffer)[1];
    uint32_t response_to = ((uint32_t *) buffer)[2];
    uint32_t op_code = ((uint32_t *) buffer)[3];

    unsigned char response[] = "I\001\000\000~\001\000\000\003\000\000\000\001\000\000\000\b\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\001\000\000\000%\001\000\000\bhelloOk\000\001\bismaster\000\001\003topologyVersion\000-\000\000\000\aprocessId\000f\225\335\246B(\\C\202\2468\351\022counter\000\000\000\000\000\000\000\000\000\000\020maxBsonObjectSize\000\000\000\000\001\020maxMessageSizeBytes\000\000l\334\002\020maxWriteBatchSize\000\240\206\001\000\tlocalTime\000\032T\246\271\220\001\000\000\020logicalSessionTimeoutMinutes\000\036\000\000\000\020connectionId\000*\000\000\000\020minWireVersion\000\000\000\000\000\020maxWireVersion\000\025\000\000\000\breadOnly\000\000\001ok\000\000\000\000\000\000\000\360?";
    unsigned char msg_response[] = "&\000\000\000\006\000\000\000\001\000\000\000\335\a\000\000\000\000\000\000\000\021\000\000\000\001ok\000\000\000\000\000\000\000\360?";
    unsigned char ok_query_response[] = "-\000\000\000\a\000\000\000\t\000\000\000\335\a\000\000\000\000\000\000\000\030\000\000\000\020n\000\001\000\000\000\001ok\000\000\000\000\000\000\000\360?";

    switch (op_code) {
        case OP_QUERY:
            elog(WARNING, "send reply");
            send(watcher->fd, response, sizeof(response), 0);
            break;
        case OP_MSG:
            process_message(request_id, buffer, msg_response);
            if (flag == 2) {
                elog(WARNING, "send ping");
                send(watcher->fd, msg_response, sizeof(msg_response), 0);
            } else if (flag == 3) {
                elog(WARNING, "send insert");
                send(watcher->fd, ok_query_response, sizeof(ok_query_response), 0);
            }
            break;
        default:
            elog(WARNING, "UNKNOWN OP_CODE :(");
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
