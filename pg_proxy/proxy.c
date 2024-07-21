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


#define BODY_MSG_SECTION_TYPE 0
#define DOC_MSG_SECTION_TYPE 1

#define MAX_BSON_OBJECTS 10



PGDLLEXPORT int main_proxy(void);

void accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
void read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
void write_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
bool insert_to_postgres(const char *json_query);
void process_message(uint32_t response_to, unsigned char *buffer, unsigned char response[BUFFER_SIZE]);
//void parse_mongodb_packet(char *buffer, char **json_strings[MAX_BSON_OBJECTS]); 
void parse_mongodb_packet(char *buffer, char **query_string, char **parameter_string);
void parse_query(char *buffer);
//int parse_message(char *buffer, char *(*json_strings[MAX_BSON_OBJECTS]));
int parse_message(char *buffer, char **query_string, char **parameter_string);
int parse_bson_object(char *my_data, bson_t **my_bson);
ssize_t get_str_len_from_doc_seq(char *doc_seq);

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

void read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    unsigned char buffer[BUFFER_SIZE];
    ssize_t read;

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

/*
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
    */


    char **query_string = (char **) malloc(sizeof(char*));
    char **parameter_string = (char **) malloc(sizeof(char*));

    parse_mongodb_packet(buffer, query_string, parameter_string);


    free(*query_string);
    free(*parameter_string);
    free(query_string);
    free(parameter_string);
   
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





void parse_mongodb_packet(char *buffer, char **query_string, char **parameter_string) {
    //header
    u_int32_t msg_length = 0;
    u_int32_t request_id = 0;
    u_int32_t response_to = 0;
    u_int32_t op_code = 0;    

    msg_length = ((u_int32_t*)buffer)[0];
    request_id = ((u_int32_t*)buffer)[1];
    response_to = ((u_int32_t*)buffer)[2];
    op_code = ((u_int32_t*)buffer)[3];    

    switch (op_code) {
    case OP_QUERY:
        //parse_query(buffer);
        break;
    case OP_MSG:
    
        parse_message(buffer, query_string, parameter_string);
        break;
    case OP_REPLY:
        /* code */
        break;
    default:
        perror("UNKNOWN OP_CODE\n");
        return;
    }
    
}


/**
 * return 0 if everything is successful
 * return -1 if not (for example, if smth with length of char *my_data)
 */
int parse_message(char *buffer, char **query_string, char **parameter_string) {
    u_int32_t flags = ((u_int32_t*)buffer)[4];
    int overall_sections_start_bit = 20; //because of mongodb protocol
    int overall_sections_end_bit = ((u_int32_t*)buffer)[0]; //msg_length
    u_int32_t msg_length = ((u_int32_t*)buffer)[0]; //msg_length
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
        checksum = ((u_int32_t*)buffer)[msg_length / sizeof(u_int32_t) - 1]; //the checksum
    }


    section_start = overall_sections_start_bit;

    for (; section_start < overall_sections_end_bit;) { 
        
        int add_to_section_start = 0;
        section_kind = buffer[section_start];
        section_start ++; //bc we got section kind

        switch (section_kind)
        {
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
            add_to_section_start += ((u_int32_t*)(buffer + section_start))[0];
            break;
        case DOC_MSG_SECTION_TYPE:
        /**
         * A Document Sequence section contains:
         * int32 - size of the section
         * cstring - Document sequence identifier.
         * Zero or more BSON objects
         * */
            u_int32_t size_section = ((u_int32_t*)(buffer + section_start))[0];
            // YES, it starts with the section start, not by cstring start - look at func realization
            ssize_t doc_string_len = get_str_len_from_doc_seq(buffer + section_start);
            if (doc_string_len < 0) {
                elog(ERROR, "UNKNOWN PROBLEM WITH STRING");
                return -1; //it means smth wrong with string => smth wrong with buffer/packet
            }

            //this is wwhere bsons located
            add_to_section_start += doc_string_len + 4; //size of the string + size of the section
            int j = 0;
            for (j = section_start + add_to_section_start; j < section_start + size_section; ) {
                u_int32_t my_bson_size = ((u_int32_t*)(buffer + j))[0];
                int flag_local = parse_bson_object((char *)(buffer + j), &(b[next_bson_to_get]));
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
    char *str = bson_as_relaxed_extended_json (b[0], &len_of_string);
    *query_string = (char *) malloc(len_of_string + 1);
    memset(*query_string, 0, len_of_string + 1);
    memcpy(*query_string, str, len_of_string);
    bson_free (str);

    

    **jsons = (char **)malloc((next_bson_to_get - 1) * sizeof(char *));


    for (int i = 1; i < next_bson_to_get; ++i) {
        size_t len_of_string = 0;
        jsons[i] = bson_as_relaxed_extended_json (b[i], &len_of_string);

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
            if (i +1 != next_bson_to_get) {

                memcpy((char *)(*parameter_string + now), jsons[i], strlen(jsons[i]));
                now += strlen(jsons[i]);
                memcpy((char *)(*parameter_string + now), ", ", 2);
                now += 2;
            } else {
                memcpy((char *)(*parameter_string + now), jsons[i], strlen(jsons[i]));
                now += strlen(jsons[i]);
                memcpy((char *)(*parameter_string + now), "]", 1);
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
    size_t my_data_len = ((u_int32_t*)my_data)[0];
    *my_bson = bson_new_from_data (my_data, my_data_len);
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
    u_int32_t section_size = ((u_int32_t*)doc_seq)[0];
    ssize_t count = 0;

    for (int i = 4; i < (int)section_size; ++i) {
        if (doc_seq[i] == '\0') {
            count++;
            return count;
        }
        count++;
    }

    return -1;
}
