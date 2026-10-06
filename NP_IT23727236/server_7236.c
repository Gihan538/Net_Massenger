#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/stat.h>

#define REG_NO "IT23727236"
#define PORT 13236
#define NID_TAG "NID:7272"
#define LOG_FILE "netmsg_IT23727236.log"
#define STORAGE_BASE "./storage/IT23727236"

#define BUFFER_SIZE 4096
#define MAX_CLIENTS 64
#define MAX_ROOMS 32
#define MAX_ROOM_MEMBERS 32

typedef struct {
    int sockfd;
    char username[32];
    int registered;
} Client;

typedef struct {
    char name[32];
    int member_fds[MAX_ROOM_MEMBERS];
    int member_count;
} Room;

Client clients[MAX_CLIENTS];
Room rooms[MAX_ROOMS];
int room_count = 0;
pthread_mutex_t state_mutex = PTHREAD_MUTEX_INITIALIZER;

void log_event(const char *event) {
    FILE *f = fopen(LOG_FILE, "a");
    if (!f) return;
    time_t now = time(NULL);
    char tbuf[32];
    strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", localtime(&now));
    fprintf(f, "[%s] %s\n", tbuf, event);
    fclose(f);
}

void send_response(int fd, const char *prefix, const char *msg) {
    char out[BUFFER_SIZE];
    snprintf(out, sizeof(out), "%s %s %s\n", prefix, msg, NID_TAG);
    send(fd, out, strlen(out), 0);
}

void send_raw(int fd, const char *msg) {
    send(fd, msg, strlen(msg), 0);
}

void create_directory_p(const char *path) {
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0777);
            *p = '/';
        }
    }
    mkdir(tmp, 0777);
}

int read_line(int sock, char *buf, size_t maxlen) {
    size_t idx = 0;
    while (idx < maxlen - 1) {
        char c;
        ssize_t n = recv(sock, &c, 1, 0);
        if (n <= 0) return -1;
        if (c == '\n') break;
        if (c != '\r') buf[idx++] = c;
    }
    buf[idx] = '\0';
    return (int)idx;
}

void disconnect_client(int fd) {
    char left_user[32] = "";
    pthread_mutex_lock(&state_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].sockfd == fd) {
            if (clients[i].registered) {
                strncpy(left_user, clients[i].username, sizeof(left_user));
            }
            clients[i].sockfd = -1;
            clients[i].registered = 0;
            clients[i].username[0] = '\0';
            break;
        }
    }
    // Remove from rooms
    for (int i = 0; i < room_count; i++) {
        for (int j = 0; j < rooms[i].member_count; j++) {
            if (rooms[i].member_fds[j] == fd) {
                for (int k = j; k < rooms[i].member_count - 1; k++) {
                    rooms[i].member_fds[k] = rooms[i].member_fds[k+1];
                }
                rooms[i].member_count--;
                break;
            }
        }
    }
    pthread_mutex_unlock(&state_mutex);
    close(fd);

    if (strlen(left_user) > 0) {
        char notify[BUFFER_SIZE], logmsg[BUFFER_SIZE];
        snprintf(notify, sizeof(notify), "MSG BCAST SERVER User '%s' has disconnected.\n", left_user);
        snprintf(logmsg, sizeof(logmsg), "DISCONNECT: User '%s'", left_user);
        log_event(logmsg);
        
        pthread_mutex_lock(&state_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].sockfd > 0 && clients[i].registered) {
                send_raw(clients[i].sockfd, notify);
            }
        }
        pthread_mutex_unlock(&state_mutex);
    }
}

void *client_handler(void *arg) {
    int fd = *(int *)arg;
    free(arg);
    char line[BUFFER_SIZE];

    while (1) {
        if (read_line(fd, line, sizeof(line)) < 0) {
            disconnect_client(fd);
            break;
        }
        if (strlen(line) == 0) continue;

        char cmd[32];
        sscanf(line, "%31s", cmd);

        if (strcmp(cmd, "REGISTER") == 0) {
            char uname[32];
            if (sscanf(line, "%*s %31s", uname) < 1) {
                send_response(fd, "ERR 005", "INVALID_ARGUMENTS");
                continue;
            }
            pthread_mutex_lock(&state_mutex);
            int exists = 0, current_idx = -1;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].sockfd == fd) current_idx = i;
                if (clients[i].sockfd > 0 && strcmp(clients[i].username, uname) == 0) exists = 1;
            }
            if (exists) {
                pthread_mutex_unlock(&state_mutex);
                send_response(fd, "ERR 001", "USERNAME_TAKEN");
            } else if (current_idx != -1) {
                strncpy(clients[current_idx].username, uname, 31);
                clients[current_idx].registered = 1;
                pthread_mutex_unlock(&state_mutex);
                
                send_response(fd, "OK", "REGISTERED");
                char logm[128];
                snprintf(logm, sizeof(logm), "REGISTER: User '%s' connected", uname);
                log_event(logm);

                char notify[BUFFER_SIZE];
                snprintf(notify, sizeof(notify), "MSG BCAST SERVER User '%s' joined.\n", uname);
                pthread_mutex_lock(&state_mutex);
                for (int i = 0; i < MAX_CLIENTS; i++) {
                    if (clients[i].sockfd > 0 && clients[i].sockfd != fd && clients[i].registered) {
                        send_raw(clients[i].sockfd, notify);
                    }
                }
                pthread_mutex_unlock(&state_mutex);
            } else {
                pthread_mutex_unlock(&state_mutex);
            }
        } 
        else if (strcmp(cmd, "LIST") == 0) {
            char ulist[BUFFER_SIZE] = "";
            pthread_mutex_lock(&state_mutex);
            int first = 1;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].sockfd > 0 && clients[i].registered) {
                    if (!first) strcat(ulist, ",");
                    strcat(ulist, clients[i].username);
                    first = 0;
                }
            }
            pthread_mutex_unlock(&state_mutex);
            char payload[BUFFER_SIZE];
            snprintf(payload, sizeof(payload), "USERS %s", ulist);
            send_response(fd, "OK", payload);
        }
        else if (strcmp(cmd, "BCAST") == 0) {
            char *msg = strchr(line, ' ');
            if (!msg) { send_response(fd, "ERR 005", "INVALID_ARGUMENTS"); continue; }
            msg++;
            
            char sender[32] = "Anon";
            pthread_mutex_lock(&state_mutex);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].sockfd == fd) strncpy(sender, clients[i].username, 31);
            }
            char forward[BUFFER_SIZE];
            snprintf(forward, sizeof(forward), "MSG BCAST %s %s\n", sender, msg);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].sockfd > 0 && clients[i].sockfd != fd && clients[i].registered) {
                    send_raw(clients[i].sockfd, forward);
                }
            }
            pthread_mutex_unlock(&state_mutex);
            send_response(fd, "OK", "SENT");
            log_event("BCAST message forwarded");
        }
        else if (strcmp(cmd, "PMSG") == 0) {
            char target[32], msg[BUFFER_SIZE];
            if (sscanf(line, "%*s %31s %[^\n]", target, msg) < 2) {
                send_response(fd, "ERR 005", "INVALID_ARGUMENTS");
                continue;
            }
            char sender[32] = "";
            int target_fd = -1;
            pthread_mutex_lock(&state_mutex);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].sockfd == fd) strncpy(sender, clients[i].username, 31);
                if (clients[i].sockfd > 0 && strcmp(clients[i].username, target) == 0) target_fd = clients[i].sockfd;
            }
            pthread_mutex_unlock(&state_mutex);

            if (target_fd == -1) {
                send_response(fd, "ERR 002", "USER_NOT_FOUND");
            } else {
                char forward[BUFFER_SIZE];
                snprintf(forward, sizeof(forward), "MSG PRIV %s %s\n", sender, msg);
                send_raw(target_fd, forward);
                send_response(fd, "OK", "SENT");
            }
        }
        else if (strcmp(cmd, "JOIN") == 0) {
            char rname[32];
            if (sscanf(line, "%*s %31s", rname) < 1) {
                send_response(fd, "ERR 005", "INVALID_ARGUMENTS");
                continue;
            }
            pthread_mutex_lock(&state_mutex);
            int ridx = -1;
            for (int i = 0; i < room_count; i++) {
                if (strcmp(rooms[i].name, rname) == 0) { ridx = i; break; }
            }
            if (ridx == -1 && room_count < MAX_ROOMS) {
                ridx = room_count++;
                strncpy(rooms[ridx].name, rname, 31);
                rooms[ridx].member_count = 0;
            }
            if (ridx != -1) {
                int in_room = 0;
                for (int m = 0; m < rooms[ridx].member_count; m++) {
                    if (rooms[ridx].member_fds[m] == fd) in_room = 1;
                }
                if (!in_room && rooms[ridx].member_count < MAX_ROOM_MEMBERS) {
                    rooms[ridx].member_fds[rooms[ridx].member_count++] = fd;
                }
            }
            pthread_mutex_unlock(&state_mutex);
            char payload[64];
            snprintf(payload, sizeof(payload), "JOINED %s", rname);
            send_response(fd, "OK", payload);
        }
        else if (strcmp(cmd, "LEAVE") == 0) {
            char rname[32];
            if (sscanf(line, "%*s %31s", rname) < 1) {
                send_response(fd, "ERR 005", "INVALID_ARGUMENTS");
                continue;
            }
            pthread_mutex_lock(&state_mutex);
            int found = 0;
            for (int i = 0; i < room_count; i++) {
                if (strcmp(rooms[i].name, rname) == 0) {
                    found = 1;
                    for (int m = 0; m < rooms[i].member_count; m++) {
                        if (rooms[i].member_fds[m] == fd) {
                            for (int k = m; k < rooms[i].member_count - 1; k++) {
                                rooms[i].member_fds[k] = rooms[i].member_fds[k+1];
                            }
                            rooms[i].member_count--;
                            break;
                        }
                    }
                    break;
                }
            }
            pthread_mutex_unlock(&state_mutex);
            if (!found) send_response(fd, "ERR 003", "ROOM_NOT_FOUND");
            else {
                char payload[64];
                snprintf(payload, sizeof(payload), "LEFT %s", rname);
                send_response(fd, "OK", payload);
            }
        }
        else if (strcmp(cmd, "ROOMS") == 0) {
            char rlist[BUFFER_SIZE] = "";
            pthread_mutex_lock(&state_mutex);
            for (int i = 0; i < room_count; i++) {
                if (i > 0) strcat(rlist, ",");
                strcat(rlist, rooms[i].name);
            }
            pthread_mutex_unlock(&state_mutex);
            char payload[BUFFER_SIZE];
            snprintf(payload, sizeof(payload), "ROOMS %s", rlist);
            send_response(fd, "OK", payload);
        }
        else if (strcmp(cmd, "RMSG") == 0) {
            char rname[32], msg[BUFFER_SIZE];
            if (sscanf(line, "%*s %31s %[^\n]", rname, msg) < 2) {
                send_response(fd, "ERR 005", "INVALID_ARGUMENTS");
                continue;
            }
            char sender[32] = "";
            pthread_mutex_lock(&state_mutex);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].sockfd == fd) strncpy(sender, clients[i].username, 31);
            }
            int ridx = -1;
            for (int i = 0; i < room_count; i++) {
                if (strcmp(rooms[i].name, rname) == 0) { ridx = i; break; }
            }
            if (ridx == -1) {
                pthread_mutex_unlock(&state_mutex);
                send_response(fd, "ERR 003", "ROOM_NOT_FOUND");
            } else {
                char forward[BUFFER_SIZE];
                snprintf(forward, sizeof(forward), "MSG ROOM %s %s %s\n", rname, sender, msg);
                for (int m = 0; m < rooms[ridx].member_count; m++) {
                    if (rooms[ridx].member_fds[m] != fd) {
                        send_raw(rooms[ridx].member_fds[m], forward);
                    }
                }
                pthread_mutex_unlock(&state_mutex);
                send_response(fd, "OK", "SENT");
            }
        }
        else if (strcmp(cmd, "SENDFILE") == 0) {
            char target[32], fname[128];
            long filesize = 0;
            if (sscanf(line, "%*s %31s %127s %ld", target, fname, &filesize) < 3 || filesize < 0) {
                send_response(fd, "ERR 005", "INVALID_ARGUMENTS");
                continue;
            }

            char sender[32] = "unknown";
            pthread_mutex_lock(&state_mutex);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].sockfd == fd) strncpy(sender, clients[i].username, 31);
            }
            pthread_mutex_unlock(&state_mutex);

            // Setup storage path
            char dir_path[512], file_path[512];
            snprintf(dir_path, sizeof(dir_path), "%s/%s", STORAGE_BASE, sender);
            create_directory_p(dir_path);
            snprintf(file_path, sizeof(file_path), "%s/%s", dir_path, fname);

            FILE *fp = fopen(file_path, "wb");
            long remaining = filesize;
            char fbuf[BUFFER_SIZE];
            while (remaining > 0) {
                size_t to_read = (remaining > (long)sizeof(fbuf)) ? sizeof(fbuf) : (size_t)remaining;
                ssize_t n = recv(fd, fbuf, to_read, 0);
                if (n <= 0) break;
                if (fp) fwrite(fbuf, 1, n, fp);
                remaining -= n;
            }
            if (fp) fclose(fp);

            char res[256];
            snprintf(res, sizeof(res), "FILE RECEIVED %s", fname);
            send_response(fd, "OK", res);

            char logmsg[512];
            snprintf(logmsg, sizeof(logmsg), "FILE_TRANSFER: %s sent %s (%ld bytes) to %s", sender, fname, filesize, target);
            log_event(logmsg);
        }
        else if (strcmp(cmd, "QUIT") == 0) {
            send_response(fd, "OK", "BYE");
            disconnect_client(fd);
            break;
        }
        else {
            send_response(fd, "ERR 000", "UNKNOWN_COMMAND");
        }
    }
    return NULL;
}

int main() {
    for (int i = 0; i < MAX_CLIENTS; i++) clients[i].sockfd = -1;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    listen(server_fd, 10);
    printf("NetMessenger Server listening on port %d [%s]...\n", PORT, NID_TAG);
    log_event("SERVER_START: Listening started");

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int *new_sock = malloc(sizeof(int));
        *new_sock = accept(server_fd, (struct sockaddr *)&client_addr, &addrlen);
        if (*new_sock < 0) {
            free(new_sock);
            continue;
        }

        pthread_mutex_lock(&state_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].sockfd == -1) {
                clients[i].sockfd = *new_sock;
                clients[i].registered = 0;
                break;
            }
        }
        pthread_mutex_unlock(&state_mutex);

        pthread_t tid;
        pthread_create(&tid, NULL, client_handler, new_sock);
        pthread_detach(tid);
    }
    close(server_fd);
    return 0;
}
