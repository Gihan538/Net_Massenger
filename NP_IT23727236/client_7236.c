#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define BUFFER_SIZE 4096
#define DEFAULT_PORT 9456

int sock = 0;

void *receive_handler(void *arg) {
    (void)arg;
    char buffer[BUFFER_SIZE];
    while (1) {
        size_t idx = 0;
        int disconnected = 0;
        while (idx < sizeof(buffer) - 1) {
            char c;
            ssize_t n = recv(sock, &c, 1, 0);
            if (n <= 0) {
                disconnected = 1;
                break;
            }
            if (c == '\n') break;
            if (c != '\r') buffer[idx++] = c;
        }
        if (disconnected) {
            printf("\n[Disconnected from server]\n");
            exit(0);
        }
        buffer[idx] = '\0';
        printf("\n< %s\n> ", buffer);
        fflush(stdout);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    int port = DEFAULT_PORT;
    const char *ip = "127.0.0.1";
    if (argc >= 2) ip = argv[1];
    if (argc >= 3) port = atoi(argv[2]);

    struct sockaddr_in serv_addr;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation error");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        perror("Invalid address");
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Connection Failed");
        return -1;
    }

    printf("Connected to NetMessenger server on %s:%d\n", ip, port);
    pthread_t recv_thread;
    pthread_create(&recv_thread, NULL, receive_handler, NULL);

    char send_buf[BUFFER_SIZE];
    while (1) {
        printf("> ");
        fflush(stdout);
        if (!fgets(send_buf, sizeof(send_buf), stdin)) break;

        // SENDFILE interception
        if (strncmp(send_buf, "SENDFILE", 8) == 0) {
            char target[32], filepath[256];
            if (sscanf(send_buf, "%*s %31s %255s", target, filepath) == 2) {
                FILE *f = fopen(filepath, "rb");
                if (!f) {
                    printf("Local file '%s' not found.\n", filepath);
                    continue;
                }
                fseek(f, 0, SEEK_END);
                long fsize = ftell(f);
                fseek(f, 0, SEEK_SET);

                char *basename = strrchr(filepath, '/');
                basename = basename ? basename + 1 : filepath;

                char cmd[512];
                snprintf(cmd, sizeof(cmd), "SENDFILE %s %s %ld\n", target, basename, fsize);
                send(sock, cmd, strlen(cmd), 0);

                char file_buf[BUFFER_SIZE];
                size_t nbytes;
                while ((nbytes = fread(file_buf, 1, sizeof(file_buf), f)) > 0) {
                    send(sock, file_buf, nbytes, 0);
                }
                fclose(f);
                continue;
            }
        }

        // Standard command dispatch
        send(sock, send_buf, strlen(send_buf), 0);
        if (strncmp(send_buf, "QUIT", 4) == 0) break;
    }

    close(sock);
    return 0;
}
