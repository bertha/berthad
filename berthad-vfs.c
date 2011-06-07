#define _GNU_SOURCE

#include <arpa/inet.h>
#include <linux/falloc.h>

#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>

#include <stdlib.h>
#include <signal.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <stdio.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>

#include <glib.h>

#ifndef CFG_BACKLOG
#define CFG_BACKLOG 10
#endif
#ifndef CFG_LIST_BUFFER
#define CFG_LIST_BUFFER 1280
#endif
#ifndef CFG_PUT_BUFFER
#define CFG_PUT_BUFFER 4096
#endif
#ifndef CFG_GET_BUFFER
#define CFG_GET_BUFFER 65536
#endif
#ifndef CFG_DATADIR_WIDTH
#define CFG_DATADIR_WIDTH 2
#endif
#ifndef CFG_DATADIR_DEPTH
#define CFG_DATADIR_DEPTH 2
#endif

/*
 * States of connections
 *
 * cmd   states
 * LIST  INITIAL => LIST
 * PUT   INITIAL =>         PUT => PUT2
 * SPUT  INITIAL => SPUT => PUT => PUT2
 * GET   INITIAL => GET  =>         GET2
 * SGET  INITIAL => GET  => SGET => GET2
 * QUIT  INITIAL
 */

/* Connection has been accepted.  We wait for the command byte. */
#define BCONN_STATE_INITIAL     0

/* We are waiting to write keys to the socket */
#define BCONN_STATE_LIST        1

/* We are waiting for data on the socket or on the VFS to write this
 * data a temporary file */
#define BCONN_STATE_PUT         2

/* We are waiting to write the key back to the socket */
#define BCONN_STATE_PUT2        3

/* We are waiting for the key on the socket */
#define BCONN_STATE_GET         4

/* We are waiting to splice the file to the socket*/
#define BCONN_STATE_GET2        5

/* We are waiting to for the length on the socket */
#define BCONN_STATE_SPUT        6

/* We are waiting to send the length of the file over the socket */
#define BCONN_STATE_SGET        7

/*
 * Commands bytes in the bertha protocol
 */
#define BERTHA_LIST     ((guint8)0)
#define BERTHA_PUT      ((guint8)1)
#define BERTHA_GET      ((guint8)2)
#define BERTHA_QUIT     ((guint8)3)
#define BERTHA_SPUT     ((guint8)4)
#define BERTHA_SGET     ((guint8)5)
#define BERTHA_NONE     ((guint8)255)

typedef struct {
        /* listening socket */
        int lsock;

        /* list of connections */
        GList *conns;

        /* Are we running? */
        gboolean running;

        /* path to the data directory */
        char* dataPath;

        /* path to the temporary directory */
        char* tmpPath;

        /* fd_sets for select (2) */
        fd_set r_fds;
        fd_set w_fds;
        int highest_fd;

        /* statistics */
        gsize n_conns; /* number of connections accepted */
        gsize n_GET_sent; /* number of bytes sent for GETs */
        gsize n_PUT_received; /* number of bytes received for PUTs */
        gsize n_cycle; /* number of non-trivial main loop cycles */

        /* threadpool for async. fadvise and fallocate */
        GThreadPool* threadpool;
} BProgram;

typedef struct {
        /* The state of this connection */
        int state;
        
        /* the socket */
        int sock;

        /* remote address */
        struct sockaddr addr;
        socklen_t addrlen;

        /* pointer to additional data associated with the state 
         * of the connection */
        gpointer state_data;

        /* Number of this connection */
        gsize n;

        /* the command send by the client */
        guint8 cmd;

        /* Number of threads working on this object */
        gint n_threads;

        /* Condition/mutex pair  used to join threads, if there are any. */
        GCond* threads_cond;
        GMutex* threads_mutex;
} BConn;

typedef struct {
        /* Partial hexadecimal key */
        GString* key;

        /* Path to the directory */
        GString* path;

        /* Depth of this directory */
        guint8 depth;
} BConnListEntry;

typedef struct {
        /* handle of the current directory enumerated */
        DIR* cdir_handle;

        /* entry of the current directory enumerated */
        BConnListEntry* cdir;

        /* Stack of directories names to search */
        GList* dirs;

        /* The send buffer */
        GByteArray* buf;
} BConnList;

typedef struct {
        /* checksum state */
        GChecksum* checksum;

        /* temporary filename */
        GString* tmp_fn;

        /* file descriptor of target file */
        int fd;

        /* Is there data waiting on the socket? */
        gboolean socket_ready;

        /* Have we received an EOS on the socket? */
        gboolean socket_eos;

        /* Can we write data to the file? */
        gboolean file_ready;

        /* For BCONN_STATE_PUT this is the file write buffer.
         * For BCONN_STATE_PUT2 this contains the hash to send back.
         * For BCONN_STATE_SPUT this is the file length read buffer. */
        GByteArray* buf;

        /* The size advised by the client in case of a SPUT */
        guint64 advised_size;
} BConnPut;

typedef struct {
        /* The buffer that holds the key */
        GByteArray* buf;
        
        /* The file descriptor of the file to send */
        int fd;

        /* The pipe used to splice from the file to the socket */
        int pipe[2];

        /* Can we send data over the socket? */
        gboolean socket_ready;

        /* Is the file depleted? */
        gboolean file_eos;

        /* Can we read data from the file? */
        gboolean file_ready;

        /* The number of bytes in the pipe */
        size_t in_buffer;
} BConnGet;

/* Call fadvise for file read for GET async, since it may block */
#define BJOB_FADVISE    0

/* Call fallocate for file written by PUT async, since it will block */
#define BJOB_FALLOCATE  1

typedef struct {
        /* type of the job */
        guint8 type;

        /* pointer to the program */
        BProgram* prog;
} BJob;

typedef struct {
        BJob parent;

        /* pointer to the connection */
        BConn* conn;
} BJobConn;

/*
 * Converts a single hexadecimal digit to a byte
 */
guint8 hex_to_uint4 (char hex)
{
        return ('0' <= hex && hex <= '9') ? hex - '0' : hex - 'a' + 10;
}

/*
 * Converts a byte in the range [0, 15] to a hexadecimal digit
 */
char uint4_to_hex (guint8 byte)
{
        return (byte < 10) ? byte + '0' : byte - 10 + 'a';
}

/*
 * Returns whether the path specified is a direcotry
 */
gboolean path_is_dir (char* path)
{
        struct stat st;
        int ret;

        ret = stat(path, &st);
        if (ret != 0)
                return FALSE;
        return S_ISDIR(st.st_mode);
}

/*
 * Ensures the specified directory exists.
 */
void mkdirs (char* path)
{
        char* path2 = g_strdup(path);
        char* tmp;
        gsize len;
        int ret;

        len = strlen(path2);

        /* strip trailing / */
        if(path2[len-1] == '/') {
                path2[len-1] = '\0';
                len--;
        }

        /* search down until we find a directory that does exist */
        while (!path_is_dir(path2)) {
                tmp = strrchr(path2, '/');
                g_assert(tmp != NULL);
                tmp[0] = '\0';
        }

        /* work up to create them */
        while (strlen(path2) < len) {
                path2[strlen(path2)] = '/';
                ret = mkdir(path2, 0700);
                g_assert(ret == 0);
        }

        g_free(path2);
}


/*
 * Converts a hexadecimal string to an bytearray
 * Free with g_slice_free1
 */
guint8* hex_to_buf (char* hex, gsize* size)
{
        gsize i;
        gsize len = strlen(hex);
        guint8* buf;
        
        g_assert(size != NULL);
        g_assert(strlen(hex) % 2 == 0);

        *size = len / 2;

        buf = g_slice_alloc(*size);

        for (i = 0; i < *size; i++)
                buf[i] = hex_to_uint4(hex[2*i]) * 16 + hex_to_uint4(hex[2*i+1]);

        return buf;
}

/*
 * Check whether a string is a string of hexadecimal characters
 */
gboolean is_hex (gchar* str)
{
        gsize i, len = strlen(str);

        for (i = 0; i < len; i++)
                if ((str[i] < 'a' || 'f' < str[i]) 
                                && (str[i] < '0' || '9' < str[i]))
                        return FALSE;

        return TRUE;
}

/*
 * Converts a buffer to a hexadecimal string
 * Free with g_slice_free1
 */
gchar* buf_to_hex (guint8* buf, gsize size)
{
        gssize i;
        char* str = g_slice_alloc(size * 2 + 1);
        
        str[size* 2] = 0;
        
        for (i = 0; i < size; i++) {
                str[2*i] = uint4_to_hex(buf[i] / 16);
                str[2*i+1] = uint4_to_hex(buf[i] % 16);
        }

        return str;
}

/*
 * Returns the path to the file for the blob with key <key>
 * It may create directories that are missing
 */
GString* key_to_path (BProgram* prog, char* key)
{
        GString* fn = g_string_sized_new(128);
        int i;

        g_string_printf(fn, "%s/", prog->dataPath);

        for (i = 0; i < CFG_DATADIR_DEPTH; i++) {
                char* bit = g_strndup(key, CFG_DATADIR_WIDTH);
                g_string_append_printf(fn, "%s/", bit);
                g_free(bit);
                key += CFG_DATADIR_WIDTH;
        }

        mkdirs(fn->str);

        g_string_append(fn, key);
        return fn;
}

/*
 * Creates a human readable string from a struct sockaddr
 */
GString* sockaddr_to_gstring (struct sockaddr* sa)
{
        gsize len = MAX(INET_ADDRSTRLEN, INET6_ADDRSTRLEN);
        char* buf = g_alloca(len);
        gpointer src;
        const char* ret;
        
        if (sa->sa_family == AF_INET)
                src = &(((struct sockaddr_in *)sa)->sin_addr);
        else if (sa->sa_family == AF_INET6)
                src = &(((struct sockaddr_in6 *)sa)->sin6_addr);
        else
                g_assert_not_reached();

        ret = inet_ntop(sa->sa_family, src, buf, len);
        g_assert(ret == buf);

        return g_string_new(buf);
}

/*
 * Sets a fd in non-blocking mode
 */
void fd_set_nonblocking(int s)
{
        int flags = fcntl(s, F_GETFL, 0);
        int ret = fcntl(s, F_SETFL, flags | O_NONBLOCK);
        g_assert(ret == 0);
}

/*
 * Logs a message for a connection
 */
void conn_log(BConn* conn, const char* format, ...)
{
        va_list arglist;
        GString* msg = g_string_sized_new(128);
        struct timeval tv;
        guint64 microts;

        /* Get microseconds timestamp */
        gettimeofday(&tv, NULL);
        microts = (guint64)tv.tv_sec * 1000000 + tv.tv_usec;

        /* Get their message */
        va_start(arglist, format);
        g_string_vprintf(msg, format, arglist);
        va_end(arglist);

        /* Print our message */
        printf("%ld %ld %s\n", conn->n, microts, msg->str);

        g_string_free(msg, TRUE);
}

/*
 * Closes and frees a connection
 */
void conn_close(BProgram* prog, GList* lhconn)
{
        BConn* conn = lhconn->data;

        /* Check if there are still threads working on this connection.
         * And if so, wait on them */
        if (conn->threads_mutex != NULL) {
                g_assert(conn->threads_cond != NULL);
                g_mutex_lock(conn->threads_mutex);
                if(conn->n_threads > 0)
                        g_cond_wait(conn->threads_cond, conn->threads_mutex);
                g_mutex_unlock(conn->threads_mutex);
                g_assert(conn->n_threads == 0);
        }

        conn_log(conn, "close");

        if (conn->state == BCONN_STATE_LIST) {
                BConnList* data = conn->state_data;
                if (data->cdir_handle)
                        closedir(data->cdir_handle);
                if (data->dirs) {
                        GList* lhdir;
                        for (lhdir = data->dirs; lhdir;
                                        lhdir = g_list_next(lhdir)) {
                                BConnListEntry* e = lhdir->data;
                                g_string_free(e->key, TRUE);
                                g_string_free(e->path, TRUE);
                                g_slice_free(BConnListEntry, lhdir->data);
                        }
                        g_list_free(data->dirs);
                }
                if (data->cdir) {
                        g_string_free(data->cdir->key, TRUE);
                        g_string_free(data->cdir->path, TRUE);
                        g_slice_free(BConnListEntry, data->cdir);
                }
                if (data->buf)
                        g_byte_array_unref(data->buf);
                g_slice_free(BConnList, data);
        } else if (conn->state == BCONN_STATE_PUT 
                        || conn->state == BCONN_STATE_PUT2
                        || conn->state == BCONN_STATE_SPUT) {
                BConnPut* data = conn->state_data;
                if (data->checksum)
                        g_checksum_free(data->checksum);
                if (data->fd)
                        close (data->fd);
                if (data->tmp_fn) {
                        unlink(data->tmp_fn->str);
                        g_string_free(data->tmp_fn, TRUE);
                }
                if (data->buf)
                        g_byte_array_unref(data->buf);
                g_slice_free(BConnPut, data);
        } else if (conn->state == BCONN_STATE_GET
                        || conn->state == BCONN_STATE_GET2
                        || conn->state == BCONN_STATE_SGET) {
                BConnGet* data = conn->state_data;
                if (data->buf)
                        g_byte_array_unref(data->buf);
                if (data->fd)
                        close(data->fd);
                if (data->pipe[0])
                        close(data->pipe[0]);
                if (data->pipe[1])
                        close(data->pipe[1]);
                g_slice_free(BConnGet, data);
        }

        if (conn->threads_mutex)
                g_mutex_free(conn->threads_mutex);
        if (conn->threads_cond)
                g_cond_free(conn->threads_cond);

        /* Close the socket */
        if (conn->sock)
                close(conn->sock); 

        /* Free the connection */
        prog->conns = g_list_delete_link(prog->conns, lhconn);
        g_slice_free(BConn, conn);
}

/*
 * Starts a BJOB for a connection
 * Used for async. fallocate and fadvise
 */
void  conn_start_job (BProgram* prog, BConn* conn, guint8 type)
{
        GError* err = NULL;
        BJobConn* job = g_slice_new0(BJobConn);

        job->parent.type = type;
        job->conn = conn;

        /* initialize mutex and condition if we have not already */
        if (conn->threads_mutex == NULL) {
                g_assert(conn->threads_cond == NULL);
                g_assert(conn->n_threads == 0);
                conn->threads_mutex = g_mutex_new();
                conn->threads_cond = g_cond_new();
        }

        g_mutex_lock(conn->threads_mutex);
        conn->n_threads++;
        g_mutex_unlock(conn->threads_mutex);
        
        /* run it */
        g_thread_pool_push(prog->threadpool, job, &err);
        if (err)
                g_error("g_thread_pool_push: %s", err->message);
}


/*
 * Accepts a new connection
 */
void conn_accept(BProgram* prog)
{
        BConn* conn = g_slice_new0(BConn);
        GString* human_addr;
#ifdef SO_NOSIGPIPE
        int opt, ret;
#endif

        conn->n = prog->n_conns++;
        conn->cmd = BERTHA_NONE;
        conn->addrlen = sizeof(conn->addr);

        /* accept the connection */
        conn->sock = accept(prog->lsock, &conn->addr, &conn->addrlen);
        g_assert(conn->sock >= 0);
        human_addr = sockaddr_to_gstring(&conn->addr);
        conn_log(conn, "accepted %s", human_addr->str);
        g_string_free(human_addr, TRUE);

        /* set the socket to non-blocking */
        fd_set_nonblocking(conn->sock);

#ifdef SO_NOSIGPIPE
        /* if possible, prevent SIGPIPE signals being raised by closed
         * sockets. */
        opt = 1;
        ret = setsockopt(conn->sock, SOL_SOCKET, SO_NOSIGPIPE,
                                &opt, sizeof(opt));
        g_assert(ret == 0);
#endif

        /* store it */
        conn->state = BCONN_STATE_INITIAL;
        prog->conns = g_list_prepend(prog->conns, conn);
}

/*
 * Splice data from the file into a pipe and from that pipe into the socket
 */
void conn_handle_get2(BProgram* prog, GList* lhconn)
{
        BConn* conn = lhconn->data;
        BConnGet* data = conn->state_data;
        ssize_t spliced, to_splice;

        /* Is there data to splice from the file to the pipe?  And is
         * there enough room in the pipe?  Then splice some data! */
        if (data->file_ready && data->in_buffer < CFG_GET_BUFFER) {
splice_some_to_pipe:
                to_splice = CFG_GET_BUFFER - data->in_buffer;

                /* Splice from file to pipe */
                spliced = splice(data->fd, NULL, data->pipe[1], NULL,
                                 to_splice, SPLICE_F_MOVE |
                                            SPLICE_F_NONBLOCK |
                                            SPLICE_F_MORE);
                
                if (spliced == -1) {
                        if (errno == EAGAIN) {
                                g_warning("splice returned EAGAIN\n");
                                goto bail_splice_to_pipe;
                        }

                        perror("splice");
                        g_error("Splice failed?!\n");
                }

                data->file_ready = FALSE;

                /* Check for end of file */
                if (spliced == 0) {
                        data->file_eos = TRUE;

                        /* If we've got some data to write to the socket and
                         * the socket is ready, we will give it a try. */
                        if (data->socket_ready && data->in_buffer > 0)
                                goto splice_some_from_pipe;

                        /* Otherwise check whether we are done */
                        goto check_if_done;
                } else
                        data->in_buffer += spliced;
        }
bail_splice_to_pipe:

        /* Is there data to splice from the pipe to the socket?  And
         * is the socket ready? */
        if (data->socket_ready && data->in_buffer > 0) {
splice_some_from_pipe:
                /* Splice from pipe to socket */
                spliced = splice(data->pipe[0], NULL, conn->sock, NULL,
                                 data->in_buffer, SPLICE_F_MOVE |
                                                  SPLICE_F_NONBLOCK |
                                                  SPLICE_F_MORE);

                if (spliced == -1) {
                        if (errno == EAGAIN) {
                                g_warning("splice returned EAGAIN\n");
                                return;
                        }

                        /* socket has been closed */
                        if (errno == EPIPE) {
                                conn_log(conn, "EPIPE");
                                conn_close(prog, lhconn);
                                return;
                        }

                        perror("splice");
                        g_error("Splice failed?!\n");
                }
                g_assert(spliced > 0);

                prog->n_GET_sent += spliced;
                data->socket_ready = FALSE;
                data->in_buffer -= spliced;

                /* Check if we have enough room to read from the file*/
                if (data->file_ready)
                        goto splice_some_to_pipe;
                goto check_if_done;
        }
        return;

check_if_done:
        if (data->file_eos && data->in_buffer == 0) {
                /* We're done! */
                shutdown(conn->sock, SHUT_RDWR);
                conn_close(prog, lhconn);
        }
}

/*
 * Read the length from the BCONN_STATE_SPUT connection.
 */
void conn_handle_sput(BProgram* prog, GList* lhconn)
{
        BConn* conn = lhconn->data;
        BConnPut* data = conn->state_data;
        ssize_t received;
        guint8 buf2[8];
        guint64* size_ptr;

        received = recv(conn->sock, buf2, 8 - data->buf->len, 0);
        g_assert(received >= 0);

        /* Premature end of stream: ignore */
        if (received == 0) {
                g_warning("SPUT Premature end of stream\n");
                conn_close(prog, lhconn);
                return;
        }

        /* Shortcut for when we've received the length in one part */
        if (received == 8) {
                g_assert(data->buf->len == 0);
                size_ptr = (guint64*)buf2;
                goto got_size;
        }

        g_byte_array_append(data->buf, buf2, received);

        /* have we received the full key? */
        if (data->buf->len == 8) {
                size_ptr = (guint64*)data->buf->data;
                goto got_size;
        }
        return;

got_size:
        /* We got the size!  Now transition into BCONN_STATE_PUT. */
        if (TRUE) {
                guint64 size = GUINT64_FROM_LE(*size_ptr);

                /* Clear the buffer */
                if (data->buf->len > 0)
                        g_byte_array_remove_range(data->buf, 0, data->buf->len);

                conn_log(conn, "SPUT %s %ld", data->tmp_fn->str, size);

                /* Pre-allocate the file in a separate thread */
                data->advised_size = size;
                conn_start_job(prog, conn, BJOB_FALLOCATE);

                conn->state = BCONN_STATE_PUT;
        }
}

/*
 * Read the key from the BCONN_STATE_GET connection
 */
void conn_handle_get(BProgram* prog, GList* lhconn)
{
        BConn* conn = lhconn->data;
        BConnGet* data = conn->state_data;
        ssize_t received;
        guint8 buf2[32];
        guint8* key = NULL;

        received = recv(conn->sock, buf2, 32 - data->buf->len, 0);
        g_assert(received >= 0);

        /* Premature end of stream: ignore */
        if (received == 0) {
                g_warning("GET Premature end of stream\n");
                conn_close(prog, lhconn);
                return;
        }

        /* Shortcut for when we've received the key in one part */
        if (received == 32) {
                g_assert(data->buf->len == 0);
                key = buf2;
                goto got_key;
        }

        g_byte_array_append(data->buf, buf2, received);

        /* have we received the full key? */
        if (data->buf->len == 32) {
                key = data->buf->data;
                goto got_key;
        }
        return;

got_key:
        /* Transition into BCONN_STATE_GET2, where we will splice the file
         * into the socket.  First, we'll need to open the file. */
        if(TRUE) {
                int ret;
                GString* fn;
                char* hex_key = buf_to_hex(key, 32);
                
                /* Open the file */
                fn = key_to_path(prog, hex_key);
                data->fd = open(fn->str, O_RDONLY, 0);
                conn_log(conn, "%s %s", conn->cmd == BERTHA_SGET
                                        ? "SGET" : "GET", hex_key);
                g_string_free(fn, TRUE);
                g_slice_free1(65, hex_key);

                /* Advise the kernel on the access pattern */
                ret = posix_fadvise(data->fd, 0, 0, POSIX_FADV_SEQUENTIAL);
                g_assert(ret == 0);

                /* We call posix_fadvise with POSIX_FADV_WILLNEED in a
                 * separate thread, since it may block. */
                conn_start_job(prog, conn, BJOB_FADVISE);

                /* The file couldn't be opened - break */
                if(data->fd < 0) {
                        g_warning("GET2 Couldn't open file\n");
                        conn_close(prog, lhconn);
                        return;
                }

                /* Set file in non-blocking mode */
                fd_set_nonblocking(data->fd);

                /* Set up a pipeline. We will splice from fd to pipe[0] and
                 * then from pipe[1] to sock. */
                ret = pipe2(data->pipe, O_NONBLOCK);
                g_assert(ret == 0);

                /* Set new state */
                data->socket_ready = FALSE;
                data->file_ready = FALSE;
                data->file_eos = FALSE;

                /* If the original command is SGET, we will first send the
                 * size of the file */
                if (conn->cmd == BERTHA_SGET) {
                        struct stat st;
                        guint64 size;

                        /* Get the size of the file */
                        ret = fstat(data->fd, &st);

                        if(ret != 0) {
                                perror("fstat");
                                g_error("fstat failed\n");
                        }

                        size = GUINT64_TO_LE(st.st_size);
                        
                        /* Write it to the buffer */
                        if (data->buf->len > 0)
                                g_byte_array_remove_range(data->buf, 0,
                                                data->buf->len);
                        g_byte_array_append(data->buf, (gpointer)&size, 8);

                        conn->state = BCONN_STATE_SGET;
                } else
                        conn->state = BCONN_STATE_GET2;
        }
}

/*
 * Write the size of the file to the BCONN_STATE_SGET connection
 */
void conn_handle_sget(BProgram* prog, GList* lhconn)
{
        BConn* conn = lhconn->data;
        BConnGet* data = conn->state_data;
        ssize_t sent;

        sent = send(conn->sock, data->buf->data, data->buf->len, MSG_NOSIGNAL);

        /* Check if the connection was unexpectedly closed */
        if (sent == -1) {
                if (errno == EPIPE) {
                        conn_log(conn, "EPIPE");
                        conn_close(prog, lhconn);
                        return;
                }
                g_assert_not_reached();
        }

        /* Is there still some left to send? */
        if (sent != data->buf->len) {
                g_byte_array_remove_range(data->buf, 0, sent);
                return;
        }

        /* Ok, we're done! */
        conn->state = BCONN_STATE_GET2;
}

/*
 * Write the key back to the BCONN_STATE_PUT2 connection
 */
void conn_handle_put2(BProgram* prog, GList* lhconn)
{
        BConn* conn = lhconn->data;
        BConnPut* data = conn->state_data;
        ssize_t sent;

        sent = send(conn->sock, data->buf->data, data->buf->len, MSG_NOSIGNAL);

        /* Check if the connection was unexpectedly closed */
        if (sent == -1) {
                if (errno == EPIPE) {
                        conn_log(conn, "EPIPE");
                        conn_close(prog, lhconn);
                        return;
                }
                g_assert_not_reached();
        }

        /* Is there still some left to send? */
        if (sent != data->buf->len) {
                g_byte_array_remove_range(data->buf, 0, sent);
                return;
        }

        /* Ok, we're done! */
        shutdown(conn->sock, SHUT_RDWR);
        conn_close(prog, lhconn);
}

/*
 * Read data from a BCONN_STATE_PUT connection and write it to file and
 * calculate the checksum.
 */
void conn_handle_put(BProgram* prog, GList* lhconn)
{
        BConn* conn = lhconn->data;
        BConnPut* data = conn->state_data;
        ssize_t received, written;
        guint8 buf2[CFG_PUT_BUFFER];

        /* Is there data to receive from the socket?  Then we will
         * if data->buf is not full */
        if (data->socket_ready && data->buf->len < CFG_PUT_BUFFER) {
read_some:
                received = recv(conn->sock, buf2, CFG_PUT_BUFFER, 0);
                g_assert(received >= 0);

                prog->n_PUT_received += received;
                data->socket_ready = FALSE;

                /* Is the stream closed? */
                if (received == 0) {
                        data->socket_eos = TRUE;

                        /* There might be still some data to write.  If
                         * the file is ready, we will give it a try */
                        if (data->file_ready && data->buf->len > 0)
                                goto write_some;

                        /* Otherwise we will check if we are done */
                        goto check_if_done;
                }

                /* Update the checksum */
                g_checksum_update(data->checksum, buf2, received);

                /* Shorcut: don't copy to data->buf if data->buf is empty,
                 * but write directly to the file */ 
                if (data->buf->len == 0 && data->file_ready) {
                        written = write(data->fd, buf2, received);
                        g_assert(written >= 0);

                        data->file_ready = FALSE;

                        /* If we couldn't write everything,
                         * put the rest in on data->buf */
                        if (written != received)
                                g_byte_array_append(data->buf,
                                                buf2 + received - written,
                                                received - written);
                        else
                                goto check_if_done;
                }

                /* Copy the received data to data->buf */
                g_byte_array_append(data->buf, buf2, received);
        }
        
        /* Can we write data to the file?  Is there data to be written
         * to the file?  Then write data to the file! */
        if (data->file_ready && data->buf->len > 0) {
write_some:
                written = write(data->fd, data->buf->data, data->buf->len);
                g_assert(written >= 0);

                data->file_ready = FALSE;

                /* Remove the written data from the buffer */
                g_byte_array_remove_range(data->buf, 0, written);

                /* Check if we have enough room to read some from the socket */
                if (data->socket_ready && written > 0)
                        goto read_some;

                goto check_if_done;
        } 
        return;

check_if_done:
        /* Are we at the end of stream and is all data written? */
        if (data->socket_eos && data->buf->len == 0) {
                int ret;
                guint8 key[32];
                gchar* key_hex;
                GString* target;
                gsize len = 32;

                /* Get the final key */
                g_checksum_get_digest(data->checksum, key, &len);
                g_assert(len == 32);
                key_hex = buf_to_hex(key, 32);
                g_checksum_free(data->checksum);
                data->checksum = NULL;

                /* Move the temporary file into place */
                target = key_to_path(prog, key_hex);
                ret = rename(data->tmp_fn->str, target->str);
                g_assert(ret == 0);
                g_string_free(target, TRUE);

                conn_log(conn, "PUT %s", key_hex);
                g_slice_free1(65, key_hex);

                /* Transition to BCONN_STATE_PUT2 */
                conn->state = BCONN_STATE_PUT2;
                g_byte_array_append(data->buf, key, 32);
        }
}

/*
 * Gets the next key in the listing.  Updates data.
 */
guint8* _conn_handle_list_next_key(BProgram* prog, BConn* conn, BConnList* data)
{
        struct dirent* de;
        GString* key_hex;
        guint8* ret;
        gsize len;

        /* Delve up and down the directory tree until it is
         * exhausted or we find a file */
        while (data->dirs || data->cdir) {
                if (!data->cdir_handle) {
                        g_assert(data->cdir == NULL);

                        /* Pop the first entry from the <dirs> stack */
                        data->cdir = data->dirs->data;
                        data->dirs = g_list_delete_link(data->dirs, data->dirs);

                        /* Open the directory */
                        data->cdir_handle = opendir(data->cdir->path->str);
                        g_assert(data->cdir_handle);
                }

                de = readdir(data->cdir_handle);

                if (!de) { /* directory is depleted */
                        /* Close directory */
                        closedir(data->cdir_handle);
                        data->cdir_handle = NULL;

                        /* Free entry */
                        g_string_free(data->cdir->path, TRUE);
                        g_string_free(data->cdir->key, TRUE);
                        g_slice_free(BConnListEntry, data->cdir);
                        data->cdir = NULL;

                        continue;
                }

                /* ignore . and .. */
                if (strcmp(de->d_name, ".") == 0
                                || strcmp(de->d_name, "..") == 0)
                        continue;

                /* If we're not at the leaves, expect more directories.
                 * Push there on the stack. */
                if (data->cdir->depth < CFG_DATADIR_DEPTH) {
                        BConnListEntry* e;

                        if (strlen(de->d_name) != CFG_DATADIR_WIDTH
                                        || !is_hex(de->d_name)) {
                                g_warning("Malformed path %s/%s in datadir",
                                        data->cdir->path->str, de->d_name);
                                continue;
                        }

                        /* Create a new entry */
                        e = g_slice_new(BConnListEntry);
                        e->path = g_string_new(data->cdir->path->str);
                        g_string_append_printf(e->path, "/%s", de->d_name);
                        e->key = g_string_new(data->cdir->key->str);
                        g_string_append(e->key, de->d_name);
                        e->depth = data->cdir->depth + 1;

                        /* Push it on the stack */
                        data->dirs = g_list_prepend(data->dirs, e);
                        continue;
                }

                g_assert(data->cdir->depth == CFG_DATADIR_DEPTH);

                /* We expect files */
                if (strlen(de->d_name) !=
                                64 - CFG_DATADIR_WIDTH * CFG_DATADIR_DEPTH
                                || !is_hex(de->d_name)) {
                        g_warning("Malformed path %s/%s in datadir",
                                        data->cdir->path->str, de->d_name);
                        continue;
                }

                /* Convert the key from hexadecimal to binary */
                key_hex = g_string_new(data->cdir->key->str);
                g_string_append(key_hex, de->d_name);
                g_assert(key_hex->len == 64);
                ret =  hex_to_buf(key_hex->str, &len);
                g_assert(len == 32);
                g_string_free(key_hex, TRUE);
                return ret;
        }

        return NULL;
}

/*
 * Writes some data to a BCONN_STATE_LIST connection
 */
void conn_handle_list(BProgram* prog, GList* lhconn)
{
        BConn* conn = lhconn->data;
        BConnList* data = conn->state_data;
        ssize_t sent;
        int flags = MSG_NOSIGNAL;

        /* Read directory names into the buffer */
        while ((data->dirs || data->cdir) && data->buf->len < CFG_LIST_BUFFER) {
                guint8* key = _conn_handle_list_next_key(prog, conn, data);

                if(!key)
                        break;

                /* Append to buffer */
                g_byte_array_append(data->buf, key, 32);
                g_slice_free1(32, key);
        }

        /* Try to send it */
        if (data->dirs || data->cdir)
                flags = MSG_MORE;

        if(data->buf->len > 0) {
                sent = send(conn->sock, data->buf->data, data->buf->len, flags);

                /* Check if the connection was unexpectedly closed */
                if (sent == -1) {
                        if (errno == EPIPE) {
                                conn_log(conn, "EPIPE");
                                conn_close(prog, lhconn);
                                return;
                        }
                        g_assert_not_reached();
                }

                g_assert(sent > 0);

                /* Remove the sent bytes from the buffer */
                g_byte_array_remove_range(data->buf, 0, sent);
        }

        /* If there is nothing more to sent, close the connection */
        if (!data->dirs && !data->cdir && data->buf->len == 0) {
                shutdown(conn->sock, SHUT_RDWR);
                conn_close(prog, lhconn);
        }
}

/*
 * Handles data on a BCONN_STATE_INITIAL connection
 */
void conn_handle_initial(BProgram* prog, GList* lhconn)
{
        int received = 0;
        BConn* conn = lhconn->data;
        guint8 cmd;

        /* Receive the first byte */
        received = recv(conn->sock, &cmd, 1, 0);
        g_assert(received == 0 || received == 1);

        /* Premature end of stream */
        if (received == 0) {
                conn_close(prog, lhconn);
                return;
        }

        conn->cmd = cmd;
        
        /* Check the command */
        if (cmd == BERTHA_LIST) {
                BConnList* data = g_slice_new0(BConnList);
                BConnListEntry* e = g_slice_new0(BConnListEntry);

                /* Set new connection state */
                conn->state = BCONN_STATE_LIST;
                conn->state_data = data;
                conn_log(conn, "LIST");
                
                /* Add the dataDir to the stack */
                e->key = g_string_new("");
                e->path = g_string_new(prog->dataPath);
                e->depth = 0;
                data->dirs = g_list_prepend(data->dirs, e);
                data->cdir = NULL;
                data->cdir_handle = NULL;

                /* Initialize the send buffer */
                data->buf = g_byte_array_sized_new(CFG_LIST_BUFFER);
        } else if (cmd == BERTHA_GET || cmd == BERTHA_SGET) {
                BConnGet* data = g_slice_new0(BConnGet);

                /* Set new connection state */
                conn->state = BCONN_STATE_GET;
                conn->state_data = data;

                /* Initialize buffer */
                data->buf = g_byte_array_sized_new(32);
        } else if (cmd == BERTHA_PUT || cmd == BERTHA_SPUT) {
                BConnPut* data = g_slice_new0(BConnPut);

                /* Set new connection state */
                conn->state = cmd == BERTHA_PUT ? BCONN_STATE_PUT
                                                        : BCONN_STATE_SPUT;
                conn->state_data = data;
                data->file_ready = FALSE;
                data->socket_ready = FALSE;
                data->socket_eos = FALSE;

                /* Create a temporary file */
                data->tmp_fn = g_string_sized_new(128);
                g_string_printf(data->tmp_fn, "%s/berthadtmp.XXXXXX",
                                                prog->tmpPath);
                data->fd = mkstemp(data->tmp_fn->str);
                g_assert(data->fd != -1);
                fd_set_nonblocking(data->fd);

                if (cmd == BERTHA_PUT)
                        conn_log(conn, "PUT %s", data->tmp_fn->str);

                /* Set up the checksum */
                data->checksum = g_checksum_new(G_CHECKSUM_SHA256);

                /* Initialize the write buffer */
                data->buf = g_byte_array_sized_new(CFG_PUT_BUFFER);
        } else if (cmd == BERTHA_QUIT) {
                GList* lhconn2;

                conn_log(conn, "QUIT");
                for (lhconn2 = prog->conns; lhconn2;
                                lhconn2 = g_list_next(lhconn2))
                        conn_close(prog, lhconn2);
                prog->running = FALSE;
        } else
                conn_close(prog, lhconn);
}

/*
 * Adds socket <s> to fd_set <set> and updates <highest_fd>
 */
void fd_set_add(fd_set* set, int s, int* highest_fd)
{
        if(*highest_fd < s)
                *highest_fd = s;
        FD_SET(s, set);
}

/*
 * Checks the activity
 */
void check_fd_sets(BProgram* prog)
{
        GList* lhconn;

        /* Is there a new connection to accept? */
        if (FD_ISSET(prog->lsock, &prog->r_fds))
                conn_accept(prog);

        for (lhconn = prog->conns; lhconn; lhconn = g_list_next(lhconn)) {
                BConn* conn = lhconn->data;

                /* There is data on a just-accepted connection */
                if(conn->state == BCONN_STATE_INITIAL
                                && FD_ISSET(conn->sock, &prog->r_fds))
                        conn_handle_initial(prog, lhconn);
                /* We can write some more to a LIST connection */
                if(conn->state == BCONN_STATE_LIST
                                && FD_ISSET(conn->sock, &prog->w_fds))
                        conn_handle_list(prog, lhconn);
                /* We can read data for a PUT connection from the socket
                 * or write data to the file */
                if(conn->state == BCONN_STATE_PUT) {
                        BConnPut* data = conn->state_data;
                        if (!data->socket_eos 
                                        && FD_ISSET(conn->sock, &prog->r_fds))
                                data->socket_ready = TRUE;
                        if (FD_ISSET(data->fd, &prog->w_fds))
                                data->file_ready = TRUE;
                        conn_handle_put(prog, lhconn);
                }
                /* We can write the key to the socket */
                if(conn->state == BCONN_STATE_PUT2
                                && FD_ISSET(conn->sock, &prog->w_fds)) {
                        conn_handle_put2(prog, lhconn);
                }
                /* We can read the key of the GET connection */
                if(conn->state == BCONN_STATE_GET
                                && FD_ISSET(conn->sock, &prog->r_fds)) {
                        conn_handle_get(prog, lhconn);
                }
                /* We can read data from the file or send data
                 * over the socket */
                if(conn->state == BCONN_STATE_GET2) {
                        BConnGet* data = conn->state_data;
                        if(!data->file_eos
                                        && FD_ISSET(data->fd, &prog->r_fds))
                                data->file_ready = TRUE;
                        if(FD_ISSET(conn->sock, &prog->w_fds)) {
                                data->socket_ready = TRUE;
                        }
                        conn_handle_get2(prog, lhconn);
                }
                /* We can read the size of the file from the socket */
                if (conn->state == BCONN_STATE_SPUT
                                && FD_ISSET(conn->sock, &prog->r_fds)) 
                        conn_handle_sput(prog, lhconn);
                /* We can write the size of the file to the socket */
                if (conn->state == BCONN_STATE_SGET
                                && FD_ISSET(conn->sock, &prog->w_fds))
                        conn_handle_sget(prog, lhconn);
        }
}

/* 
 * Fills the fd_sets for select (2)
 */
void reset_fd_sets(BProgram* prog)
{
        GList* lhconn;

        /* Zero out */
        FD_ZERO(&prog->r_fds);
        FD_ZERO(&prog->w_fds);

        /* Listen for connections prog->lsock */
        fd_set_add(&prog->r_fds, prog->lsock, &prog->highest_fd);
        
        for (lhconn = prog->conns; lhconn; lhconn = g_list_next(lhconn)) {
                BConn* conn = lhconn->data;
                /* We wait for a command on BCONN_STATE_INITIAL */
                if (conn->state == BCONN_STATE_INITIAL)
                        fd_set_add(&prog->r_fds, conn->sock,
                                                &prog->highest_fd);
                /* We wait to write on BCONN_STATE_LIST */
                else if (conn->state == BCONN_STATE_LIST)
                        fd_set_add(&prog->w_fds, conn->sock,
                                                &prog->highest_fd);
                /* We wait for data on BCONN_STATE_PUT on the socket
                 * and wait to write on its target file */
                else if (conn->state == BCONN_STATE_PUT) {
                        BConnPut* data = conn->state_data;
                        if(!data->socket_eos && !data->socket_ready)
                                fd_set_add(&prog->r_fds, conn->sock,
                                                &prog->highest_fd);
                        if(!data->file_ready)
                                fd_set_add(&prog->w_fds, data->fd,
                                                &prog->highest_fd);
                } /* We wait to write the key back to socket */ 
                else if (conn->state == BCONN_STATE_PUT2)
                        fd_set_add(&prog->w_fds, conn->sock,
                                                &prog->highest_fd);
                /* We wait to read the key from the conn */
                else if (conn->state == BCONN_STATE_GET)
                        fd_set_add(&prog->r_fds, conn->sock,
                                                &prog->highest_fd);
                /* We wait for data from a file to send over
                 * a BCONN_STATE_GET2 connection */
                else if (conn->state == BCONN_STATE_GET2) {
                        BConnGet* data = conn->state_data;
                        if(!data->socket_ready)
                                fd_set_add(&prog->w_fds, conn->sock,
                                                &prog->highest_fd);
                        if(!data->file_eos && !data->file_ready)
                                fd_set_add(&prog->r_fds, data->fd,
                                                &prog->highest_fd);
                } /* We wait to read the size from the conn */
                else if (conn->state == BCONN_STATE_SPUT)
                        fd_set_add(&prog->r_fds, conn->sock,
                                                &prog->highest_fd);
                /* We wait to write the size of the file to the buffer */
                else if (conn->state == BCONN_STATE_SGET)
                        fd_set_add(&prog->w_fds, conn->sock,
                                                &prog->highest_fd);
        }
}

/*
 * Advices the kernel to readahead some data on a file being spliced.
 */
void job_fadvise(BJobConn* job)
{
        BConn* conn = job->conn;
        BConnGet* data = conn->state_data;
        int ret;

        ret = posix_fadvise(data->fd, 0, 0, POSIX_FADV_WILLNEED);
        g_assert(ret == 0);
}

/*
 * Preallocates a file for a PUT
 */
void job_fallocate(BJobConn* job)
{
        BConn* conn = job->conn;
        BConnPut* data = conn->state_data;
        int ret;
        
        ret = fallocate(data->fd, FALLOC_FL_KEEP_SIZE, 0, data->advised_size);
        g_assert(ret == 0);
}

/*
 * Entry of jobs started at the threadpool.
 */
void threadpool_entry(gpointer _job, gpointer unused)
{
        BJob* job = _job;
        if (job->type == BJOB_FADVISE ||
                        job->type == BJOB_FALLOCATE) {
                BJobConn* jobc = _job;

                if (job->type == BJOB_FADVISE)
                        job_fadvise(jobc);
                else if (job->type == BJOB_FALLOCATE)
                        job_fallocate(jobc);
                else
                        g_assert_not_reached();

                /* Signal the connection condition if we were the last thread
                 * on that connection */
                g_mutex_lock(jobc->conn->threads_mutex);
                if (--jobc->conn->n_threads == 0)
                        g_cond_signal(jobc->conn->threads_cond);
                g_mutex_unlock(jobc->conn->threads_mutex);
                g_slice_free(BJobConn, jobc);
        } else
                g_assert_not_reached();
}

int main (int argc, char** argv)
{
        BProgram prog;
        struct addrinfo hints;
        struct addrinfo *addrs, *addr;
        int ret;
        int optval;

        /* Initialize GLib threads */
        g_thread_init(NULL);

        /* Check number of arguments */
        if (argc != 5) {
                g_printerr("Usage: berthad-vfs <bound host> "
                                "<port> <data dir> <tmp dir>\n");
                exit(EXIT_FAILURE);
        }

        /* Ignore SIGPIPE. If we splice to a socket that is shut down by the
         * other end, SIGPIPE is raised.  That is more easily handled
         * at the call site of splice instead of in a signal handler. */
        signal(SIGPIPE, SIG_IGN);

        memset(&prog, 0, sizeof(BProgram));
        prog.running = TRUE;
        prog.dataPath = argv[3];
        prog.tmpPath = argv[4];

        g_assert(path_is_dir(prog.dataPath));
        g_assert(path_is_dir(prog.tmpPath));

        /* Initialize thread_pool */
        prog.threadpool = g_thread_pool_new(threadpool_entry, NULL,
                                                        -1, FALSE, NULL);

        /* Resolve hostname */
        memset(&hints, 0, sizeof(struct addrinfo));
        hints.ai_family = AF_UNSPEC;            /* IPv4 or IPv6 */
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE;
        hints.ai_protocol = 0;
        hints.ai_canonname = NULL;
        hints.ai_addr = NULL;
        hints.ai_next = NULL;

        ret = getaddrinfo(argv[1], argv[2], &hints, &addrs);

        if (ret != 0)
                g_error("getaddrinfo: %s", gai_strerror(ret));

        /* Try to bind to each of the addresses */
        for (addr = addrs; addr; addr = addr->ai_next) {
                prog.lsock = socket(addr->ai_family,
                                    addr->ai_socktype,
                                    addr->ai_protocol);
                if (prog.lsock == -1)
                        continue;

                /* Reuse address - useful if we don't want to wait while
                 * debugging */
                optval = 1;
                setsockopt(prog.lsock, SOL_SOCKET, SO_REUSEADDR,
                                &optval, sizeof optval);

                if (bind(prog.lsock, addr->ai_addr,
                                     addr->ai_addrlen) == 0)
                        break; /* success! */
                close(prog.lsock);
        }

        if (!addr) { /* no address succeeded */
                g_printerr("Could not bind to %s: %s\n", argv[1], argv[2]);
                perror("bind");
                exit(EXIT_FAILURE);
        }

        freeaddrinfo(addrs);

        /* listen */
        if(listen(prog.lsock, CFG_BACKLOG) != 0) {
                perror("listen");
                exit(EXIT_FAILURE);
        }

        /* set the socket in nonblocking mode */
        fd_set_nonblocking(prog.lsock);

        /* The main loop! */
        while (prog.running) {
                struct timeval timeout;
                timeout.tv_sec = 1;
                timeout.tv_usec = 0;

                reset_fd_sets(&prog);
                
                /* wait for activity */
                ret = select(prog.highest_fd + 1, &prog.r_fds, &prog.w_fds,
                                NULL, &timeout);
                if (ret == 0) { /* no activity */
                        continue;
                } else if (ret == -1) { /* error?! */
                        perror("select");
                        exit(EXIT_FAILURE);
                } 

                prog.n_cycle++;

                /* Check the activity */
                check_fd_sets(&prog);
        }

        g_thread_pool_free(prog.threadpool, FALSE, TRUE);

        return 0;
}
