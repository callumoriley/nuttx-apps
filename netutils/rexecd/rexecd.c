/****************************************************************************
 * apps/netutils/rexecd/rexecd.c
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The
 * ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 ****************************************************************************/

/****************************************************************************
 * Included Files
 ****************************************************************************/

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <netpacket/rpmsg.h>
#include <sys/socket.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <poll.h>
#include <syslog.h>

/****************************************************************************
 * Pre-processor Definitions
 ****************************************************************************/

#define REXECD_PORT    512
#define REXECD_BUFSIZE 512

/****************************************************************************
 * Private Types
 ****************************************************************************/

/****************************************************************************
 * Private Function Prototypes
 ****************************************************************************/

static FAR void *doit(pthread_addr_t pvarg);
static int getstr(int fd, FAR char *buf, size_t size);

/****************************************************************************
 * Private Data
 ****************************************************************************/

/****************************************************************************
 * Private Functions
 ****************************************************************************/

/* Read a NUL-terminated string of at most 'size' bytes, NUL included.
 * Returns its length with the NUL (> 0), or <= 0 if the peer closed, the
 * read failed or the string does not fit.
 */

static int getstr(int fd, FAR char *buf, size_t size)
{
  size_t len = 0;
  int ret;

  do
    {
      if (len >= size)
        {
          return -1;
        }

      ret = read(fd, &buf[len], 1);
      if (ret <= 0)
        {
          return ret;
        }
    }
  while (buf[len++] != '\0');

  return len;
}

static FAR void *doit(pthread_addr_t pvarg)
{
  char buf[REXECD_BUFSIZE];
  struct pollfd fds[2];
  pid_t pid;
  int sock = (long)pvarg;
  int ret;
  int i;

  /* Read err_sock, user and passwd, which are ignored, and then the
   * command.  Drop the connection if the peer closes before sending all
   * four, as buf would not hold a command.
   */

  for (i = 0; i < 4; i++)
    {
      if (getstr(sock, buf, sizeof(buf)) <= 0)
        {
          goto errout;
        }
    }

  memset(fds, 0, sizeof(fds));
  fds[0].events = POLLIN;
  fds[1].fd = sock;
  fds[1].events = POLLIN;

  fds[0].fd = dpopen(buf, O_RDWR, &pid);
  if (fds[0].fd < 0)
    {
      goto errout;
    }

  while (1)
    {
      ret = poll(fds, 2, -1);
      if (ret <= 0)
        {
          continue;
        }

      if (fds[0].revents & POLLIN)
        {
          ret = read(fds[0].fd, buf, REXECD_BUFSIZE);
          if (ret <= 0)
            {
              break;
            }

          /* MSG_NOSIGNAL: a closed peer ends this session, not rexecd */

          ret = send(sock, buf, ret, MSG_NOSIGNAL);
          if (ret < 0)
            {
              break;
            }
        }

      if (fds[1].revents & POLLIN)
        {
          ret = read(sock, buf, REXECD_BUFSIZE);
          if (ret <= 0)
            {
              break;
            }

          ret = send(fds[0].fd, buf, ret, MSG_NOSIGNAL);
          if (ret < 0)
            {
              break;
            }
        }

      if (((fds[0].revents | fds[1].revents) & POLLHUP) &&
          ((fds[0].revents | fds[1].revents) & POLLIN) == 0)
        {
          break;
        }
    }

  dpclose(fds[0].fd, pid);
errout:
  close(sock);
  return NULL;
}

static void usage(FAR const char *progname)
{
  fprintf(stderr, "Usage: %s [-4|-6|-r] [-t]\n", progname);
  fprintf(stderr, "Remote Execution Daemon:\n"
                  " -4, Specify address family to AF_INET(default)\n"
                  " -6, Specify address family to AF_INET6\n"
                  " -r, Specify address family to AF_RPMSG\n"
                  " -t, Serve each connection inline without spawning a"
                  " per-connection thread (saves heap, no concurrency)\n");
  exit(EXIT_FAILURE);
}

int main(int argc, FAR char **argv)
{
  struct sockaddr_storage addr;
  pthread_attr_t attr;
  pthread_t tid;
  bool threadless = false;
  int family;
  int option;
  int serv;
  int sock;
  int ret;

  family = AF_INET;
  while ((option = getopt(argc, argv, "46rt")) != ERROR)
    {
      switch (option)
        {
          case '4':
            family = AF_INET;
            break;
          case '6':
            family = AF_INET6;
            break;
          case 'r':
            family = AF_RPMSG;
            break;
          case 't':
            threadless = true;
            break;
          default:
            usage(argv[0]);
        }
    }

  serv = socket(family, SOCK_STREAM, 0);
  if (serv < 0)
    {
      return serv;
    }

  memset(&addr, 0, sizeof(addr));
  switch (family)
    {
      default:
      case AF_INET:
        ((FAR struct sockaddr_in *)&addr)->sin_family = AF_INET;
        ((FAR struct sockaddr_in *)&addr)->sin_port = htons(REXECD_PORT);
        ret = sizeof(struct sockaddr_in);
        break;
      case AF_INET6:
        ((FAR struct sockaddr_in6 *)&addr)->sin6_family = AF_INET6;
        ((FAR struct sockaddr_in6 *)&addr)->sin6_port = htons(REXECD_PORT);
        ret = sizeof(struct sockaddr_in6);
        break;
      case AF_RPMSG:
        ((FAR struct sockaddr_rpmsg *)&addr)->rp_family = AF_RPMSG;
        snprintf(((FAR struct sockaddr_rpmsg *)&addr)->rp_name,
                 RPMSG_SOCKET_NAME_SIZE, "%d", htons(REXECD_PORT));
        ret = sizeof(struct sockaddr_rpmsg);
    }

  ret = bind(serv, (FAR struct sockaddr *)&addr, ret);
  if (ret < 0)
    {
      goto err_out;
    }

  ret = listen(serv, 5);
  if (ret < 0)
    {
      goto err_out;
    }

  if (!threadless)
    {
      ret = pthread_attr_init(&attr);
      if (ret != 0)
        {
          goto err_out;
        }

      ret = pthread_attr_setstacksize(&attr,
                                      CONFIG_NETUTILS_REXECD_STACKSIZE);
      if (ret != 0)
        {
          goto attr_out;
        }

      ret = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
      if (ret != 0)
        {
          goto attr_out;
        }
    }

  while (1)
    {
      sock = accept4(serv, NULL, 0, SOCK_CLOEXEC);
      if (sock < 0)
        {
          /* A peer that closed before it was accepted fails only its own
           * connection, so keep serving the others.
           */

          if (errno == EINTR || errno == ECONNABORTED || errno == ENOTCONN)
            {
              continue;
            }
          else
            {
              ret = sock;
              goto attr_out;
            }
        }

      if (threadless)
        {
          doit((pthread_addr_t)(long)sock);
        }
      else
        {
          ret = pthread_create(&tid, &attr, doit,
                               (pthread_addr_t)(long)sock);
          if (ret < 0)
            {
              close(sock);
            }
        }
    }

attr_out:
  if (!threadless)
    {
      pthread_attr_destroy(&attr);
    }

err_out:
  syslog(LOG_ERR, "rexecd failed ret:%d errno:%d\n", ret, errno);
  close(serv);
  return ret;
}
