import threading
import socket
import sys
from logger import logger
import utils


list_of_tuples = []  # [(s1, topic1), (s1, topic2), (s2, topic1), ... ]
sub_id_port = []  # [(s1, 8000), (s2, 8001), ..., (s10, 8009), ...]


def pub_thread(host, p_port):
    s = socket.socket()  # Create a socket object
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, p_port))  # Bind to the port
    s.listen(5)  # Now wait for client connection.
    thread_count = 0
    logger.info(f"Broker listening Publisher on {host}:{p_port}")
    while True:
        conn, addr = s.accept()  # Establish connection with client.
        pub_port = conn.recv(1024).strip().decode("utf-8")
        logger.info(f"Publisher port is: {pub_port}")
        thread_count += 1
        logger.info(f'Got connection from {addr}')
        logger.info(f'Thread Number: {thread_count}')
        threading.Thread(target=new_pub, args=(conn, host,)).start()


def new_pub(conn, host):
    while True:
        try:
            data = conn.recv(1024).strip()
            data = data.decode("utf-8")  # Convert bytes to String
            logger.info(f"Received from Pub: {data}")
            topic = utils.get_topic(data)
            conn.sendall(bytes("OK", "utf-8"))
            res = [item for item in list_of_tuples if item[1] == topic]
            if len(res) > 0:
                for sub in res:
                    sub_port = utils.get_port(sub_id_port, sub[0])
                    logger.info(f"Subscriber {sub[0]} has port {sub_port}")
                    logger.info(f"found {topic} for {res} sub_port: {sub_port}")
                    try:
                        sub_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sub_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        sub_socket.bind((host, sub_port))  # Connect to subscriber
                        sub_socket.listen(5)
                        client_socket, address = sub_socket.accept()
                        client_socket.sendall(bytes(data, "utf-8"))  # Forward message to subscriber
                        sub_socket.close()
                    except socket.error as e:
                        logger.error(f"Socker Error: {e}")

        except socket.error as e:
            logger.error(f"Pub Disconnected {e}")
            # time.sleep(1)
            break


def sub_thread(host, s_port):
    s = socket.socket()  # Create a socket object
    logger.info(f"Broker listening Subscribers on {host}:{s_port}")
    s.bind((host, s_port))  # Bind to the port
    s.listen(5)  # Now wait for client connection.
    thread_count = 0
    while True:
        conn, addr = s.accept()  # Establish connection with client.
        sub_info = conn.recv(1024).strip().decode("utf-8")
        conn.sendall(bytes("OK", "utf-8"))
        thread_count += 1
        sub_id = sub_info[:sub_info.index(' ')]
        sub_port = sub_info[sub_info.index(' '):].strip()
        sub_id_port.append((sub_id, int(sub_port)))
        threading.Thread(target=new_sub, args=(conn,)).start()


def new_sub(conn):
    while True:
        try:
            data = conn.recv(1024).strip()
            conn.sendall(bytes("OK", "utf-8"))
            data = data.decode("utf-8")  # Convert bytes to String
            sub_id = utils.get_id(data)
            topic = utils.get_topic(data)

            if "unsub" in data:
                for key, value in list_of_tuples:
                    if key == sub_id and value == topic:
                        list_of_tuples.remove((key, value))
                        logger.info(f"[-] [{sub_id}] {topic}")
            elif "sub" in data:
                res = [item for item in list_of_tuples if item[0] == sub_id and item[1] == topic]
                if len(res) == 0:
                    list_of_tuples.append((sub_id, topic))
                    logger.info(f"[+] [{sub_id}] {topic}")
            else:
                logger.warning(f"{data} has a typo")

        except socket.error as e:
            logger.info(f"Sub Disconnected {e}")
            # conn.close()
            break


def main():
    s_port, p_port = utils.get_broker_params()
    # host, bytes_size = utils.read_config('/config.ini')
    host = '127.0.0.1'
    try:
        threading.Thread(target=pub_thread, args=(host, p_port,)).start()
        threading.Thread(target=sub_thread, args=(host, s_port,)).start()

    except KeyboardInterrupt as e:
        logger.error(e)
        sys.exit(0)


if __name__ == "__main__":
    main()
