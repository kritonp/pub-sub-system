import socket
import utils
import time
from logger import logger
import threading


def process_file(file_name, sock, sub_id):
    if file_name:
        try:
            if utils.test_sub_command_file(file_name) == 0:
                with open(file_name) as f:
                    for line in f:
                        wait_time = utils.get_wait_time(line)
                        time.sleep(wait_time)
                        line = line.replace(line[:line.index(' ')], sub_id, 1)  # replace first token with sub id
                        sock.sendall(bytes(line, "utf-8"))
                        received = str(sock.recv(1024), "utf-8")  # receive an OKAY from broker
                        logger.info(f"Message received from broker: {received}")
        except IOError as e:
            logger.error(f"{file_name} doesn't exist {e}")
            exit(1)
    else:
        logger.info(f"-f param was {file_name}, check again or type commands in terminal")


def listen_for_pubs(host, port):
    while True:
        try:
            s = socket.socket()
            s.connect((host, port))
            received = str(s.recv(1024), "utf-8")
            logger.info(f"Received msg for topic {utils.get_topic(received)}: {utils.get_msg(received)}")
            s.close()
        except socket.error:
            time.sleep(1)  # Listening for pubs


def get_cmd_input(sub_id, sock):
    while True:
        input_cmd = input()
        wait_time = utils.get_wait_time(input_cmd)
        time.sleep(wait_time)
        input_cmd = input_cmd.replace(input_cmd[:input_cmd.index(' ')], sub_id, 1)  # replace first token with sub_id
        sock.sendall(bytes(input_cmd, "utf-8"))
        try:
            received = str(sock.recv(1024), "utf-8")  # receive an OKAY from broker
            logger.info(f"Message received from broker: {received}")
        except socket.error as e:
            print("Error/Disconnect ", e)


def main():
    sub_id, sub_port, host, broker_port, file_name = utils.get_params()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, broker_port))
    sock.sendall(bytes(sub_id + " " + str(sub_port), "utf-8"))  # In the first connection we send the sub's port
    str(sock.recv(1024), "utf-8")

    process_file(file_name, sock, sub_id)
    threading.Thread(target=listen_for_pubs, args=(host, sub_port, )).start()
    threading.Thread(target=get_cmd_input, args=(sub_id, sock, )).start()


if __name__ == "__main__":
    main()

