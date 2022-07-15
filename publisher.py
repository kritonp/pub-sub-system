import socket
import time
import utils
from logger import logger


def process_file(file_name, sock, pub_id):
    if file_name:
        try:
            testing_code = utils.test_pub_command_file(file_name)
            if testing_code == 0:
                with open(file_name) as f:
                    for line in f:
                        topic = utils.get_topic(line)
                        message = utils.get_msg(line)
                        wait_time = utils.get_wait_time(line)
                        time.sleep(wait_time)
                        line = line.replace(line[:line.index(' ')], pub_id, 1)  # replace first token with sub id
                        sock.sendall(bytes(line, "utf-8"))
                        # received = str(sock.recv(1024), "utf-8")  # receive an OKAY from broker
                        sock.recv(1024)  # receive an OKAY from broker
                        logger.info(f"Published msg for topic : {topic}: {message}")

        except IOError as e:
            logger.warning(f"{file_name} doesn't exist {e}")
    else:
        logger.info(f"-f param was None, check again or type commands in terminal")


def get_cmd_input(pub_id, sock):
    while True:
        input_cmd = input()
        topic = utils.get_topic(input_cmd)
        message = utils.get_msg(input_cmd)
        wait_time = utils.get_wait_time(input_cmd)
        time.sleep(wait_time)
        input_cmd = input_cmd.replace(input_cmd[:input_cmd.index(' ')], pub_id, 1)  # replace first token with pub id
        sock.sendall(bytes(input_cmd, "utf-8"))
        try:
            received = str(sock.recv(1024), "utf-8")  # receive an OKAY from broker
            logger.info(f"Published msg for topic : {topic}: {message}")
        except socket.error as e:
            logger.error(f"Error/Disconnect {e}")


def main():
    pub_id, pub_port, host, broker_port, file_name = utils.get_params()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, broker_port))
    sock.sendall(bytes(str(pub_port), "utf-8"))  # In the first connection we send the pub's port

    process_file(file_name, sock, pub_id)
    get_cmd_input(pub_id, sock)


if __name__ == "__main__":
    main()
