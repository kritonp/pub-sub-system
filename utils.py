import argparse
import configparser
import os
from logger import logger
import socket


def get_params():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-i", help="indicates the id of this subscriber")
    parser.add_argument("-r", help="indicates the port of this specific subscriber", type=int)
    parser.add_argument("-h", help="indicates the IP address of the broker")
    parser.add_argument("-p", help="indicates the port of the broker", type=int)
    parser.add_argument("-f", help="s an optional parameter that indicates a file name")
    args = parser.parse_args()
    logger.info(f'Command line parameters: {args}')
    return args.i, args.r, args.h, args.p, args.f


def get_broker_params():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-s", help="indicates the port of this specific broker where subscribers will connect", type=int)
    parser.add_argument("-p", help="indicates the port of this specific broker where publishers will connect", type=int)
    args = parser.parse_args()
    return args.s, args.p


def _get_text_col(lines, col_id):
    """
    :param lines: A file parsed in lines
    :param col_id: The id of the column we want to retrieve
    :return: The whole <col_id> column of a text file
    """
    tokens_column_number = col_id
    result_token = []
    for line in lines:
        result_token.append(line.split()[tokens_column_number])
    return result_token


def test_sub_command_file(filename):
    """
    A simple function for testing subs command files. First column must have positive integers and second column must have
    either the command pub or sub.
    :param filename: subscriber command file
    :return: 0 if passes the tests, otherwise 1
    """
    logger.info(f'Testing {filename} for errors.')
    first_col_flag, second_col_flag = False, False
    file = open(filename, 'r')
    lines = file.readlines()

    result_token = _get_text_col(lines, 0)
    for num in result_token:
        if not num.isnumeric():
            first_col_flag = True

    result_token = _get_text_col(lines, 1)
    for word in result_token:
        if word != 'sub' and word != 'unsub':
            second_col_flag = True

    file.close()
    if first_col_flag is True or second_col_flag is True:
        logger.error(f'{filename} is corrupted. File must follow the following format: WAIT_TIME COMMAND MESSAGE')
        return 1
    else:
        logger.info(f'Testing {filename} completed.')
        return 0


def test_pub_command_file(filename):
    """
    A simple function for testing pubs command files. First column must have positive integers and second column must
    have the command pub.
    :param filename: publisher command file
    :return: 0 if passes the tests, otherwise 1
    """
    logger.info(f'Testing {filename} for errors.')
    first_col_flag, second_col_flag = False, False
    file = open(filename, 'r')
    lines = file.readlines()

    result_token = _get_text_col(lines, 0)
    for num in result_token:
        if not num.isnumeric():
            first_col_flag = True

    result_token = _get_text_col(lines, 1)
    for word in result_token:
        if word != 'pub':
            second_col_flag = True

    file.close()
    if first_col_flag is True or second_col_flag is True:
        logger.error(f'{filename} is corrupted. File must follow the following format: WAIT_TIME COMMAND TOPIC '
                     f'MESSAGE')
        return 1
    else:
        logger.info(f'Testing {filename} completed.')
        return 0


def read_config(conf_file):
    config_file = os.path.dirname(os.path.realpath(__file__)) + conf_file
    logger.info(f'Reading {conf_file} file')
    config = configparser.ConfigParser()
    try:
        f = open(config_file)
        config.read(config_file)
        host = config['DEFAULT']['host']
        bytes_size = config['DEFAULT']['bytes_size']
        f.close()
        return host, int(bytes_size)
    except IOError as e:
        logger.error('%s', e)
        exit(-1)


def connect(host, port, name):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(10)
    logger.info(f"Broker listening {name} on %s:%d" % (host, port))
    conn, addr = sock.accept()
    logger.info(f"{name} Connected : " + addr[0] + ":" + str(addr[1]))
    return conn


def get_topic(command):
    list_str = command.split()
    topic = ' '.join(map(str, list_str[2:3]))
    return topic


def get_id(command):
    return command.split()[0]


def get_msg(command):
    list_str = command.split()
    msg = ' '.join(map(str, list_str[3:]))
    return msg


def get_wait_time(command):
    wait_time = command.split()[0]
    if not wait_time.isnumeric() or int(wait_time) < 0:
        logger.warning(f'WAIT_TIME type isn\'t numerical or positive. WAIT_TIME set to 0')
        wait_time = 0
    return int(wait_time)


def get_port(sub_id_port, sub_id):
    """
    This is a helper function for broker (publisher side), in which we search for the corresponding port number
    for a given subscriber_id, so we can forward that message.
    :param sub_id_port: This is a list of tuples like [(s1, 8000), (s2, 8001), ..., (s10, 8009), ...]
    :param sub_id: The id of the subscriber we want to search for the corresponding port
    :return: subscriber port number
    """
    res = [item for item in sub_id_port if item[0] == sub_id]
    sub_port = res[0][1]
    return sub_port
