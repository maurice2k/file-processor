#!/usr/bin/python3

import signal
import sys
import os
import argparse
import subprocess
import ctypes
import time
import re

verbosity = 0
shutdown_state = 0
process_timeout = 300
working_list = []
cleanup_list = []
re_lock = re.compile(r'^\.fp-lock-\d{8}T\d{6}_\d+_')

# signal handler for SIGINT
def signal_handler_sigint(signal, frame):
    global shutdown_state
    if shutdown_state == 1:
        sys.exit(0)

    if shutdown_state == 0:
        shutdown_state = 1

def reload_file_lists(working_dir):
    global working_list
    global cleanup_list
    global process_timeout
    global re_lock
    working_list = []
    locked_timeout = '.fp-lock-' + time.strftime('%Y%m%dT%H%M%S', time.localtime(time.time() - process_timeout))
    for root, dirs, files in os.walk(working_dir):
        for file in files:
            if re_lock.match(file):
                if file < locked_timeout and len(cleanup_list) < 1000:
                    cleanup_list.append([root, file])
            elif not file.startswith('.'):
                working_list.append([root, file])
                if len(working_list) >= 100000:
                    break
        if len(working_list) >= 100000:
            break

    if args.sort == 'mtime':
        working_list.sort(key=lambda item: os.path.getmtime(os.path.join(*item)), reverse=args.sort_reverse)
    elif args.sort == 'name':
        working_list.sort(reverse=args.sort_reverse)

def cleanup():
    global cleanup_list
    global re_lock
    for item in cleanup_list:
        dirname, filename = item
        lock_file = os.path.join(*item)
        file = os.path.join(dirname, re_lock.sub('', filename))
        try:
            os.rename(lock_file, file)
        except FileNotFoundError:
            pass

def check_concurrency():
    class TestArgumentParser(argparse.ArgumentParser):
        def exit(self, status=0, message=None):
            raise Exception(message)

    global args
    
    my_pid = os.getpid()
    with open('/proc/%i/cmdline' % my_pid) as cmdline_file:
        my_cmdline = cmdline_file.read()
        i = my_cmdline.find(sys.argv[0])
        if i == -1:
            error('Unable to parse my own /proc/%i/cmdline file!' % my_pid, 3)

        i += len(sys.argv[0])
        my_argv0 = my_cmdline[0:i]

    list = os.listdir('/proc/')
    re_digit = re.compile(r'^\d+$')
    concurrency = 0

    for item in list:
        if not re_digit.match(item) or item == str(my_pid):
            continue

        with open(os.path.join('/proc', item, 'cmdline')) as cmdline_file:
            cmdline = cmdline_file.read()
            i = cmdline.find(my_argv0)
            if i == 0:
                tmp_argv = cmdline[len(my_argv0)+1:].rstrip("\x00").split("\x00")
                tmp_parser = TestArgumentParser()
                tmp_parser.add_argument('working_dir')
                tmp_parser.add_argument('command')
                tmp_args = tmp_parser.parse_known_args(tmp_argv)
                if tmp_args[0].working_dir == args.working_dir:
                    concurrency += 1
    
    if concurrency >= args.max_concurrency:
        verb(1, 'There are already running %i instances for working dir "%s"; not starting a new one.' % (concurrency, args.working_dir))
        sys.exit(4)    

def verb(level, message):
    global verbosity
    if level <= verbosity:
        print(message)

def error(message, exit_code):
    print(message, file=sys.stderr)
    sys.exit(exit_code)

# install signal handler
signal.signal(signal.SIGINT, signal_handler_sigint)

# parse arguments
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('working_dir', metavar='<DIRECTORY>', help='Working directory')
parser.add_argument('command', metavar='<COMMAND>', help='Command to run on each file. Use {} as a placeholder for the file path. If no placeholder is found, the file\'s content is piped to STDIN of the given command. On success the command MUST return with exit code 0. All other exit codes will be considered as failure and will trigger a retry.')
parser.add_argument('--verbose', '-v', action='count', help='Level of verbosity')
parser.add_argument('--sort', '-s', choices={'mtime', 'name'}, default='mtime', help='Sort by modification time or name')
parser.add_argument('--sort-reverse', '-r', default=False, action='store_true', help='Sort in reverse order')
parser.add_argument('--max-concurrency', '-c', type=int, default=10, help='Max. number of parallel file processors on the given directory')
parser.add_argument('--max-runtime', type=int, default=0, help='Maximum runtime in secondss')
parser.add_argument('--process-timeout', type=int, default=process_timeout, help='Time in seconds after which a file in process will be considered as failed (and will be retried). This is dependent on how long the processing command (--cmd) will usually take to process a single file.')
parser.add_argument('--move-to', help='Move files to given location (instead of renaming to .fp-done-...) after being successfully processed. Files are moved without sub-directories and existing files will be overwritten.')
parser.add_argument('--delete', default=False, action='store_true', help='Delete files (instead of renaming to .fp-done-...) after being successfully processed.')
args = parser.parse_args()

verbosity = args.verbose
process_timeout = args.process_timeout

working_dir = os.path.abspath(args.working_dir)
if os.path.isdir(working_dir) == False:
    error('Working directory %s does not exist!' % working_dir, 1)

move_to_dir = None
if args.move_to != None:
    move_to_dir = os.path.abspath(args.move_to)
    if os.path.isdir(move_to_dir) == False:
        error('Move-to directory %s does not exist!' % move_to_dir, 1)

    if os.path.commonpath([working_dir, move_to_dir]) == working_dir:
        error('Move-to directory must not be a sub directory of the working dir!', 1)

started = time.time()

# check concurrency
check_concurrency()

# reload file lists and do cleanup
reload_file_lists(working_dir)
cleanup()

# main loop
while True:
    try:
        item = working_list.pop()
    except IndexError:
        time.sleep(1)
        reload_file_lists(working_dir)
        continue

    dirname, filename = item
    file = os.path.join(*item)
    lock_file = os.path.join(dirname, '.fp-lock-' + time.strftime("%Y%m%dT%H%M%S") + '_' + str(os.getpid()) + '_' + filename)
    
    try:
        os.rename(file, lock_file)
    except FileNotFoundError as e:
        verb(4, 'Unable to lock file %s; skipping...' % (file))
        continue

    command = ''
    if args.command.find('{}') == -1:
        command = args.command + ' <' + lock_file
    else:
        command = args.command.replace('{}', lock_file)

    try:
        subprocess.run([command], timeout=process_timeout, check=True, shell=True)
    except subprocess.CalledProcessError as e:
        verb(1, 'Running command "%s" failed with exit code %i' % (command, e.returncode))
    except subprocess.TimeoutExpired as e:
        verb(2, 'Timeout for command "%s" after %i seconds' % (command, process_timeout))

    try:
        done_file = os.path.join(dirname, '.fp-done-' + time.strftime("%Y%m%dT%H%M%S") + '_' + str(os.getpid()) + '_' + filename)
        if args.delete:
            try:
                os.remove(lock_file)
            except OSError:
                verb(2, 'File %s could not be deleted; renaming to %s' % (lock_file, done_file))
                os.rename(lock_file, done_file)

        else:
            if move_to_dir != None:
                done_file = os.path.join(move_to_dir, filename)
            os.rename(lock_file, done_file)

    except FileNotFoundError:
        verb(2, 'Unable to rename file %s to %s' % (lock_file, done_file))

    if shutdown_state > 0:
        break

    if time.time() - started >= args.max_runtime:
        break

    
sys.exit(0)
