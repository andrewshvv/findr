import subprocess


def run_pkill(process_name):
    # Run the pkill command to kill the processes
    subprocess.call(['pkill', '-f', process_name])

