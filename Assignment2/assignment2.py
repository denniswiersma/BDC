#!/usr/local/bin/python3.11

"""
Module Docstring
"""

# IMPORTS
import argparse as ap
import multiprocessing as mp
import queue
import sys
import time
from multiprocessing.managers import BaseManager

import numpy as np
import pandas as pd


def parse_args():
    """
    Parse the command line arguments
    :return: An argparse object containing the arguments
    """
    # Create the parser
    arg_parser = ap.ArgumentParser(
        description="Script voor Opdracht 2 van Big Data Computing"
    )

    # mutually exclusive client/server arguments
    client_server_mode = arg_parser.add_mutually_exclusive_group(required=True)
    client_server_mode.add_argument(
        "-s",
        "--server",
        action="store_true",
        help="Run the program in Server mode; see extra options needed below",
    )
    client_server_mode.add_argument(
        "-c",
        "--client",
        action="store_true",
        help="Run the program in Client mode; see extra options needed below",
    )

    # server args
    server_args = arg_parser.add_argument_group(
        title="Arguments when run in server mode"
    )
    # Add argument for the output file
    server_args.add_argument(
        "-o",
        "--output",
        action="store",
        dest="csvfile",
        type=ap.FileType("w", encoding="UTF-8"),
        required=False,
        help="CSV file om de output in op te slaan. Default is output "
        "naar terminal STDOUT",
    )
    # Add argument for the input files
    server_args.add_argument(
        "fastq_files",
        action="store",
        type=ap.FileType("r"),
        nargs="*",
        help="Minstens 1 Illumina Fastq Format file om te verwerken",
    )
    server_args.add_argument(
        "-k",
        "--chunks",
        action="store",
        dest="chunks",
        type=int,
        help="Aantal chunks om te gebruiken.",
    )

    # client args
    client_args = arg_parser.add_argument_group(
        title="Arguments when run in client mode"
    )
    # Add argument for the number of cores to use
    client_args.add_argument(
        "-n",
        action="store",
        dest="n",
        type=int,
        help="Aantal cores om te gebruiken.",
    )
    # Add argument for the server address
    arg_parser.add_argument(
        "-t",
        "--host",
        action="store",
        dest="host",
        type=str,
        default="localhost",
        help="IP adres van de server",
    )
    # Add argument for the server port
    client_args.add_argument(
        "-p",
        "--port",
        action="store",
        dest="port",
        type=int,
        help="Poortnummer van de server",
    )

    return arg_parser.parse_args()


# CLASSES
class Server:
    """
    A class to create a server for the client-server model.
    """

    POISONPILL = "MEMENTOMORI"

    def __init__(self, host, port, authkey, csvfile=None):
        self.host = host
        self.port = port
        self.authkey = authkey
        self.csvfile = csvfile

    def make_server_manager(self):
        """
        Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
        """
        job_q = queue.Queue()
        result_q = queue.Queue()

        class QueueManager(BaseManager):
            """
            A class to create a manager for the server.
            """

        QueueManager.register("get_job_q", callable=lambda: job_q)
        QueueManager.register("get_result_q", callable=lambda: result_q)

        manager = QueueManager(
            address=(self.host, self.port), authkey=self.authkey
        )
        manager.start()
        print(f"Server started at port {self.port}")
        return manager

    def runserver(self, function, data):
        """
        Run the server, sending data to the client.
        """
        # Start a shared manager server and access its queues
        manager = self.make_server_manager()
        shared_job_q = manager.get_job_q()
        shared_result_q = manager.get_result_q()

        if not data:
            print("No data to send!")
            return
        print("Ready to send data to clients!")
        for data_part in data:
            shared_job_q.put({"fn": function, "arg": data_part})
        results = []
        while True:
            try:
                result = shared_result_q.get_nowait()
                results.append(result)
                if len(results) == len(data):
                    print("Got all results!")
                    break
            except queue.Empty:
                continue
        # Tell the client process no more data will be forthcoming
        print("Time to kill some peons!")
        shared_job_q.put(Server.POISONPILL)
        # Sleep a bit before shutting down the server - to give clients time to
        # realize the job queue is empty and exit in an orderly way.
        time.sleep(5)
        means = [r["result"] for r in results]
        total_means = MeanPhredCalculator.calculate_total_means(means)
        # Write to output
        if self.csvfile:
            pd.DataFrame(total_means).to_csv(self.csvfile, header=False)
        else:
            pd.DataFrame(total_means).to_csv(sys.stdout, header=False)
        print("Shutting down server")
        manager.shutdown()


class Client:
    """
    A class to create a client for the client-server model.
    """

    POISONPILL = "MEMENTOMORI"

    def make_client_manager(self, ipaddress, port, authkey):
        """Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
        """

        class ServerQueueManager(BaseManager):
            """
            A class to create a manager for the client.
            """

        ServerQueueManager.register("get_job_q")
        ServerQueueManager.register("get_result_q")

        manager = ServerQueueManager(address=(ipaddress, port), authkey=authkey)
        manager.connect()

        print(f"Client connected to {ipaddress}:{port}")
        return manager

    def runclient(self, num_processes, ipaddress, port, authkey):
        """
        Run the client, connecting to the server and starting the worker
        processes.
        """
        manager = self.make_client_manager(ipaddress, port, authkey)
        job_q = manager.get_job_q()
        result_q = manager.get_result_q()
        self.run_workers(job_q, result_q, num_processes)

    def run_workers(self, job_q, result_q, ncores):
        """
        Run the worker processes. This function creates a number of
        processes and starts them. Each process runs the peon function,
        which gets jobs from the job queue and puts results in the result
        queue.
        """
        processes = []
        for _ in range(ncores):
            temp_process = mp.Process(
                target=Client.peon, args=(job_q, result_q)
            )
            processes.append(temp_process)
            temp_process.start()
        print(f"Started {len(processes)} workers!")
        for temp_process in processes:
            temp_process.join()

    @staticmethod
    def peon(job_q, result_q):
        """
        A worker process that gets jobs from the job queue and puts
        results in the result queue.
        """
        my_name = mp.current_process().name
        while True:
            try:
                job = job_q.get_nowait()
                if job == Client.POISONPILL:
                    job_q.put(Client.POISONPILL)
                    print("killed peon", my_name)
                    return
                else:
                    try:
                        result = job["fn"](job["arg"])
                        result_q.put({"job": job, "result": result})
                    except NameError as error:
                        print("Error in worker process", error)
                        result_q.put({"job": job, "result": "some error"})

            except queue.Empty:
                print("sleepytime for", my_name)
                time.sleep(1)


class MeanPhredCalculator:
    """
    A class to calculate the mean phred score of a fastq file.
    """

    def __init__(self):
        return

    @staticmethod
    def read_phreds(file):
        """
        Return the PHRED scores from a FASTQ file as a list of Numpy arrays.
        """
        phred_scores = []
        for i, line in enumerate(file):
            if i % 4 == 3:
                phred_scores.append(line.strip())

        return phred_scores

    @staticmethod
    def batch_iterator(iterator, batch_size):
        """Returns lists of length batch_size.

        This can be used on any iterator, for example to batch up
        SeqRecord objects from Bio.SeqIO.parse(...), or to batch
        Alignment objects from Bio.Align.parse(...), or simply
        lines from a file handle.

        This is a generator function, and it returns lists of the
        entries from the supplied iterator.  Each list will have
        batch_size entries, although the final list may be shorter.

        Found at https://biopython.org/wiki/Split_large_file
        """
        batch = []
        for entry in iterator:
            batch.append(entry)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    @staticmethod
    def calculate_means_from_batch(batch):
        """
        Calculate the means of the phred score from a batch of records
        :param batch: A batch of records
        :return: A list of the mean phred scores for each base position in the records
        """
        ascii_dict = {chr(i): i - 33 for i in range(33, 127)}
        phreds = np.array(
            [[ascii_dict[ch] for ch in phred_line] for phred_line in batch]
        )
        return np.mean(phreds, axis=0)

    @staticmethod
    def calculate_total_means(means_per_batch):
        """
        Calculate the total mean phred score per base position from a list of means per batch
        :param means_per_batch: A list of means per batch
        :return: The total mean phred score
        """
        total_means = np.vstack(means_per_batch)
        return total_means.mean(axis=0)


# FUNCTIONS


# MAIN
def main():
    """
    Main function
    """
    args = parse_args()
    authkey = b"secretauthkey"

    if args.server:
        server = Server(
            host=args.host,
            port=args.port,
            authkey=authkey,
            csvfile=args.csvfile,
        )

        mpc = MeanPhredCalculator()
        for file in args.fastq_files:
            print(f"Reading file {file.name}")
            records = mpc.read_phreds(file)
            batches = list(mpc.batch_iterator(records, args.chunks))
            server.runserver(
                function=MeanPhredCalculator.calculate_means_from_batch,
                data=batches,
            )

    elif args.client:
        client = Client()
        client.runclient(
            num_processes=args.n,
            ipaddress=args.host,
            port=args.port,
            authkey=authkey,
        )

    # for file in mpc.args.fastq_files:
    #     print("reading file")
    #     records = mpc.read_phreds(file)
    #     print("Calculating batches")
    #     batches = list(mpc.batch_iterator(records, 5000))
    #     print("Calculating means")
    #     with mp.Pool(mpc.args.n) as pool:
    #         means_per_batch = pool.map(mpc.calculate_means_from_batch, batches)
    #     print("calculating total means")
    #     total_means = mpc.calculate_total_means(means_per_batch)
    #
    #     print("writing to csv")
    #     if mpc.args.csvfile:
    #         mpc.write_to_csv(total_means)
    #     else:
    #         mpc.write_to_stdout(total_means)


if __name__ == "__main__":
    main()
