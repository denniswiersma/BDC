#!/usr/local/bin/python3.11


"""
Calculate the mean phred score for each base position in a fastq file.
"""

# IMPORTS
import argparse as ap
import sys
import warnings

import numpy as np
import pandas as pd


# CLASSES
class MeanPhredCalculator:
    """
    A class to calculate the mean phred score of a fastq file.
    """

    def __init__(self):
        self.args = self.parse_args()

    @staticmethod
    def parse_args():
        """
        Parse the command line arguments
        :return: An argparse object containing the arguments
        """
        # Create the parser
        arg_parser = ap.ArgumentParser(
            description="Script voor Opdracht 1 van Big Data Computing"
        )

        mode = arg_parser.add_mutually_exclusive_group(required=True)
        mode.add_argument("--chunkmode", action="store_true")
        mode.add_argument("--totalmode", action="store_true")

        # Add argument for the output file
        arg_parser.add_argument(
            "-o",
            action="store",
            dest="csvfile",
            type=ap.FileType("w", encoding="UTF-8"),
            required=False,
            help="CSV file om de output in op te slaan. Default is output "
            "naar terminal STDOUT",
        )

        return arg_parser.parse_args()

    @staticmethod
    def read_phreds():
        """
        Return the PHRED scores from a FASTQ file as a list of Numpy arrays.
        """
        for i, line in enumerate(sys.stdin):
            if i % 4 == 3:
                yield line.strip()

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
        max_length = max(len(line) for line in batch)
        phreds = []

        for phred_line in batch:
            phreds_num = np.full((max_length,), np.nan)
            for i, character in enumerate(phred_line):
                phreds_num[i] = ascii_dict[character]
            phreds.append(phreds_num)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            with np.errstate(invalid="ignore"):
                return np.nanmean(phreds, axis=0)

    @staticmethod
    def calculate_total_means(means_per_batch):
        """
        Calculate the total mean phred score per base position from a list of means per batch
        :param means_per_batch: A list of means per batch
        :return: The total mean phred score
        """
        total_means = np.vstack(means_per_batch)
        means = total_means.mean(axis=0)
        means = means[~np.isnan(means)]
        return means

    def write_to_csv(self, total_means):
        """
        Write the total means to a csv file

        :param total_means: An array of the total means
        """
        data_frame = pd.DataFrame(total_means)
        data_frame.to_csv(self.args.csvfile, header=False)

    @staticmethod
    def write_to_stdout(total_means):
        """
        Write the total means to stdout

        :param total_means: An array of the total means
        """
        data_frame = pd.DataFrame(total_means)
        data_frame.to_csv(sys.stdout, header=False)


# FUNCTIONS


# MAIN
def main():
    """
    Main function
    """
    mpc = MeanPhredCalculator()

    if mpc.args.chunkmode:
        records = list(mpc.read_phreds())
        means = mpc.calculate_means_from_batch(records)
        if len(means) > 1:
            print(" ".join(str(x) for x in means))

    elif mpc.args.totalmode:
        means_per_batch = []
        for line in sys.stdin:
            values = np.fromstring(line.strip(), sep=" ")
            means_per_batch.append(values)

        max_len = max(len(arr) for arr in means_per_batch)
        padded = [
            np.pad(arr, (0, max_len - len(arr)), constant_values=np.nan)
            for arr in means_per_batch
        ]

        total = mpc.calculate_total_means(padded)
        pd.DataFrame(total).to_csv(sys.stdout, header=False, index=False)


if __name__ == "__main__":
    main()
