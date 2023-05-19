#!/usr/local/bin/python3.9

"""
Module description
"""

# IMPORTS
import argparse as ap


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
        arg_parser = ap.ArgumentParser(description="Script voor Opdracht 1 van Big Data Computing")

        # Add argument for the number of cores to use
        arg_parser.add_argument("-n", action="store",
                                dest="n", required=True, type=int,
                                help="Aantal cores om te gebruiken.")
        # Add argument for the output file
        arg_parser.add_argument("-o", action="store", dest="csvfile",
                                type=ap.FileType('w', encoding='UTF-8'),
                                required=False,
                                help="CSV file om de output in op te slaan. Default is output "
                                     "naar terminal STDOUT")
        # Add argument for the input files
        arg_parser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+',
                                help="Minstens 1 Illumina Fastq Format file om te verwerken")

        return arg_parser.parse_args()


# FUNCTIONS

# MAIN
def main():
    """
    Main function
    """
    mpc = MeanPhredCalculator()


if __name__ == "__main__":
    main()
