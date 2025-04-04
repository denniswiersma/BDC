#!/usr/bin/env python3

"""
Analyseer de variatie in PHRED scores over meerdere runs.
"""

# IMPORTS
import os

import numpy as np
import pandas as pd


# CLASSES
class Analyser:
    """
    Analyseer de numerieke instabiliteit van floating point berekeningen
    tussen verschillende aantallen workers en herhalingen.
    """

    def __init__(self):
        self.results = {}

    def load_data(self):
        for workers in range(1, 5):
            self.results[workers] = []
            for rep in range(1, 4):
                filename = f"results_w{workers}_r{rep}.csv"
                if os.path.exists(filename):
                    arr = (
                        pd.read_csv(filename, header=None)
                        .squeeze("columns")
                        .to_numpy()
                    )
                    self.results[workers].append(arr)
                else:
                    print(f"{filename} niet gevonden.")

    def analyse(self):
        print(
            "a. Hoeveel verschil tussen de PHRED scores zie je optreden binnen een run (standaard deviatie)? Is dit constant over de read?\n"
        )

        for workers, runs in self.results.items():
            if len(runs) < 2:
                continue
            stacked = np.vstack(runs)
            stds = np.std(stacked, axis=0)
            mean_std = np.mean(stds)
            max_std = np.max(stds)
            max_pos = np.argmax(stds)

            print(f"Voor {workers} workers:")
            print(
                f"  Gemiddelde standaarddeviatie over alle posities: {mean_std:.4f}"
            )
            print(
                f"  Grootste afwijking op positie {max_pos} met een stddev van {max_std:.4f}"
            )

        print(
            "b. Hoeveel verschil zie je tussen de PHRED scores die optreden tussen runs met minder of meer workers? Voor hoeveel workers is de standaard deviatie het grootst?\n"
        )

        stds_per_worker = {
            workers: np.mean(np.std(np.vstack(runs), axis=0))
            for workers, runs in self.results.items()
            if len(runs) >= 2
        }

        for workers, mean_std in stds_per_worker.items():
            print(
                f"{workers} workers: gemiddelde stddev tussen runs = {mean_std:.4f}"
            )

        if stds_per_worker:
            largest_std = max(stds_per_worker, key=stds_per_worker.get)
            print(
                f"De grootste standaarddeviatie tussen runs zie je bij {largest_std} workers"
            )
            print(
                "Het lijkt erop dat de verschillende runs exact dezelfde resultaten bevatten \n"
            )

    def analyse_times(self):
        df = pd.read_csv("timings.csv", names=["workers", "runtime"])
        print("Times:")

        for workers in sorted(df["workers"].unique()):
            runtimes = df[df["workers"] == workers]["runtime"]
            avg = runtimes.mean()
            std = runtimes.std()
            print(
                f"{workers} workers: gemiddelde tijd {avg:.2f} sec, stddev {std:.2f} sec"
            )

        besttime = df.groupby("workers")["runtime"].mean().idxmin()
        print(f"Snelste gemiddelde tijd was bij {besttime} workers")


# MAIN
def main():
    analyser = Analyser()
    analyser.load_data()
    analyser.analyse()
    analyser.analyse_times()


if __name__ == "__main__":
    main()
