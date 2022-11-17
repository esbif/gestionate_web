import pandas as pd
import matplotlib.pyplot as plt
import os

from flask import current_app

class Grapher:

    def progress_scatter(data, path, dot_size=5):

        def apply_color(row):
            return "Red" if not row["pass"] else "DarkBlue"
        data["c"] = data.apply(lambda row: apply_color(row), axis=1)

        fig, axs = plt.subplots(figsize=(20, 6))
        data.plot.scatter(
                ax=axs, x="x", y="y", c="c", s=dot_size)

        axs.set_ylabel("throughput")
        axs.set_xlabel("timestamp")

        path = os.path.join(current_app.config["GRAPH_FOLDER"], path)
        fig.savefig(path)

        return True
        
    def progress_stacked_area(data, path):
        fig, axs = plt.subplots(figsize=(20, 6))
        data.plot.area(ax=axs, linewidth=0.5)
        axs.set_ylabel("number of failed tests")
        axs.set_xlabel("timestamp (res=1hr)")
        
        # Shrink current axis by 20%
        box = axs.get_position()
        axs.set_position([box.x0, box.y0, box.width * 0.8, box.height])

        # Put a legend to the right of the current axis
        axs.legend(loc='center left', bbox_to_anchor=(1, 0.5))

        path = os.path.join(current_app.config["GRAPH_FOLDER"], path)
        fig.savefig(path)

        return True

