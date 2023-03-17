import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression

class Graph:
    def __init__(self):
        pass

    def graph_plot(self, data, headers, name_x, name_y):
        years = list(range(1990, 2020))

        for i, sublist in enumerate(data):
            # Extract data from sublist and convert to a numpy array
            values = sublist[0][0].split()
            x = np.arange(1, len(values) + 1)
            y = np.array(values, dtype=float)

            plt.title(headers[i])
            plt.xlabel(name_x)
            plt.ylabel(name_y)

            plt.scatter(x, y)
            m, b = np.polyfit(x, y, 1)
            slope = (m * x) + b
            plt.plot(x, slope)
            plt.xticks(x[::3], years[::3])
            plt.show()
