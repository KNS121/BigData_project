import matplotlib.pyplot as plt
import numpy as np

class WellPlotter:
    def __init__(self):
        pass

    def normalize_coords(self, coords):
        x_coords, y_coords = zip(*coords)
        x_coords = np.array(x_coords)
        y_coords = np.array(y_coords)
        x_min, x_max = np.min(x_coords), np.max(x_coords)
        y_min, y_max = np.min(y_coords), np.max(y_coords)
        x_coords = (x_coords - x_min) / (x_max - x_min)
        y_coords = (y_coords - y_min) / (y_max - y_min)
        return list(zip(x_coords, y_coords))

    def plot_wells(self, wells):
        for field, well_data in wells.items():
            production_coords = well_data['production']
            injection_coords = well_data['injection']

            #if production_coords:
             #   production_coords = self.normalize_coords(production_coords)
            #if injection_coords:
             #   injection_coords = self.normalize_coords(injection_coords)

            plt.figure()

            if production_coords:
                x_coords, y_coords = zip(*production_coords)
                plt.scatter(x_coords, y_coords, color='blue', label='Добывающие скважины', marker = '.')

            if injection_coords:
                x_coords, y_coords = zip(*injection_coords)
                plt.scatter(x_coords, y_coords, color='red', label='Нагнетательные скважины', marker = '.')

            plt.xlabel('X')
            plt.ylabel('Y')
            plt.title(f'Расположение скважин на месторождении {field}')
            plt.legend()
            plt.grid(True)
            #plt.savefig(f'{field}_wells.png')
            #plt.close()
            plt.show()