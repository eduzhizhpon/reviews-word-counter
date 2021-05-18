import pandas as pd
import matplotlib.pyplot as plt
from os import path
from os import makedirs

dir_path = 'reports'

def list_to_csv(l, name='dataframe.csv'):
    create_dir(dir_path)
    df = pd.DataFrame(l[1:], columns=l[:1])
    df.to_csv(dir_path + '/' + name, index=False)
    df = None

def create_dir(dir_path):
    if not path.exists(dir_path):
        makedirs(dir_path)

def plot_serial_process_time(name='fig1.png', df_name='dataframe.csv'):
    
    df = pd.read_csv(dir_path + "/" + df_name)
    x = df['amount']
    serial_y = df['serial_time']
    process_y = df['process_time']

    plt.plot(x, serial_y, ls='--', marker='o')
    plt.plot(x, process_y, ls='--', marker='o')

    plt.xlabel('Cantidad de muestras')
    plt.ylabel('Tiempo de ejecución')
    plt.legend(['Serial', 'Procesos'])
    plt.title('Comparación de tiempos de ejecución')

    create_dir(dir_path)
    plt.savefig(dir_path + '/' + name, dpi=300)