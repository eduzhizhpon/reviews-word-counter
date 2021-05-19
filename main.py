import count_word as cw
import argparse
import report
import sys

def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--sample", type=str,  
                                help="Crear muestreos aleatorios")

    parser.add_argument("-d", "--delete", action='store_true',  
                                help="Eliminar los archivos generados")

    parser.add_argument("-p", "--plot-report", action='store_true',  
                                help="Realizar gráfica de los resultados")

    parser.add_argument("-r", "--run", action='store_true',  
                                help="Comenzar")

    parser.add_argument("-c", "--core", type=int, default=5, 
                                help="Cantidad de cores")

    return vars(parser.parse_args())

def main():
    args = get_args()

    if not args['sample'] == None:
        cw.make_sample_step(train_path=args['sample'], step=0.05)

    if not len(sys.argv) > 1 or args['run']:
        cw.n_core = args['core']
        print(f'Comenzando entrenamiento con: {cw.n_core} núcleos')
        report.list_to_csv(cw.start(), name='resultados.csv')
    
    if args['plot_report']:
        report.plot_serial_process_time(name='resultados_comparativa.png', df_name='resultados.csv')

    if args['delete']:
        cw.delete_all()

if __name__ == '__main__':
    main()