###################################
# # # 01 ETL DATOS CLIMATICOS # # #
###################################
#------------#
# # INDICE # #
#------------#
# 1. Cargue de librerias
# 2. Funcion de consolidado de bases
#    - Unificacion de archivos
#    - Seleccion de variables a emplear
#    - Media movil con ventana k para faltantes
# 3. Exportar datos 
# 4. Funcion consolidada

#==================================================================

#--------------------------------#
# # # 1. Cargue de librerias # # #
#--------------------------------#
import pandas as pd
import numpy as np
import os

import warnings
warnings.filterwarnings('ignore')

#--------------------------------------------#
# # # 2. Funcion de consolidado de bases # # #
#--------------------------------------------#
def cons_bases(ruta):
    '''
    Consolida los archivos con la informacion climatologica de un folder dado
    Argumentos:
        -  ruta: ruta de la carpeta con los archivos
    Retorno:
        -  base consolidada con las series de tiempo
    '''

    # Lista de bases
    archivos = os.listdir(ruta)
    archivos = [archivo for archivo in archivos if "Bogota" in archivo]
    print(f"SE ENCONTRARON {len(archivos)} ARCHIVOS EN LA CARPETA")

    #    - Unificacion de archivos
    df_consolidado = pd.DataFrame()
    for i in archivos:
        df_temp = pd.read_csv(ruta + i)
        df_consolidado = pd.concat([df_consolidado, df_temp])
        print("se ha consolidado: ", i)
    
    #    - Seleccion de variables a emplear
    df_consolidado = df_consolidado[[
        'datetime', 'tempmax', 'tempmin', 'temp',
        # 'feelslikemax','feelslikemin', 'feelslike', 'dew',
        'humidity', 'precip', #'precipprob', 'precipcover',
        'windspeed', # 'winddir', 'sealevelpressure',
        # 'cloudcover', # 'visibility',
        'solarradiation' #, 'solarenergy', 'uvindex' #, 'sunrise', 'sunset', 'moonphase', 'conditions', 'description', 'icon', 'stations'
        ]]
    
    #    - Media movil con ventana k para faltantes
    for col in df_consolidado.columns:
        try:
            df_consolidado[col] = df_consolidado[col].fillna(method='ffill').fillna(method='bfill')
        except:
            pass
    
    print(f"BASE CONSOLIDADA CON EXITO!! {df_consolidado.shape[0]} filas y {df_consolidado.shape[1]} columnas 👌😎")
    print(df_consolidado.head())

    return df_consolidado
        
#---------------------------#        
# # # 3. Exportar datos # # #
#---------------------------#
def exportador_datos(ruta, archivo):
    '''
    Exporta el archivo consolidado en la carpeta Bases_Datos bajo el nombre Series_consolidadas
    Argumentos:
        -  ruta: ruta de la carpeta con los archivos
        -  archivo: Archivo que se exportara en csv
    Retorno:
        -  base consolidada con las series de tiempo en la carpeta definida
    '''

    # Exportando en csv
    archivo.to_csv(ruta+"Series_consolidadas.csv", index=False)
    print("Archivo con serie consolidada exportado con exito!! 👌👌")


#--------------------------------#
# # # 4. Funcion consolidada # # #
#--------------------------------#
def main(ruta):
    '''
    '''
    # Consolidando y depurando la informacion
    print("\n")

    ruta = ruta.strip().replace("\\", "/") 
    if not ruta.endswith("/"):
        ruta += "/"
    df = cons_bases(ruta)

    # Exportando la base consolidada
    print("\n")
    exportador_datos(ruta = ruta, archivo = df)



# Prueba
if __name__ == "__main__":
    ruta = input("Ingrese la ruta del folder con los datos: ")
    if not os.path.exists(ruta):
        print("⚠️ La ruta ingresada no es válida. Verifica e intenta nuevamente.")
    else:
        main(ruta=ruta)
