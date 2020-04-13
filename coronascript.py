# Dask es una libreria para el procesamiento distribuido de conjuntos ingentes de datos
import dask.dataframe as dd
from dask.distributed import Client

# Libreria para análisis de datos
import pandas as pd

# Libreria para obtener datos de la memoria libre y cores de la máquina
import psutil as ps

# Libreria para expresiones regulares
import re

# Directorios de entrada y salida de datos
path_dir_input = '/data/'
path_dir_output = '/output/'

#path_dir_input = 'C:/input/'
#path_dir_output = 'C:/output/'

# Se muestra la memoria libre, disponible y usada de la máquina donde se ejecutará el proceso
print(ps.virtual_memory())

# Se obtienen la memoria libre, número de cores y threads que seran usados como parametros por el proceso
# encargado de ejecutar el analisis de datos.
n_workers = ps.cpu_count()
memory_free = ps.virtual_memory().free
n_threads_per_worker = 2

# Cliente de Dask que ejecutará el proceso de análisis de datos 
client = Client(n_workers=n_workers, threads_per_worker=n_threads_per_worker, processes=False,
                memory_limit=memory_free, scheduler_port=0, 
                silence_logs=True, diagnostics_port=0)
				
# Expresión regular que se usará para indentificar los valores a convertir en float durante la conversión de weight_pounds a kg			
rex = re.compile("[0-9]*[.,][0-9]*")
			
# Función para la conversión de libras a kg para ser aplicada sobre el campo weight_pounds durante lectura de csv's			
def convertPoundsToKg(x):
	if(rex.match(x.strip()) and x != 'NaN'):
		try:
			weigth_kg = (float(x) / 2.2046)
			return weigth_kg
		except: 
			print("Error en conversión del valor: "+x)
	else:		
		return 0
		
# Función para establecer la década en función de year para agrupar los datos
def calculateDecade(x):
	row = int(x)
	if row >= 1970 and row <= 1979:
		return '70'
	elif row >= 1980 and row <= 1989:
		return '80'
	elif row >= 1990 and row <= 1999:
		return '90'
	else:
		return '00'		

# Columnas que se van a leer del csv
usecols = ['source_year', 'year','state','is_male','child_race','weight_pounds']

# Convertidores que serán aplicados en las columnas weigth_pounds y source_year
converters = {'weight_pounds': convertPoundsToKg, 'source_year':calculateDecade}

# Lectura de ficheros csv de directorio de entrada /input/natalidad0000*.csv
dfNatalidadRow = dd.read_csv(path_dir_input+'natalidad0000000000*.csv', engine = "c", low_memory=False, header="infer", delimiter=",", 
					usecols = usecols, converters = converters)

# Eliminar NaN values en las columnas donde están presentes
dfNatalidad = dfNatalidadRow.dropna(subset=['state', 'child_race'])

# Conversion a tipos de datos más eficientes
dfNatalidad['source_year'] = dfNatalidad.source_year.astype('int64')
dfNatalidad['year'] = dfNatalidad.year.astype('int64')
dfNatalidad['state'] = dfNatalidad.state.astype('category')
dfNatalidad['child_race'] = dfNatalidad.child_race.astype('int64')
dfNatalidad['weight_pounds'] = dfNatalidad.weight_pounds.astype('float')

# Índices
dfNatalidad.set_index("child_race")

# Lectura de fichero csv de razas
dfRace = dd.read_csv(path_dir_input+'race.csv', engine="c", delimiter=",", header="infer", memory_map = "true")

# Conversion a tipos de datos más eficientes
dfRace['id_race'] = dfRace.id_race.astype('int64')
dfRace['race'] = dfRace.race.astype('category')

# Índice
dfRace.set_index("id_race")

# Lectura de fichero csv de sexos
dfSex = dd.read_csv(path_dir_input+'sex.csv', engine="c", delimiter=",", header="infer", memory_map = "true")

# Conversion a tipos de datos más eficientes
dfSex['de_sex'] = dfSex.de_sex.astype('category')

#Índice
dfSex.set_index("id_sex")

# Obtener descripcion de la raza de bebés
dfNatalidad_raza = dfNatalidad.merge(dfRace, how="left", left_index=True, right_index=True)[['source_year', 'state', 'year', 'race', 'is_male', 'weight_pounds']]

dfNatalidad_raza.set_index("is_male")

# Obtener descripción del sexo de bebés
dfNatalidad_raza_sexo = dfNatalidad_raza.merge(dfSex, how="left", left_index=True, right_index=True)[['source_year', 'state', 'year', 'race', 'de_sex', 'weight_pounds']]

# Índice
dfNatalidad_raza_sexo.set_index("year")

# Función que realiza las diferentes consultas agrupadas por state
def calcularDatos(dataNatalidadByDecade):
    # Se convierte dataframe de Dask a dataframe de Pandas	
	dfNatalidadByDecade = dataNatalidadByDecade.compute()
	
	#Prefijo para identificar datos de cada década
	prefix = dfNatalidadByDecade['source_year'].values[0]
	
	suffix = prefix.astype('str')
	prefix_born = 'B'+suffix
	prefix_race = 'Race'+suffix
	prefix_male = 'Male_'+suffix
	prefix_female = 'Female_'+suffix
	prefix_weight = 'Weight_'+suffix
	
	# Número de nacimientos por state
	dfNacimientosByState = dfNatalidadByDecade.groupby(['state']).agg({'year':'count', 'weight_pounds':'mean'}).rename(columns={'year':prefix_born, 'weight_pounds':prefix_weight}).reset_index()
	
	# Número de nacimientos por state y race
	dfNacimientosByRaceState = dfNatalidadByDecade.groupby(['state', 'race']).agg({'year':'count'}).reset_index().rename(columns={'year':'NumBornByRace'})
	
	# Raza con máximo número de nacimientos por state
	dfMaxNacimientosByRaceState = dfNacimientosByRaceState.loc[dfNacimientosByRaceState.groupby(['state'])['NumBornByRace'].idxmax()].reset_index().rename(columns={'race':prefix_race})
		
	# leff join entre los df de nacimientos por state y máximo número de nacimientos por race y state
	dfNacimientosByDecade = dfNacimientosByState.merge(dfMaxNacimientosByRaceState, how="left", left_on="state", right_on="state")[['state', prefix_born, prefix_race, prefix_weight]]
    
	# Número de nacimientos por state y de_sex
	dfNacimientosBySexDecade = dfNatalidadByDecade.groupby(['state', 'de_sex']).agg({'year':'count'}).reset_index()
	
	# Número de nacimientos por state y de_sex Male
	dfNacimientosHombresByDecade = dfNacimientosBySexDecade[dfNacimientosBySexDecade['de_sex'] == 'Male'].rename(columns={'year':prefix_male})
	
	# Número de nacimientos por state y de_sex Female
	dfNacimientosMujeresByDecade = dfNacimientosBySexDecade[dfNacimientosBySexDecade['de_sex'] == 'Female'].rename(columns={'year':prefix_female})
    
	dfNacimientosByDecadeSubTotal =  dfNacimientosByDecade.merge(dfNacimientosHombresByDecade, how="left", on="state")[['state', prefix_born, prefix_race, prefix_male, prefix_weight]]
	
	# Consolidacion de nacimientos de hombres y mujeres por state
	dfNacimientosByDecadeTotal =  dfNacimientosByDecadeSubTotal.merge(dfNacimientosMujeresByDecade, how="left", on="state")[['state', prefix_born, prefix_race, prefix_male, prefix_female, prefix_weight]]
	
	return dfNacimientosByDecadeTotal
	

# Función que invoca al proceso de análisisde datos
def process_data(dfNatalidad_raza_sexo):	
	dfResult = calcularDatos(dfNatalidad_raza_sexo)
	return dfResult
	
# Se crea dataframe por década	
df70 = dfNatalidad_raza_sexo[dfNatalidad_raza_sexo.year.isin(range(1970, 1979))]
df80 = dfNatalidad_raza_sexo[dfNatalidad_raza_sexo.year.isin(range(1980, 1989))]
df90 = dfNatalidad_raza_sexo[dfNatalidad_raza_sexo.year.isin(range(1990, 1999))]
df2k = dfNatalidad_raza_sexo[dfNatalidad_raza_sexo.year.isin(range(2000, 2010))]

# Se compone array de dataframes agrupados por década
df_arr = [df70, df80, df90, df2k]

# Función que invoca al proceso de análisis de datos para cada década, usando el Client, 
# donde se indica la memoria libre, cores, etc., para ejecutar el análisis.
df_map = client.map(process_data, df_arr)

# Se obtienen los datos calculados por la función
results_arr = client.gather(df_map)

# Se crean índices para la columna state, por la cual se van a relacionar los dataframes resultantes
results_arr[0] = results_arr[0].set_index('state') 
results_arr[1] = results_arr[1].set_index('state')
results_arr[2] = results_arr[2].set_index('state')
results_arr[3] = results_arr[3].set_index('state')

# Se cruzan todos los dataframes resultantes por el campo state, el cual tiene se le ha asignado un índice anteriormente
dfTotal = results_arr[0].join([results_arr[1], results_arr[2], results_arr[3]]) 
# Se consolida el numero de hombres nacidos en las diferentes décadas del 70 al 2010
dfTotal['Male'] = dfTotal['Male_70'] + dfTotal['Male_80'] + dfTotal['Male_90'] + dfTotal['Male_0']
# Se consolida el numero de mujeres nacidas en las diferentes décadas del 70 al 2010
dfTotal['Female'] = dfTotal['Female_70'] + dfTotal['Female_80'] + dfTotal['Female_90'] + dfTotal['Female_0']
# Se consolida el peso medio en kg de todos los nacidos en las diferentes décadas del 70 al 2010
dfTotal['Weight'] = (dfTotal['Weight_70'] + dfTotal['Weight_80'] + dfTotal['Weight_90'] + dfTotal['Weight_0']) / 4

print(dfTotal.head(20))

# Columnas que tendra el csv resultante
header = ['state', 'B70', 'B80', 'B90', 'B0', 'Race70', 'Race80', 'Race90', 'Race0', 'Male', 'Female', 'Weight']
# Se exportan los resultados a csv en el directorio /output
dfTotal.reset_index().to_csv(path_dir_output+'output_ojfuquene.csv', columns = header, index=False)