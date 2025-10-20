from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def map_categoria(df:DataFrame, curr_col:str, new_col:str, mapper:dict, default = None)->DataFrame:
    df[new_col] = df[curr_col].apply(lambda x: mapper.get(x, default))
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	map_categoria(curr_col='actividad', new_col='actividad2', mapper={'Equipos informáticos, electrónicos y eléctricos': 'Electrónica', 'Productos de madera, papel e impresión': 'Madera y papel', 'Productos alimenticios, bebidas y tabaco': 'Alimentos, bebidas y tabaco', 'Textiles, productos textiles, cuero y calzado': 'Textiles', 'Equipos de transporte': 'Equipos de transporte', 'Metales básicos y productos metálicos fabricados': 'Siderúrgica y metalúrgica', 'Productos químicos y minerales no metálicos': 'Química y minerales no metálicos', 'Manufactura n.c.o.p.; reparación e instalación de maquinaria y equipos': 'Reparaciones y otras manufacturas', 'Maquinaria y equipos, n.c.o.p.': 'Maquinaria y equipos'}, default=None),
	multiplicar_por_escalar(col='prop_sobre_industria', k=100),
	ordenar_dos_columnas(col1='geocodigoFundar', order1=['STP', 'COD', 'PHL', 'HKG', 'CMR', 'NGA', 'SEN', 'NZL', 'ISL', 'LAO', 'CIV', 'CRI', 'UKR', 'CHL', 'IDN', 'CYP', 'COL', 'ARG', 'JOR', 'MMR', 'GRC', 'MAR', 'PER', 'MEX', 'KAZ', 'THA', 'AUS', 'ASEAN', 'AGO', 'PAK', 'BLR', 'ROU', 'ZAF', 'ESP', 'NOR', 'HRV', 'BGR', 'EGY', 'LTU', 'BRA', 'NLD', 'CAN', 'TUN', 'LVA', 'PRT', 'GBR', 'TUR', 'FRA', 'BGD', 'RUS', 'MLT', 'BEL', 'POL', 'EST', 'APEC', 'ARE', 'G20', 'SAU', 'OECD', 'ISR', 'EU28', 'AUT', 'CHN', 'EU15', 'LUX', 'EU27_2020', 'EA19', 'ITA', 'JPN', 'KHM', 'USA', 'VNM', 'IND', 'HUN', 'MYS', 'DNK', 'CZE', 'CHE', 'SVK', 'DEU', 'SVN', 'FIN', 'IRL', 'SWE', 'SGP', 'TWN', 'KOR', 'BRN'], col2='actividad2', order2=['Alimentos, bebidas y tabaco', 'Textiles', 'Reparaciones y otras manufacturas', 'Madera y papel', 'Química y minerales no metálicos', 'Equipos de transporte', 'Siderúrgica y metalúrgica', 'Electrónica', 'Maquinaria y equipos'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 22174 entries, 0 to 22173
#  Data columns (total 6 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   anio                  22174 non-null  int64  
#   1   geocodigoFundar       22174 non-null  object 
#   2   sector                22174 non-null  object 
#   3   actividad             22174 non-null  object 
#   4   prop_sobre_industria  22174 non-null  float64
#   5   geonombreFundar       22174 non-null  object 
#  
#  |    |   anio | geocodigoFundar   | sector   | actividad                                        |   prop_sobre_industria | geonombreFundar   |
#  |---:|-------:|:------------------|:---------|:-------------------------------------------------|-----------------------:|:------------------|
#  |  0 |   2006 | COL               | C24_25   | Metales básicos y productos metálicos fabricados |              0.0436438 | Colombia          |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 792 entries, 105 to 8842
#  Data columns (total 7 columns):
#   #   Column                Non-Null Count  Dtype   
#  ---  ------                --------------  -----   
#   0   anio                  792 non-null    int64   
#   1   geocodigoFundar       792 non-null    category
#   2   sector                792 non-null    object  
#   3   actividad             792 non-null    object  
#   4   prop_sobre_industria  792 non-null    float64 
#   5   geonombreFundar       792 non-null    object  
#   6   actividad2            792 non-null    category
#  
#  |     |   anio | geocodigoFundar   | sector   | actividad                                       |   prop_sobre_industria | geonombreFundar   | actividad2   |
#  |----:|-------:|:------------------|:---------|:------------------------------------------------|-----------------------:|:------------------|:-------------|
#  | 105 |   2022 | FIN               | C26_27   | Equipos informáticos, electrónicos y eléctricos |                16.9238 | Finlandia         | Electrónica  |
#  
#  ------------------------------
#  
#  map_categoria(curr_col='actividad', new_col='actividad2', mapper={'Equipos informáticos, electrónicos y eléctricos': 'Electrónica', 'Productos de madera, papel e impresión': 'Madera y papel', 'Productos alimenticios, bebidas y tabaco': 'Alimentos, bebidas y tabaco', 'Textiles, productos textiles, cuero y calzado': 'Textiles', 'Equipos de transporte': 'Equipos de transporte', 'Metales básicos y productos metálicos fabricados': 'Siderúrgica y metalúrgica', 'Productos químicos y minerales no metálicos': 'Química y minerales no metálicos', 'Manufactura n.c.o.p.; reparación e instalación de maquinaria y equipos': 'Reparaciones y otras manufacturas', 'Maquinaria y equipos, n.c.o.p.': 'Maquinaria y equipos'}, default=None)
#  Index: 792 entries, 105 to 8842
#  Data columns (total 7 columns):
#   #   Column                Non-Null Count  Dtype   
#  ---  ------                --------------  -----   
#   0   anio                  792 non-null    int64   
#   1   geocodigoFundar       792 non-null    category
#   2   sector                792 non-null    object  
#   3   actividad             792 non-null    object  
#   4   prop_sobre_industria  792 non-null    float64 
#   5   geonombreFundar       792 non-null    object  
#   6   actividad2            792 non-null    category
#  
#  |     |   anio | geocodigoFundar   | sector   | actividad                                       |   prop_sobre_industria | geonombreFundar   | actividad2   |
#  |----:|-------:|:------------------|:---------|:------------------------------------------------|-----------------------:|:------------------|:-------------|
#  | 105 |   2022 | FIN               | C26_27   | Equipos informáticos, electrónicos y eléctricos |                16.9238 | Finlandia         | Electrónica  |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop_sobre_industria', k=100)
#  Index: 792 entries, 105 to 8842
#  Data columns (total 7 columns):
#   #   Column                Non-Null Count  Dtype   
#  ---  ------                --------------  -----   
#   0   anio                  792 non-null    int64   
#   1   geocodigoFundar       792 non-null    category
#   2   sector                792 non-null    object  
#   3   actividad             792 non-null    object  
#   4   prop_sobre_industria  792 non-null    float64 
#   5   geonombreFundar       792 non-null    object  
#   6   actividad2            792 non-null    category
#  
#  |     |   anio | geocodigoFundar   | sector   | actividad                                       |   prop_sobre_industria | geonombreFundar   | actividad2   |
#  |----:|-------:|:------------------|:---------|:------------------------------------------------|-----------------------:|:------------------|:-------------|
#  | 105 |   2022 | FIN               | C26_27   | Equipos informáticos, electrónicos y eléctricos |                16.9238 | Finlandia         | Electrónica  |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geocodigoFundar', order1=['STP', 'COD', 'PHL', 'HKG', 'CMR', 'NGA', 'SEN', 'NZL', 'ISL', 'LAO', 'CIV', 'CRI', 'UKR', 'CHL', 'IDN', 'CYP', 'COL', 'ARG', 'JOR', 'MMR', 'GRC', 'MAR', 'PER', 'MEX', 'KAZ', 'THA', 'AUS', 'ASEAN', 'AGO', 'PAK', 'BLR', 'ROU', 'ZAF', 'ESP', 'NOR', 'HRV', 'BGR', 'EGY', 'LTU', 'BRA', 'NLD', 'CAN', 'TUN', 'LVA', 'PRT', 'GBR', 'TUR', 'FRA', 'BGD', 'RUS', 'MLT', 'BEL', 'POL', 'EST', 'APEC', 'ARE', 'G20', 'SAU', 'OECD', 'ISR', 'EU28', 'AUT', 'CHN', 'EU15', 'LUX', 'EU27_2020', 'EA19', 'ITA', 'JPN', 'KHM', 'USA', 'VNM', 'IND', 'HUN', 'MYS', 'DNK', 'CZE', 'CHE', 'SVK', 'DEU', 'SVN', 'FIN', 'IRL', 'SWE', 'SGP', 'TWN', 'KOR', 'BRN'], col2='actividad2', order2=['Alimentos, bebidas y tabaco', 'Textiles', 'Reparaciones y otras manufacturas', 'Madera y papel', 'Química y minerales no metálicos', 'Equipos de transporte', 'Siderúrgica y metalúrgica', 'Electrónica', 'Maquinaria y equipos'])
#  Index: 792 entries, 8654 to 8804
#  Data columns (total 7 columns):
#   #   Column                Non-Null Count  Dtype   
#  ---  ------                --------------  -----   
#   0   anio                  792 non-null    int64   
#   1   geocodigoFundar       792 non-null    category
#   2   sector                792 non-null    object  
#   3   actividad             792 non-null    object  
#   4   prop_sobre_industria  792 non-null    float64 
#   5   geonombreFundar       792 non-null    object  
#   6   actividad2            792 non-null    category
#  
#  |      |   anio | geocodigoFundar   | sector   | actividad                                |   prop_sobre_industria | geonombreFundar       | actividad2                  |
#  |-----:|-------:|:------------------|:---------|:-----------------------------------------|-----------------------:|:----------------------|:----------------------------|
#  | 8654 |   2022 | STP               | C10T12   | Productos alimenticios, bebidas y tabaco |                92.1434 | Santo Tomé y Príncipe | Alimentos, bebidas y tabaco |
#  
#  ------------------------------
#  