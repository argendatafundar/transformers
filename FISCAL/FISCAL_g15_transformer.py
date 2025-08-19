from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def custom_logic(df: DataFrame) -> DataFrame:
    df['categoria'] = df.apply(
        lambda row: row['subfunciones'] if pd.notnull(row['subfunciones'])
        else row['funciones'] if pd.notnull(row['funciones'])
        else row['finalidades'] if pd.notnull(row['finalidades'])
        else (_ for _ in ()).throw(ValueError("No se encontró valor para 'categoria'")), axis=1
    )
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='finalidades', curr_value='SERVICIOS DE LA DEUDA PÚBLICA', new_value='Servicios de la deuda pública'),
	custom_logic()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1188 entries, 0 to 1187
#  Data columns (total 6 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       1188 non-null   int64  
#   1   codigo                     1188 non-null   object 
#   2   finalidades                1188 non-null   object 
#   3   funciones                  1144 non-null   object 
#   4   subfunciones               660 non-null    object 
#   5   gasto_publico_consolidado  1188 non-null   float64
#  
#  |    |   anio | codigo   | finalidades               | funciones              | subfunciones   |   gasto_publico_consolidado |
#  |---:|-------:|:---------|:--------------------------|:-----------------------|:---------------|----------------------------:|
#  |  0 |   1980 | 1.1.1    | FUNCIONAMIENTO DEL ESTADO | Administración general |                |                     1.84962 |
#  
#  ------------------------------
#  
#  replace_value(col='finalidades', curr_value='SERVICIOS DE LA DEUDA PÚBLICA', new_value='Servicios de la deuda pública')
#  RangeIndex: 1188 entries, 0 to 1187
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       1188 non-null   int64  
#   1   codigo                     1188 non-null   object 
#   2   finalidades                1188 non-null   object 
#   3   funciones                  1144 non-null   object 
#   4   subfunciones               660 non-null    object 
#   5   gasto_publico_consolidado  1188 non-null   float64
#   6   categoria                  1188 non-null   object 
#  
#  |    |   anio | codigo   | finalidades               | funciones              | subfunciones   |   gasto_publico_consolidado | categoria              |
#  |---:|-------:|:---------|:--------------------------|:-----------------------|:---------------|----------------------------:|:-----------------------|
#  |  0 |   1980 | 1.1.1    | FUNCIONAMIENTO DEL ESTADO | Administración general |                |                     1.84962 | Administración general |
#  
#  ------------------------------
#  
#  custom_logic()
#  RangeIndex: 1188 entries, 0 to 1187
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       1188 non-null   int64  
#   1   codigo                     1188 non-null   object 
#   2   finalidades                1188 non-null   object 
#   3   funciones                  1144 non-null   object 
#   4   subfunciones               660 non-null    object 
#   5   gasto_publico_consolidado  1188 non-null   float64
#   6   categoria                  1188 non-null   object 
#  
#  |    |   anio | codigo   | finalidades               | funciones              | subfunciones   |   gasto_publico_consolidado | categoria              |
#  |---:|-------:|:---------|:--------------------------|:-----------------------|:---------------|----------------------------:|:-----------------------|
#  |  0 |   1980 | 1.1.1    | FUNCIONAMIENTO DEL ESTADO | Administración general |                |                     1.84962 | Administración general |
#  
#  ------------------------------
#  