from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

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
def agg_sum(df: DataFrame, key_cols:list[str], summarised_col:str) -> DataFrame:
    return df.groupby(key_cols)[summarised_col].sum().reset_index()
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='finalidades', curr_value='SERVICIOS DE LA DEUDA PÚBLICA', new_value='Servicios de la deuda pública'),
	custom_logic(),
	query(condition="categoria != 'Educación superior y universitaria'"),
	replace_multiple_values(col='categoria', replacements={'Administración general': 'Adm. general', 'Justicia': 'Justicia', 'Defensa y seguridad': 'Defensa y seguridad', 'Educación básica': 'Educación básica', 'Educación superior y universitaria': 'Educación superior', 'Ciencia y técnica': 'Ciencia y técnica', 'Cultura': 'Cultura', 'Educación y cultura sin discriminar': 'Educación y cultura', 'Atención pública de la salud': 'Salud pública', 'Obras sociales - Atención de la salud': 'Obras sociales', 'INSSJyP - Atención de la salud': 'PAMI', 'Agua potable y alcantarillado': 'Agua y alcantarillado', 'Vivienda y urbanismo': 'Vivienda y urbanismo', 'Promoción y asistencia social pública': 'Asistencia social', 'Obras sociales - Prestaciones sociales': 'Obras sociales', 'INSSJyP - Prestaciones sociales': 'PAMI', 'Previsión social': 'Previsión social', 'Programas de empleo y seguro de desempleo': 'Empleo', 'Asignaciones familiares': 'Asignaciones familiares', 'Otros servicios urbanos': 'Servicios urbanos', 'Producción primaria': 'Producción primaria', 'Energía y combustible': 'Energía y combustible', 'Industria': 'Industria', 'Transporte': 'Transporte', 'Comunicaciones': 'Comunicaciones', 'Otros gastos en servicios económicos': 'Servicios económicos', 'Servicios de la deuda pública': 'Deuda pública'}),
	agg_sum(key_cols=['anio', 'categoria'], summarised_col='gasto_publico_consolidado')
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
#  query(condition="categoria != 'Educación superior y universitaria'")
#  Index: 1144 entries, 0 to 1187
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       1144 non-null   int64  
#   1   codigo                     1144 non-null   object 
#   2   finalidades                1144 non-null   object 
#   3   funciones                  1100 non-null   object 
#   4   subfunciones               616 non-null    object 
#   5   gasto_publico_consolidado  1144 non-null   float64
#   6   categoria                  1144 non-null   object 
#  
#  |    |   anio | codigo   | finalidades               | funciones              | subfunciones   |   gasto_publico_consolidado | categoria    |
#  |---:|-------:|:---------|:--------------------------|:-----------------------|:---------------|----------------------------:|:-------------|
#  |  0 |   1980 | 1.1.1    | FUNCIONAMIENTO DEL ESTADO | Administración general |                |                     1.84962 | Adm. general |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='categoria', replacements={'Administración general': 'Adm. general', 'Justicia': 'Justicia', 'Defensa y seguridad': 'Defensa y seguridad', 'Educación básica': 'Educación básica', 'Educación superior y universitaria': 'Educación superior', 'Ciencia y técnica': 'Ciencia y técnica', 'Cultura': 'Cultura', 'Educación y cultura sin discriminar': 'Educación y cultura', 'Atención pública de la salud': 'Salud pública', 'Obras sociales - Atención de la salud': 'Obras sociales', 'INSSJyP - Atención de la salud': 'PAMI', 'Agua potable y alcantarillado': 'Agua y alcantarillado', 'Vivienda y urbanismo': 'Vivienda y urbanismo', 'Promoción y asistencia social pública': 'Asistencia social', 'Obras sociales - Prestaciones sociales': 'Obras sociales', 'INSSJyP - Prestaciones sociales': 'PAMI', 'Previsión social': 'Previsión social', 'Programas de empleo y seguro de desempleo': 'Empleo', 'Asignaciones familiares': 'Asignaciones familiares', 'Otros servicios urbanos': 'Servicios urbanos', 'Producción primaria': 'Producción primaria', 'Energía y combustible': 'Energía y combustible', 'Industria': 'Industria', 'Transporte': 'Transporte', 'Comunicaciones': 'Comunicaciones', 'Otros gastos en servicios económicos': 'Servicios económicos', 'Servicios de la deuda pública': 'Deuda pública'})
#  Index: 1144 entries, 0 to 1187
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       1144 non-null   int64  
#   1   codigo                     1144 non-null   object 
#   2   finalidades                1144 non-null   object 
#   3   funciones                  1100 non-null   object 
#   4   subfunciones               616 non-null    object 
#   5   gasto_publico_consolidado  1144 non-null   float64
#   6   categoria                  1144 non-null   object 
#  
#  |    |   anio | codigo   | finalidades               | funciones              | subfunciones   |   gasto_publico_consolidado | categoria    |
#  |---:|-------:|:---------|:--------------------------|:-----------------------|:---------------|----------------------------:|:-------------|
#  |  0 |   1980 | 1.1.1    | FUNCIONAMIENTO DEL ESTADO | Administración general |                |                     1.84962 | Adm. general |
#  
#  ------------------------------
#  
#  agg_sum(key_cols=['anio', 'categoria'], summarised_col='gasto_publico_consolidado')
#  RangeIndex: 1056 entries, 0 to 1055
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       1056 non-null   int64  
#   1   categoria                  1056 non-null   object 
#   2   gasto_publico_consolidado  1056 non-null   float64
#  
#  |    |   anio | categoria    |   gasto_publico_consolidado |
#  |---:|-------:|:-------------|----------------------------:|
#  |  0 |   1980 | Adm. general |                     1.84962 |
#  
#  ------------------------------
#  