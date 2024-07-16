from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df

@transformer.convert
def str_to_title(df: DataFrame, col:str):
    df[col] = df[col].str.title()
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='concepto != "total"'),
	rename_cols(map={'destino': 'nivel1', 'concepto': 'nivel2', 'porcentaje_total': 'valor'}),
	replace_values(col='nivel1', values={'local': 'Gastos locales', 'gastos a no residentes': 'Gastos a no residentes'}),
	str_to_title(col='nivel2'),
	replace_values(col='nivel2', values={'Consumo Intermedio Nacional (Neto De Importaciones Indirectas)': 'Consumo intermedio nacional', 'Contribuciones A La Seguridad Social': 'Contribuciones a la seguridad social', 'Impuesto A Las Ganancias': 'Impuesto a las ganancias', 'Amortizaciones Nacionales (Neto De Importaciones Indirectas)': 'Amortizaciones nacionales', 'Regalías Y Fideicomisos': 'Regalías y fideicomisos', 'Impuestos A La Producción (Netos De Subsidios)': 'Impuestos a la producción', 'Consumo Intermedio Importado': 'Consumo intermedio importado', 'Contenido Importado En Consumo Intermedio Nacional': 'Contenido importado en consumo intermedio nacional', 'Amortizaciones Importadas': 'Amortizaciones importadas', 'Contenido Importado En Amortizaciones Nacionales': 'Contenido importado en amortizaciones nacionales', 'Ingreso Neto Disponible': 'Ingreso neto disponible'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   destino           16 non-null     object 
#   1   concepto          16 non-null     object 
#   2   porcentaje_total  16 non-null     float64
#  
#  |    | destino   | concepto                                                       |   porcentaje_total |
#  |---:|:----------|:---------------------------------------------------------------|-------------------:|
#  |  0 | local     | consumo intermedio nacional (neto de importaciones indirectas) |               29.8 |
#  
#  ------------------------------
#  
#  query(condition='concepto != "total"')
#  Index: 14 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   destino           14 non-null     object 
#   1   concepto          14 non-null     object 
#   2   porcentaje_total  14 non-null     float64
#  
#  |    | destino   | concepto                                                       |   porcentaje_total |
#  |---:|:----------|:---------------------------------------------------------------|-------------------:|
#  |  0 | local     | consumo intermedio nacional (neto de importaciones indirectas) |               29.8 |
#  
#  ------------------------------
#  
#  rename_cols(map={'destino': 'nivel1', 'concepto': 'nivel2', 'porcentaje_total': 'valor'})
#  Index: 14 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  14 non-null     object 
#   1   nivel2  14 non-null     object 
#   2   valor   14 non-null     float64
#  
#  |    | nivel1   | nivel2                                                         |   valor |
#  |---:|:---------|:---------------------------------------------------------------|--------:|
#  |  0 | local    | consumo intermedio nacional (neto de importaciones indirectas) |    29.8 |
#  
#  ------------------------------
#  
#  replace_values(col='nivel1', values={'local': 'Gastos locales', 'gastos a no residentes': 'Gastos a no residentes'})
#  Index: 14 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  14 non-null     object 
#   1   nivel2  14 non-null     object 
#   2   valor   14 non-null     float64
#  
#  |    | nivel1         | nivel2                                                         |   valor |
#  |---:|:---------------|:---------------------------------------------------------------|--------:|
#  |  0 | Gastos locales | Consumo Intermedio Nacional (Neto De Importaciones Indirectas) |    29.8 |
#  
#  ------------------------------
#  
#  str_to_title(col='nivel2')
#  Index: 14 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  14 non-null     object 
#   1   nivel2  14 non-null     object 
#   2   valor   14 non-null     float64
#  
#  |    | nivel1         | nivel2                                                         |   valor |
#  |---:|:---------------|:---------------------------------------------------------------|--------:|
#  |  0 | Gastos locales | Consumo Intermedio Nacional (Neto De Importaciones Indirectas) |    29.8 |
#  
#  ------------------------------
#  
#  replace_values(col='nivel2', values={'Consumo Intermedio Nacional (Neto De Importaciones Indirectas)': 'Consumo intermedio nacional', 'Contribuciones A La Seguridad Social': 'Contribuciones a la seguridad social', 'Impuesto A Las Ganancias': 'Impuesto a las ganancias', 'Amortizaciones Nacionales (Neto De Importaciones Indirectas)': 'Amortizaciones nacionales', 'Regalías Y Fideicomisos': 'Regalías y fideicomisos', 'Impuestos A La Producción (Netos De Subsidios)': 'Impuestos a la producción', 'Consumo Intermedio Importado': 'Consumo intermedio importado', 'Contenido Importado En Consumo Intermedio Nacional': 'Contenido importado en consumo intermedio nacional', 'Amortizaciones Importadas': 'Amortizaciones importadas', 'Contenido Importado En Amortizaciones Nacionales': 'Contenido importado en amortizaciones nacionales', 'Ingreso Neto Disponible': 'Ingreso neto disponible'})
#  Index: 14 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  14 non-null     object 
#   1   nivel2  14 non-null     object 
#   2   valor   14 non-null     float64
#  
#  |    | nivel1         | nivel2                      |   valor |
#  |---:|:---------------|:----------------------------|--------:|
#  |  0 | Gastos locales | Consumo intermedio nacional |    29.8 |
#  
#  ------------------------------
#  