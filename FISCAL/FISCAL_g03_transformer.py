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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def filtrar_por_max_anio(df, group_cols):
    idx = df.groupby(group_cols)['anio'].transform('max') == df['anio']
    return df[idx]

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col='geocodigoFundar', axis=1),
	filtrar_por_max_anio(group_cols=['geonombreFundar', 'ingreso']),
	replace_multiple_values(col='ingreso', replacements={'Ingresos de mercado más pensiones': 'Pre-intervención', 'Ingreso final': 'Post-intervención'}),
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Costa de Marfil', 'Tayikistán', 'Comoras', 'Etiopía', 'Jordania', 'Indonesia', 'Sri Lanka', 'Togo', 'Burkina Faso', 'Ghana', 'Uganda', 'Armenia', 'Tanzanía', 'Egipto', 'Paraguay', 'Guatemala', 'Albania', 'Nicaragua', 'India', 'Belarús', 'Irán', 'Bolivia', 'El Salvador', 'Perú', 'Rumania', 'Honduras', 'Chile', 'Rusia', 'Ecuador', 'Ucrania', 'Kenia', 'Túnez', 'Rep. Dominicana', 'Croacia', 'Botswana', 'Zambia', 'Colombia', 'Turquía', 'Costa Rica', 'Venezuela', 'Lesotho', 'Polonia', 'Panamá', 'Uruguay', 'Namibia', 'México', 'Eswatini', 'Brasil', 'Estados Unidos', 'Argentina', 'España', 'Sudáfrica'], col2='ingreso', order2=['Pre-intervención', 'Post-intervención']),
	query(condition="geonombreFundar.isin(['Costa de Marfil', 'Tayikistán', 'Sudáfrica', 'España', 'Argentina', 'Ucrania', 'Bielorrusia', 'Croacia', 'Estados Unidos', 'México', 'Uruguay', 'Costa Rica', 'Colombia', 'Brasil', 'Rusia', 'Chile', 'Indonesia', 'Turquía'])")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 124 entries, 0 to 123
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  124 non-null    object 
#   1   geonombreFundar  124 non-null    object 
#   2   anio             124 non-null    int64  
#   3   ingreso          124 non-null    object 
#   4   gini             124 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | ingreso                           |   gini |
#  |---:|:------------------|:------------------|-------:|:----------------------------------|-------:|
#  |  0 | ALB               | Albania           |   2015 | Ingresos de mercado más pensiones | 0.3693 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  RangeIndex: 124 entries, 0 to 123
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  124 non-null    object 
#   1   anio             124 non-null    int64  
#   2   ingreso          124 non-null    object 
#   3   gini             124 non-null    float64
#  
#  |    | geonombreFundar   |   anio | ingreso                           |   gini |
#  |---:|:------------------|-------:|:----------------------------------|-------:|
#  |  0 | Albania           |   2015 | Ingresos de mercado más pensiones | 0.3693 |
#  
#  ------------------------------
#  
#  filtrar_por_max_anio(group_cols=['geonombreFundar', 'ingreso'])
#  Index: 104 entries, 0 to 123
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  104 non-null    category
#   1   anio             104 non-null    int64   
#   2   ingreso          104 non-null    category
#   3   gini             104 non-null    float64 
#  
#  |    | geonombreFundar   |   anio | ingreso          |   gini |
#  |---:|:------------------|-------:|:-----------------|-------:|
#  |  0 | Albania           |   2015 | Pre-intervención | 0.3693 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='ingreso', replacements={'Ingresos de mercado más pensiones': 'Pre-intervención', 'Ingreso final': 'Post-intervención'})
#  Index: 104 entries, 0 to 123
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  104 non-null    category
#   1   anio             104 non-null    int64   
#   2   ingreso          104 non-null    category
#   3   gini             104 non-null    float64 
#  
#  |    | geonombreFundar   |   anio | ingreso          |   gini |
#  |---:|:------------------|-------:|:-----------------|-------:|
#  |  0 | Albania           |   2015 | Pre-intervención | 0.3693 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Costa de Marfil', 'Tayikistán', 'Comoras', 'Etiopía', 'Jordania', 'Indonesia', 'Sri Lanka', 'Togo', 'Burkina Faso', 'Ghana', 'Uganda', 'Armenia', 'Tanzanía', 'Egipto', 'Paraguay', 'Guatemala', 'Albania', 'Nicaragua', 'India', 'Belarús', 'Irán', 'Bolivia', 'El Salvador', 'Perú', 'Rumania', 'Honduras', 'Chile', 'Rusia', 'Ecuador', 'Ucrania', 'Kenia', 'Túnez', 'Rep. Dominicana', 'Croacia', 'Botswana', 'Zambia', 'Colombia', 'Turquía', 'Costa Rica', 'Venezuela', 'Lesotho', 'Polonia', 'Panamá', 'Uruguay', 'Namibia', 'México', 'Eswatini', 'Brasil', 'Estados Unidos', 'Argentina', 'España', 'Sudáfrica'], col2='ingreso', order2=['Pre-intervención', 'Post-intervención'])
#  Index: 104 entries, 20 to 121
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  104 non-null    category
#   1   anio             104 non-null    int64   
#   2   ingreso          104 non-null    category
#   3   gini             104 non-null    float64 
#  
#  |    | geonombreFundar   |   anio | ingreso          |     gini |
#  |---:|:------------------|-------:|:-----------------|---------:|
#  | 20 | Costa de Marfil   |   2015 | Pre-intervención | 0.404751 |
#  
#  ------------------------------
#  
#  query(condition="geonombreFundar.isin(['Costa de Marfil', 'Tayikistán', 'Sudáfrica', 'España', 'Argentina', 'Ucrania', 'Bielorrusia', 'Croacia', 'Estados Unidos', 'México', 'Uruguay', 'Costa Rica', 'Colombia', 'Brasil', 'Rusia', 'Chile', 'Indonesia', 'Turquía'])")
#  Index: 34 entries, 20 to 121
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  34 non-null     category
#   1   anio             34 non-null     int64   
#   2   ingreso          34 non-null     category
#   3   gini             34 non-null     float64 
#  
#  |    | geonombreFundar   |   anio | ingreso          |     gini |
#  |---:|:------------------|-------:|:-----------------|---------:|
#  | 20 | Costa de Marfil   |   2015 | Pre-intervención | 0.404751 |
#  
#  ------------------------------
#  