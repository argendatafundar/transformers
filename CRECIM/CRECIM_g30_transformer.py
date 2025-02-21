from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='pib_per_capita', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             7831 non-null   int64  
#   1   iso3             7831 non-null   object 
#   2   pib_per_capita   7831 non-null   float64
#   3   cambio_relativo  7831 non-null   float64
#  
#  |    |   anio | iso3   |   pib_per_capita |   cambio_relativo |
#  |---:|-------:|:-------|-----------------:|------------------:|
#  |  0 |   1820 | ARG    |             1591 |                 0 |
#  
#  ------------------------------
#  
#  drop_col(col='pib_per_capita', axis=1)
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             7831 non-null   int64  
#   1   iso3             7831 non-null   object 
#   2   cambio_relativo  7831 non-null   float64
#  
#  |    |   anio | iso3   |   cambio_relativo |
#  |---:|-------:|:-------|------------------:|
#  |  0 |   1820 | ARG    |                 0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'})
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       7831 non-null   int64  
#   1   geocodigo  7831 non-null   object 
#   2   valor      7831 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1820 | ARG         |       0 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       7831 non-null   int64  
#   1   geocodigo  7831 non-null   object 
#   2   valor      7831 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1820 | ARG         |       0 |
#  
#  ------------------------------
#  