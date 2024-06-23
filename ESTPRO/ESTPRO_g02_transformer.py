from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tipo_sector': 'nivel1', 'letra_desc': 'indicador', 'vab': 'valor'}),
	drop_col(col=['letra'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1476 entries, 0 to 1475
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        1476 non-null   int64  
#   1   letra_desc  1476 non-null   object 
#   2   vab         1476 non-null   float64
#   3   letra       1476 non-null   object 
#  
#  |    |   anio | letra_desc   |     vab | letra   |
#  |---:|-------:|:-------------|--------:|:--------|
#  |  0 |   1900 | Agro         | 3830.61 | A       |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_sector': 'nivel1', 'letra_desc': 'indicador', 'vab': 'valor'})
#  RangeIndex: 1476 entries, 0 to 1475
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1476 non-null   int64  
#   1   indicador  1476 non-null   object 
#   2   valor      1476 non-null   float64
#   3   letra      1476 non-null   object 
#  
#  |    |   anio | indicador   |   valor | letra   |
#  |---:|-------:|:------------|--------:|:--------|
#  |  0 |   1900 | Agro        | 3830.61 | A       |
#  
#  ------------------------------
#  
#  drop_col(col=['letra'], axis=1)
#  RangeIndex: 1476 entries, 0 to 1475
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1476 non-null   int64  
#   1   indicador  1476 non-null   object 
#   2   valor      1476 non-null   float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1900 | Agro        | 3830.61 |
#  
#  ------------------------------
#  