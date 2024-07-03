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
rename_cols(map={'valor_en_mtco2e': 'valor', 'subsector': 'indicador'}),
	drop_col(col='sector', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             145 non-null    int64  
#   1   sector           145 non-null    object 
#   2   subsector        145 non-null    object 
#   3   valor_en_mtco2e  145 non-null    float64
#  
#  |    |   anio | sector   | subsector                                                          |   valor_en_mtco2e |
#  |---:|-------:|:---------|:-------------------------------------------------------------------|------------------:|
#  |  0 |   1990 | Energía  | Emisiones fugitivas provenientes de la fabricación de combustibles |              6.44 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_mtco2e': 'valor', 'subsector': 'indicador'})
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   sector     145 non-null    object 
#   2   indicador  145 non-null    object 
#   3   valor      145 non-null    float64
#  
#  |    |   anio | sector   | indicador                                                          |   valor |
#  |---:|-------:|:---------|:-------------------------------------------------------------------|--------:|
#  |  0 |   1990 | Energía  | Emisiones fugitivas provenientes de la fabricación de combustibles |    6.44 |
#  
#  ------------------------------
#  
#  drop_col(col='sector', axis=1)
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      145 non-null    float64
#  
#  |    |   anio | indicador                                                          |   valor |
#  |---:|-------:|:-------------------------------------------------------------------|--------:|
#  |  0 |   1990 | Emisiones fugitivas provenientes de la fabricación de combustibles |    6.44 |
#  
#  ------------------------------
#  