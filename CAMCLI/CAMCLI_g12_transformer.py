from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'subsector': 'indicador', 'valor_en_mtco2e': 'valor'}),
	drop_col(col='sector', axis=1)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             132 non-null    int64  
#   1   sector           132 non-null    object 
#   2   subsector        132 non-null    object 
#   3   valor_en_mtco2e  132 non-null    float64
#  
#  |    |   anio | sector                                   | subsector                |   valor_en_mtco2e |
#  |---:|-------:|:-----------------------------------------|:-------------------------|------------------:|
#  |  0 |   1990 | Procesos industriales y uso de productos | Industria de los metales |              4.95 |
#  
#  ------------------------------
#  
#  rename_cols(map={'subsector': 'indicador', 'valor_en_mtco2e': 'valor'})
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       132 non-null    int64  
#   1   sector     132 non-null    object 
#   2   indicador  132 non-null    object 
#   3   valor      132 non-null    float64
#  
#  |    |   anio | sector                                   | indicador                |   valor |
#  |---:|-------:|:-----------------------------------------|:-------------------------|--------:|
#  |  0 |   1990 | Procesos industriales y uso de productos | Industria de los metales |    4.95 |
#  
#  ------------------------------
#  
#  drop_col(col='sector', axis=1)
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       132 non-null    int64  
#   1   indicador  132 non-null    object 
#   2   valor      132 non-null    float64
#  
#  |    |   anio | indicador                |   valor |
#  |---:|-------:|:-------------------------|--------:|
#  |  0 |   1990 | Industria de los metales |    4.95 |
#  
#  ------------------------------
#  