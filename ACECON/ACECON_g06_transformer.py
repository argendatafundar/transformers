from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def abs_col(df:DataFrame, col:str, new_col:str):
    df[new_col] = df[col].apply(lambda x: abs(x))
    return df

@transformer.convert
def rescale(df:DataFrame, group_cols:list[str], summarised_col:str, new_col:str) -> DataFrame:
    df[new_col] = df.groupby(group_cols)[summarised_col].transform(
    lambda x: 100*(x/x.sum()))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	abs_col(col='valor', new_col='valor_abs'),
	rescale(group_cols=['anio'], summarised_col='valor_abs', new_col='valor_abs_scaled')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 528 entries, 0 to 527
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         528 non-null    int64  
#   1   comp_dem_ag  528 non-null    object 
#   2   valor        528 non-null    float64
#   3   valor_abs    528 non-null    float64
#  
#  |    |   anio | comp_dem_ag     |   valor |   valor_abs |
#  |---:|-------:|:----------------|--------:|------------:|
#  |  0 |   1935 | Consumo hogares | 75.7468 |     75.7468 |
#  
#  ------------------------------
#  
#  abs_col(col='valor', new_col='valor_abs')
#  RangeIndex: 528 entries, 0 to 527
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              528 non-null    int64  
#   1   comp_dem_ag       528 non-null    object 
#   2   valor             528 non-null    float64
#   3   valor_abs         528 non-null    float64
#   4   valor_abs_scaled  528 non-null    float64
#  
#  |    |   anio | comp_dem_ag     |   valor |   valor_abs |   valor_abs_scaled |
#  |---:|-------:|:----------------|--------:|------------:|-------------------:|
#  |  0 |   1935 | Consumo hogares | 75.7468 |     75.7468 |            61.5439 |
#  
#  ------------------------------
#  
#  rescale(group_cols=['anio'], summarised_col='valor_abs', new_col='valor_abs_scaled')
#  RangeIndex: 528 entries, 0 to 527
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              528 non-null    int64  
#   1   comp_dem_ag       528 non-null    object 
#   2   valor             528 non-null    float64
#   3   valor_abs         528 non-null    float64
#   4   valor_abs_scaled  528 non-null    float64
#  
#  |    |   anio | comp_dem_ag     |   valor |   valor_abs |   valor_abs_scaled |
#  |---:|-------:|:----------------|--------:|------------:|-------------------:|
#  |  0 |   1935 | Consumo hogares | 75.7468 |     75.7468 |            61.5439 |
#  
#  ------------------------------
#  