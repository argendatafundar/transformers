from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def ordenar_categorica(df, col1:str, order1:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    return df.sort_values(by=[col1])

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'grupo_carne': 'indicador'}),
	replace_value(col='indicador', curr_value='aviar', new_value='Aviar'),
	replace_value(col='indicador', curr_value='vacuna', new_value='Vacuna'),
	replace_value(col='indicador', curr_value='caprina_ovina', new_value='Caprina y ovina'),
	replace_value(col='indicador', curr_value='porcina', new_value='Porcina'),
	replace_value(col='indicador', curr_value='otras_carnes', new_value='Otras carnes'),
	replace_value(col='indicador', curr_value='pescado_mariscos', new_value='Pescados y mariscos'),
	ordenar_categorica(col1='indicador', order1=['Vacuna', 'Aviar', 'Porcina', 'Pescados y mariscos', 'Caprina y ovina', 'Otras carnes']),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         360 non-null    int64  
#   1   grupo_carne  360 non-null    object 
#   2   valor        360 non-null    float64
#  
#  |    |   anio | grupo_carne   |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1961 | aviar         | 2.07759 |
#  
#  ------------------------------
#  
#  rename_cols(map={'grupo_carne': 'indicador'})
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       360 non-null    int64  
#   1   indicador  360 non-null    object 
#   2   valor      360 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | aviar       | 2.07759 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='aviar', new_value='Aviar')
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       360 non-null    int64  
#   1   indicador  360 non-null    object 
#   2   valor      360 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | Aviar       | 2.07759 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='vacuna', new_value='Vacuna')
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       360 non-null    int64  
#   1   indicador  360 non-null    object 
#   2   valor      360 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | Aviar       | 2.07759 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='caprina_ovina', new_value='Caprina y ovina')
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       360 non-null    int64  
#   1   indicador  360 non-null    object 
#   2   valor      360 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | Aviar       | 2.07759 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='porcina', new_value='Porcina')
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       360 non-null    int64  
#   1   indicador  360 non-null    object 
#   2   valor      360 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | Aviar       | 2.07759 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='otras_carnes', new_value='Otras carnes')
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       360 non-null    int64  
#   1   indicador  360 non-null    object 
#   2   valor      360 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | Aviar       | 2.07759 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='pescado_mariscos', new_value='Pescados y mariscos')
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       360 non-null    int64   
#   1   indicador  360 non-null    category
#   2   valor      360 non-null    float64 
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | Aviar       | 2.07759 |
#  
#  ------------------------------
#  
#  ordenar_categorica(col1='indicador', order1=['Vacuna', 'Aviar', 'Porcina', 'Pescados y mariscos', 'Caprina y ovina', 'Otras carnes'])
#  Index: 360 entries, 118 to 299
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       360 non-null    int64   
#   1   indicador  360 non-null    category
#   2   valor      360 non-null    float64 
#  
#  |     |   anio | indicador   |   valor |
#  |----:|-------:|:------------|--------:|
#  | 118 |   2019 | Vacuna      | 48.0177 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       360 non-null    int64   
#   1   indicador  360 non-null    category
#   2   valor      360 non-null    float64 
#  
#  |    |   anio | indicador    |    valor |
#  |---:|-------:|:-------------|---------:|
#  |  0 |   1961 | Otras carnes | 0.912113 |
#  
#  ------------------------------
#  