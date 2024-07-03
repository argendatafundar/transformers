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

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'provincia': 'geocodigo', 'sector': 'indicador', 'valor_en_mtco2e': 'valor'}),
	drop_col(col='anio', axis=1),
	replace_value(col='geocodigo', curr_value='Ciudad Autónoma de Buenos Aires', new_value='AR-C'),
	replace_value(col='geocodigo', curr_value='Buenos Aires', new_value='AR-B'),
	replace_value(col='geocodigo', curr_value='Catamarca', new_value='AR-K'),
	replace_value(col='geocodigo', curr_value='Cordoba', new_value='AR-X'),
	replace_value(col='geocodigo', curr_value='Corrientes', new_value='AR-W'),
	replace_value(col='geocodigo', curr_value='Chaco', new_value='AR-H'),
	replace_value(col='geocodigo', curr_value='Chubut', new_value='AR-U'),
	replace_value(col='geocodigo', curr_value='Entre Rios', new_value='AR-E'),
	replace_value(col='geocodigo', curr_value='Formosa', new_value='AR-P'),
	replace_value(col='geocodigo', curr_value='Jujuy', new_value='AR-Y'),
	replace_value(col='geocodigo', curr_value='La Pampa', new_value='AR-L'),
	replace_value(col='geocodigo', curr_value='La Rioja', new_value='AR-F'),
	replace_value(col='geocodigo', curr_value='Mendoza', new_value='AR-M'),
	replace_value(col='geocodigo', curr_value='Misiones', new_value='AR-N'),
	replace_value(col='geocodigo', curr_value='Neuquen', new_value='AR-Q'),
	replace_value(col='geocodigo', curr_value='Rio Negro', new_value='AR-R'),
	replace_value(col='geocodigo', curr_value='Salta', new_value='AR-A'),
	replace_value(col='geocodigo', curr_value='San Juan', new_value='AR-J'),
	replace_value(col='geocodigo', curr_value='San Luis', new_value='AR-D'),
	replace_value(col='geocodigo', curr_value='Santa Cruz', new_value='AR-Z'),
	replace_value(col='geocodigo', curr_value='Santa Fe', new_value='AR-S'),
	replace_value(col='geocodigo', curr_value='Santiago del Estero', new_value='AR-G'),
	replace_value(col='geocodigo', curr_value='Tucumán', new_value='AR-T'),
	replace_value(col='geocodigo', curr_value='Tierra del Fuego', new_value='AR-V')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             864 non-null    int64  
#   1   provincia        864 non-null    object 
#   2   sector           864 non-null    object 
#   3   valor_en_mtco2e  864 non-null    float64
#  
#  |    |   anio | provincia                       | sector   |   valor_en_mtco2e |
#  |---:|-------:|:--------------------------------|:---------|------------------:|
#  |  0 |   2010 | Ciudad Autónoma de Buenos Aires | Energía  |             17.88 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'geocodigo', 'sector': 'indicador', 'valor_en_mtco2e': 'valor'})
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       864 non-null    int64  
#   1   geocodigo  864 non-null    object 
#   2   indicador  864 non-null    object 
#   3   valor      864 non-null    float64
#  
#  |    |   anio | geocodigo                       | indicador   |   valor |
#  |---:|-------:|:--------------------------------|:------------|--------:|
#  |  0 |   2010 | Ciudad Autónoma de Buenos Aires | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo                       | indicador   |   valor |
#  |---:|:--------------------------------|:------------|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Ciudad Autónoma de Buenos Aires', new_value='AR-C')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Buenos Aires', new_value='AR-B')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Catamarca', new_value='AR-K')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Cordoba', new_value='AR-X')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Corrientes', new_value='AR-W')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Chaco', new_value='AR-H')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Chubut', new_value='AR-U')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Entre Rios', new_value='AR-E')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Formosa', new_value='AR-P')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Jujuy', new_value='AR-Y')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='La Pampa', new_value='AR-L')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='La Rioja', new_value='AR-F')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Mendoza', new_value='AR-M')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Misiones', new_value='AR-N')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Neuquen', new_value='AR-Q')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Rio Negro', new_value='AR-R')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Salta', new_value='AR-A')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='San Juan', new_value='AR-J')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='San Luis', new_value='AR-D')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Santa Cruz', new_value='AR-Z')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Santa Fe', new_value='AR-S')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Santiago del Estero', new_value='AR-G')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Tucumán', new_value='AR-T')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='Tierra del Fuego', new_value='AR-V')
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | Energía     |   17.88 |
#  
#  ------------------------------
#  