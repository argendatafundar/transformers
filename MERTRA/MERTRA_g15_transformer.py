from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def round_to(df, col, n):
    from decimal import Decimal, ROUND_HALF_UP

    def round_to_n_digits(number: float, n: int) -> float:
        decimal_number = Decimal(str(number))
        rounding_format = Decimal('1.' + '0' * n)
        rounded_number = decimal_number.quantize(rounding_format, rounding=ROUND_HALF_UP)
        return float(rounded_number)

    def round_to(n):
        return lambda x: round_to_n_digits(x,n)
    
    df[col] = df[col].map(round_to(n))

    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
round_to(col='valor', n=2),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_wvs_code  12 non-null     int64  
#   1   region_wvs       12 non-null     object 
#   2   nivel_acuerdo    12 non-null     object 
#   3   valor            12 non-null     float64
#  
#  |    |   region_wvs_code | region_wvs                     | nivel_acuerdo   |    valor |
#  |---:|------------------:|:-------------------------------|:----------------|---------:|
#  |  0 |                 1 | Centro y Sur (exc. CABA y PBA) | De acuerdo      | 0.128527 |
#  
#  ------------------------------
#  
#  round_to(col='valor', n=2)
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_wvs_code  12 non-null     int64  
#   1   region_wvs       12 non-null     object 
#   2   nivel_acuerdo    12 non-null     object 
#   3   valor            12 non-null     float64
#  
#  |    |   region_wvs_code | region_wvs                     | nivel_acuerdo   |   valor |
#  |---:|------------------:|:-------------------------------|:----------------|--------:|
#  |  0 |                 1 | Centro y Sur (exc. CABA y PBA) | De acuerdo      |      13 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_wvs_code  12 non-null     int64  
#   1   region_wvs       12 non-null     object 
#   2   nivel_acuerdo    12 non-null     object 
#   3   valor            12 non-null     float64
#  
#  |    |   region_wvs_code | region_wvs                     | nivel_acuerdo   |   valor |
#  |---:|------------------:|:-------------------------------|:----------------|--------:|
#  |  0 |                 1 | Centro y Sur (exc. CABA y PBA) | De acuerdo      |      13 |
#  
#  ------------------------------
#  