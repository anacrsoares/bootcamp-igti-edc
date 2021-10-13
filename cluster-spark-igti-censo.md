```pyspark
import pyspark
```


```pyspark
spark
```


    VBox()


    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>1</td><td>application_1634148169901_0003</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="http://ip-172-31-23-166.us-east-2.compute.internal:20888/proxy/application_1634148169901_0003/">Link</a></td><td><a target="_blank" href="http://ip-172-31-27-234.us-east-2.compute.internal:8042/node/containerlogs/container_1634148169901_0003_01_000001/livy">Link</a></td><td>✔</td></tr></table>



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    SparkSession available as 'spark'.



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    <pyspark.sql.session.SparkSession object at 0x7f2b233ea590>


```pyspark
matriculas = (
                spark
                .read
                .format('csv')
                .option('header', True)
                .option('inferSchema', True)
                .option('delimiter', '|')
                .load('s3://datalake-anacrsoares-688125619919/raw-data/dados-censo-2020/')
                )
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
(
    matriculas
    .write
    .mode('overwrite')
    .format('parquet')
    .save("s3://datalake-anacrsoares-688125619919/staging-zone/dados-censo-2020")
)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
matriculas_pt = (
                 spark
                 .read
                 .format('parquet')
                 .option('inferSchema', True)
                 .load("s3://datalake-anacrsoares-688125619919/staging-zone/dados-censo-2020")
                 )
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
matriculas_pt.printSchema()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    root
     |-- NU_ANO_CENSO: integer (nullable = true)
     |-- ID_ALUNO: string (nullable = true)
     |-- ID_MATRICULA: integer (nullable = true)
     |-- NU_MES: integer (nullable = true)
     |-- NU_ANO: integer (nullable = true)
     |-- NU_IDADE_REFERENCIA: integer (nullable = true)
     |-- NU_IDADE: integer (nullable = true)
     |-- TP_SEXO: integer (nullable = true)
     |-- TP_COR_RACA: integer (nullable = true)
     |-- TP_NACIONALIDADE: integer (nullable = true)
     |-- CO_PAIS_ORIGEM: integer (nullable = true)
     |-- CO_UF_NASC: integer (nullable = true)
     |-- CO_MUNICIPIO_NASC: integer (nullable = true)
     |-- CO_UF_END: integer (nullable = true)
     |-- CO_MUNICIPIO_END: integer (nullable = true)
     |-- TP_ZONA_RESIDENCIAL: integer (nullable = true)
     |-- TP_LOCAL_RESID_DIFERENCIADA: integer (nullable = true)
     |-- IN_NECESSIDADE_ESPECIAL: integer (nullable = true)
     |-- IN_BAIXA_VISAO: integer (nullable = true)
     |-- IN_CEGUEIRA: integer (nullable = true)
     |-- IN_DEF_AUDITIVA: integer (nullable = true)
     |-- IN_DEF_FISICA: integer (nullable = true)
     |-- IN_DEF_INTELECTUAL: integer (nullable = true)
     |-- IN_SURDEZ: integer (nullable = true)
     |-- IN_SURDOCEGUEIRA: integer (nullable = true)
     |-- IN_DEF_MULTIPLA: integer (nullable = true)
     |-- IN_AUTISMO: integer (nullable = true)
     |-- IN_SUPERDOTACAO: integer (nullable = true)
     |-- IN_RECURSO_LEDOR: integer (nullable = true)
     |-- IN_RECURSO_TRANSCRICAO: integer (nullable = true)
     |-- IN_RECURSO_INTERPRETE: integer (nullable = true)
     |-- IN_RECURSO_LIBRAS: integer (nullable = true)
     |-- IN_RECURSO_LABIAL: integer (nullable = true)
     |-- IN_RECURSO_AMPLIADA_18: integer (nullable = true)
     |-- IN_RECURSO_AMPLIADA_24: integer (nullable = true)
     |-- IN_RECURSO_CD_AUDIO: integer (nullable = true)
     |-- IN_RECURSO_PROVA_PORTUGUES: integer (nullable = true)
     |-- IN_RECURSO_VIDEO_LIBRAS: integer (nullable = true)
     |-- IN_RECURSO_BRAILLE: integer (nullable = true)
     |-- IN_RECURSO_NENHUM: integer (nullable = true)
     |-- IN_AEE_LIBRAS: integer (nullable = true)
     |-- IN_AEE_LINGUA_PORTUGUESA: integer (nullable = true)
     |-- IN_AEE_INFORMATICA_ACESSIVEL: integer (nullable = true)
     |-- IN_AEE_BRAILLE: integer (nullable = true)
     |-- IN_AEE_CAA: integer (nullable = true)
     |-- IN_AEE_SOROBAN: integer (nullable = true)
     |-- IN_AEE_VIDA_AUTONOMA: integer (nullable = true)
     |-- IN_AEE_OPTICOS_NAO_OPTICOS: integer (nullable = true)
     |-- IN_AEE_ENRIQ_CURRICULAR: integer (nullable = true)
     |-- IN_AEE_DESEN_COGNITIVO: integer (nullable = true)
     |-- IN_AEE_MOBILIDADE: integer (nullable = true)
     |-- TP_OUTRO_LOCAL_AULA: integer (nullable = true)
     |-- IN_TRANSPORTE_PUBLICO: integer (nullable = true)
     |-- TP_RESPONSAVEL_TRANSPORTE: integer (nullable = true)
     |-- IN_TRANSP_BICICLETA: integer (nullable = true)
     |-- IN_TRANSP_MICRO_ONIBUS: integer (nullable = true)
     |-- IN_TRANSP_ONIBUS: integer (nullable = true)
     |-- IN_TRANSP_TR_ANIMAL: integer (nullable = true)
     |-- IN_TRANSP_VANS_KOMBI: integer (nullable = true)
     |-- IN_TRANSP_OUTRO_VEICULO: integer (nullable = true)
     |-- IN_TRANSP_EMBAR_ATE5: integer (nullable = true)
     |-- IN_TRANSP_EMBAR_5A15: integer (nullable = true)
     |-- IN_TRANSP_EMBAR_15A35: integer (nullable = true)
     |-- IN_TRANSP_EMBAR_35: integer (nullable = true)
     |-- TP_ETAPA_ENSINO: integer (nullable = true)
     |-- IN_ESPECIAL_EXCLUSIVA: integer (nullable = true)
     |-- IN_REGULAR: integer (nullable = true)
     |-- IN_EJA: integer (nullable = true)
     |-- IN_PROFISSIONALIZANTE: integer (nullable = true)
     |-- ID_TURMA: integer (nullable = true)
     |-- CO_CURSO_EDUC_PROFISSIONAL: integer (nullable = true)
     |-- TP_MEDIACAO_DIDATICO_PEDAGO: integer (nullable = true)
     |-- NU_DURACAO_TURMA: integer (nullable = true)
     |-- NU_DUR_ATIV_COMP_MESMA_REDE: integer (nullable = true)
     |-- NU_DUR_ATIV_COMP_OUTRAS_REDES: integer (nullable = true)
     |-- NU_DUR_AEE_MESMA_REDE: integer (nullable = true)
     |-- NU_DUR_AEE_OUTRAS_REDES: integer (nullable = true)
     |-- NU_DIAS_ATIVIDADE: integer (nullable = true)
     |-- TP_UNIFICADA: integer (nullable = true)
     |-- TP_TIPO_ATENDIMENTO_TURMA: integer (nullable = true)
     |-- TP_TIPO_LOCAL_TURMA: integer (nullable = true)
     |-- CO_ENTIDADE: integer (nullable = true)
     |-- CO_REGIAO: integer (nullable = true)
     |-- CO_MESORREGIAO: integer (nullable = true)
     |-- CO_MICRORREGIAO: integer (nullable = true)
     |-- CO_UF: integer (nullable = true)
     |-- CO_MUNICIPIO: integer (nullable = true)
     |-- CO_DISTRITO: integer (nullable = true)
     |-- TP_DEPENDENCIA: integer (nullable = true)
     |-- TP_LOCALIZACAO: integer (nullable = true)
     |-- TP_CATEGORIA_ESCOLA_PRIVADA: integer (nullable = true)
     |-- IN_CONVENIADA_PP: integer (nullable = true)
     |-- TP_CONVENIO_PODER_PUBLICO: integer (nullable = true)
     |-- IN_MANT_ESCOLA_PRIVADA_EMP: integer (nullable = true)
     |-- IN_MANT_ESCOLA_PRIVADA_ONG: integer (nullable = true)
     |-- IN_MANT_ESCOLA_PRIVADA_OSCIP: integer (nullable = true)
     |-- IN_MANT_ESCOLA_PRIV_ONG_OSCIP: integer (nullable = true)
     |-- IN_MANT_ESCOLA_PRIVADA_SIND: integer (nullable = true)
     |-- IN_MANT_ESCOLA_PRIVADA_SIST_S: integer (nullable = true)
     |-- IN_MANT_ESCOLA_PRIVADA_S_FINS: integer (nullable = true)
     |-- TP_REGULAMENTACAO: integer (nullable = true)
     |-- TP_LOCALIZACAO_DIFERENCIADA: integer (nullable = true)
     |-- IN_EDUCACAO_INDIGENA: integer (nullable = true)

## Qual foi o estado brasileiro (CO_UF) com o segundo maior número de alunos no Censo Escolar 2020?


```pyspark
from pyspark.sql.functions import col, max, min, count, mean
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
type(matriculas_pt)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    <class 'pyspark.sql.dataframe.DataFrame'>


```pyspark

```


```pyspark
from pyspark.sql.functions import *
(
    matriculas_pt
    .groupBy("CO_UF")
    .agg(
        count((col("ID_MATRICULA")))
        )
    .sort(desc("count(ID_MATRICULA)"))
    .show()
)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----+-------------------+
    |CO_UF|count(ID_MATRICULA)|
    +-----+-------------------+
    |   35|           10399931|
    |   31|            4494458|
    |   33|            3617974|
    |   29|            3507272|
    |   41|            2731721|
    |   23|            2377672|
    |   43|            2377005|
    |   15|            2309261|
    |   26|            2248002|
    |   21|            2094724|
    |   42|            1716964|
    |   13|            1173846|
    |   25|             977977|
    |   32|             915822|
    |   22|             882072|
    |   27|             867722|
    |   24|             826043|
    |   28|             541954|
    |   17|             427753|
    |   11|             414641|
    +-----+-------------------+
    only showing top 20 rows

## No estado de Minas Gerais, quantos alunos moravam na zona rural (tp_zona_residencial = 2)?


```pyspark
(    matriculas
    .select("TP_ZONA_RESIDENCIAL", "CO_UF")
    .where(col("TP_ZONA_RESIDENCIAL") == 2)
    .where(col("CO_UF") == 31)
    .count()
)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    556054

## No estado de São Paulo, quantos alunos moravam na zona urbana (tp_zona_residencial = 1)?"TP_ZONA_RESIDENCIAL"


```pyspark
(
    matriculas
    .select("TP_ZONA_RESIDENCIAL", "CO_UF")
    .where(col("TP_ZONA_RESIDENCIAL") == 1)
    .where(col("CO_UF") == 35)
    .count()
)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    9905267

## No estado do Espírito Santo, quantos alunos do sexo masculino (tp_sexo=1) tinham autismo?


```pyspark
(
    matriculas
    .select("TP_SEXO", "CO_UF", "IN_AUTISMO")
    .where(col("TP_SEXO") == 1)
    .where(col("CO_UF") == 32)
    .where(col("IN_AUTISMO") == 1)    
    .count()
)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    9326

## Qual estado brasileiro possui o segundo maior número de alunas do sexo feminino com cegueira (in_cegueira=1)?


```pyspark
(
    matriculas
    .select("TP_SEXO", "IN_CEGUEIRA", "CO_UF", "ID_MATRICULA")
    .where(col("TP_SEXO") == 2)
    .where(col("IN_CEGUEIRA") == 1)
    .groupBy("CO_UF")
    .agg(
        count((col("ID_MATRICULA"))).alias("total_alunas_cegueira")
        )
    .sort(desc("total_alunas_cegueira"))
    .show(5)
)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----+---------------------+
    |CO_UF|total_alunas_cegueira|
    +-----+---------------------+
    |   35|                  718|
    |   33|                  523|
    |   31|                  401|
    |   29|                  395|
    |   43|                  335|
    +-----+---------------------+
    only showing top 5 rows

## Qual é a diferença (em número absoluto de pessoas) entre o segundo e o terceiro estados brasileiros que possuem os maiores números de alunas do sexo feminino com surdez (in_surdez=1)?


```pyspark
(
    matriculas
    .select("TP_SEXO", "IN_SURDEZ", "CO_UF", "ID_MATRICULA")
    .where(col("TP_SEXO") == 2)
    .where(col("IN_SURDEZ") == 1)
    .groupBy("CO_UF")
    .agg(
        count((col("ID_MATRICULA"))).alias("total_alunas_surdez")
        )
    .sort(desc("total_alunas_surdez"))
    .show()
)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----+-------------------+
    |CO_UF|total_alunas_surdez|
    +-----+-------------------+
    |   35|               3007|
    |   31|               1117|
    |   41|               1012|
    |   29|                962|
    |   15|                870|
    |   26|                780|
    |   43|                695|
    |   23|                665|
    |   21|                618|
    |   33|                585|
    |   42|                523|
    |   25|                388|
    |   13|                378|
    |   27|                336|
    |   24|                305|
    |   22|                270|
    |   32|                246|
    |   28|                175|
    |   11|                151|
    |   17|                149|
    +-----+-------------------+
    only showing top 20 rows


```pyspark
1117-1012
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    105

## Qual é o estado brasileiro que possui o menor número de alunos matriculados no 9 ano (tp_etapa_ensino = 41)?


```pyspark
(
    matriculas
    .select("TP_ETAPA_ENSINO", "CO_UF", "ID_MATRICULA")
    .where(col("TP_ETAPA_ENSINO") == 41)
    .groupBy("CO_UF")
    .agg(
        count((col("ID_MATRICULA"))).alias("total_alunos_nono")
        )
    .sort(asc("total_alunos_nono"))
    .show()
)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----+-----------------+
    |CO_UF|total_alunos_nono|
    +-----+-----------------+
    |   14|            10327|
    |   16|            11517|
    |   12|            14720|
    |   17|            24929|
    |   11|            25961|
    |   28|            29053|
    |   24|            40235|
    |   22|            44331|
    |   27|            45269|
    |   32|            47447|
    |   25|            51321|
    |   13|            65816|
    |   42|            89842|
    |   21|           111014|
    |   23|           122388|
    |   43|           125677|
    |   26|           125725|
    |   15|           127408|
    |   41|           150216|
    |   33|           181470|
    +-----+-----------------+
    only showing top 20 rows
