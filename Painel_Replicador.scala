/*
 *                     REPLICADOR DE TABELAS
 *
 * Autor: Miguel Pereira da Silva Neto (F7141129)
 * Data: 08/03/2022
 * Assunto: Processamento dos dados para geração de um painel de acompanhamento das tabelas replicadas.
 *          Há uma base no postgree que contém os metadados do Replicador de Tabelas, e uma base no Big_Hive que contem os dados de acesso às tabelas do Hive.
 *          Este código se propõe a gerar uma base para construir relatórios que respondam o seguinte:
 *            - Quais das tabelas replicadas estão mais sendo utilizadas?
 *            - Quais tabelas replicadas estão sem acesso há mais de 90 dias?
 *            - Nas tabelas replicadas, qual a quantidade de erros e reagendamentos tivemos?
 *          
 *          
 *          
 * Versão da Adriana         
 *          
 *  
 *             
 *            
 *            
 *            
 *            
 * Parâmetros: 
 * Versão    : 
 *           : 
 *                                     
 */

// SPARK-SHELL deve ser iniciado com o jar de conexão como posgree:
//    spark-shell  --driver-memory 6G --executor-memory 4G --num-executors 30 --jars /home/f7141129/jars/postgresql-42.3.1.jar
// Drivers podem ser baixados do HDFS "/dados/shared/bin/"


//IMPORTS
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, IntegerType, LongType, ShortType, StringType, StructField, StructType}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel




// PADRÕES E DEFINIÇÕES

val dateFormat = "yyyy-MM-dd"
val data_atual = spark.range(1).select(date_format(current_timestamp,dateFormat)).collectAsList().get(0).get(0).toString()

val qtd_dias = 90;   //Quantidade de dias considerados nas analises de utilização de tabelas replicadas.


/* _____    ____    _   _   ______  __   __   ____    ______    _____ 
  / ____|  / __ \  | \ | | |  ____| \ \ / /  / __ \  |  ____|  / ____|
 | |      | |  | | |  \| | | |__     \ V /  | |  | | | |__    | (___  
 | |      | |  | | | . ` | |  __|     > <   | |  | | |  __|    \___ \ 
 | |____  | |__| | | |\  | | |____   / . \  | |__| | | |____   ____) |
  \_____|  \____/  |_| \_| |______| /_/ \_\  \____/  |______| |_____/ 
*/

//Propriedades de conexão Postgresql
val url_postgre = "jdbc:postgresql://silobig12-master.postgresql.bdh.servicos.bb.com.br:5432/bbdata"

 val sqlHiveContext = new org.apache.spark.sql.SQLContext(sc);
 // sqlHiveContext.sql("set spark.sql.hive.convertMetastoreOrc=true");

//------------------------------------------------------------------------------------------------------------------------------------------------------------


 /*
  _______            ____    ______   _                    _____ 
 |__   __|   /\     |  _ \  |  ____| | |          /\      / ____|
    | |     /  \    | |_) | | |__    | |         /  \    | (___  
    | |    / /\ \   |  _ <  |  __|   | |        / /\ \    \___ \ 
    | |   / ____ \  | |_) | | |____  | |____   / ____ \   ____) |
    |_|  /_/    \_\ |____/  |______| |______| /_/    \_\ |_____/ 
 */                                                                  
                    
//Tabelas Postgresql - replicas_sqoop.hst_prct_repl_tab
val df_post__hst_prct_repl_tab = spark.read.format("jdbc").option("url", url_postgre).option("query", "select * from replicas_sqoop.hst_prct_repl_tab").option("user", "user_bbdata").option("password", "bstdiwyr").load()
// Filtando apenas os registros com a quantidade de dias menor que o qtd_dias considerado. Analise apenas dos últimos X dias.
val df_post__hst_prct_repl_tab_02 = df_post__hst_prct_repl_tab.withColumn("data_prct", to_date($"dt_prct")).withColumn("data_atual", lit(data_atual)).withColumn("qtd_dias", datediff($"data_atual", $"data_prct")).drop($"data_prct").drop($"data_atual").filter($"qtd_dias"<=qtd_dias).drop($"qtd_dias")

//controle_tabelas_replicadas
val df_controle_tabelas_replicadas = spark.read.format("jdbc").option("url", url_postgre).option("query", "select * from replicas_sqoop.controle_tabelas_replicadas").option("user", "user_bbdata").option("password", "bstdiwyr").load()



//Tabela Hive big_hive.hive_big.ranger_audits_v2 - Contem dados de acesso às tabelas
val file = "/ranger/audit/hiveServer2/*"
val df_hive__ranger_audits = spark.read.json(file).filter($"access"==="SELECT").persist(StorageLevel.MEMORY_AND_DISK);


//------------------------------------------------------------------------------------------------------------------------------------------------------------


/*
  ______   _    _   _   _    _____    ____    ______    _____ 
 |  ____| | |  | | | \ | |  / ____|  / __ \  |  ____|  / ____|
 | |__    | |  | | |  \| | | |      | |  | | | |__    | (___  
 |  __|   | |  | | | . ` | | |      | |  | | |  __|    \___ \ 
 | |      | |__| | | |\  | | |____  | |__| | | |____   ____) |
 |_|       \____/  |_| \_|  \_____|  \____/  |______| |_____/ 
*/


//------------------------------------------------------------------------------------------------------------------------------------------------------------


//Função que extrai de uma string, substrings que atendem à uma Rexex. No caso, estou estraindo as tabelas DB2 de um SQL passado por parametro.
///////////////  fonte: (https://stackoverflow.com/questions/47981699/extract-words-from-a-string-column-in-spark-dataframe) ///////////////
import java.util.regex.Pattern
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit

def regexp_extractAll = udf((job: String, exp: String, groupIdx: Int) => {
      println("the column value is" + job.toString())
      val pattern = Pattern.compile(exp.toString)
      val m = pattern.matcher(job.toString)
      var result = Seq[String]()
      while (m.find) {
        val temp = 
        result =result:+m.group(groupIdx)
      }
      result.mkString(",")
    })
///////////////  fonte: (https://stackoverflow.com/questions/47981699/extract-words-from-a-string-column-in-spark-dataframe) ///////////////



//Verifica o quanto cada tabela foi utilizada nos últimos "X" dias.
//Recebe dataframe com 1 coluna chave: "evtTime" contendo timestamp.
//filtra somente os registros cujo o acesso foi feito num prazo máximo de X dias (dias_limite).
def limiteDiasUtilizacao (dias_limite:Integer, DF:DataFrame) : DataFrame = {

   // Filtra apenas os registros do tipo "Select" e que contem no código SQL a substring "db2"
   //val df_utiliz_tabelas = df_hive__ranger_audits_v2.filter($"access"==="SELECT" && upper($"reqData").rlike("DB2")).select("access", "resource", "reqUser", "evtTime", "reqData")

  //Criação da Coluna qtd_dias
  //val df_dias1 = DF.select($"evtTime")
  val df_dias2 = DF.withColumn("evtDate", to_date($"evtTime")).withColumn("data_atual", lit(data_atual)).withColumn("qtd_dias", datediff($"data_atual", $"evtDate")).drop($"evtDate").drop($"data_atual")

  //Filtrando apenas os registros dos ultimos X dias
  val df_retorno = df_dias2.filter($"qtd_dias" <= dias_limite)

   return df_retorno
}


//Cria dataframe com o nome da tabela e a quantidade de dias que ela está sem ser utilizada.
//Recebe dataframe com 1 coluna chave: "evtTime" contendo timestamp do acesso à tabela.
def limiteDiasOciosidade (DF:DataFrame) : DataFrame = {

   //A partir do Dataframe inicial, pega a ultima utilização de cada tabela
   val df_dias_ociosidade_1 = DF.groupBy($"table").agg(max($"evtTime").as("UltimaUtilizacao"))

  //Criação da Coluna qtd_dias desde a ultima utilizacao das tabelas
   val df_retorno = df_dias_ociosidade_1.withColumn("Data_UltimaUtilizacao", to_date($"UltimaUtilizacao")).withColumn("data_atual", lit(data_atual)).withColumn("qtd_dias", datediff($"data_atual", $"Data_UltimaUtilizacao")).drop($"UltimaUtilizacao").drop($"data_atual").orderBy(desc("qtd_dias"))

   return df_retorno
}

//-------------------------------------



/*
  _____  __  __  _____    ____   _   _    ____      _       ___  
 | ____| \ \/ / | ____|  / ___| | | | |  / ___|    / \     / _ \ 
 |  _|    \  /  |  _|   | |     | | | | | |       / _ \   | | | |
 | |___   /  \  | |___  | |___  | |_| | | |___   / ___ \  | |_| |
 |_____| /_/\_\ |_____|  \____|  \___/   \____| /_/   \_\  \___/ 
*/

//Padronizando todo o sql como upper para localizar as tabelas com Regex
val df_hive__ranger_audits_1 = df_hive__ranger_audits.withColumn("SQL", upper($"reqData"));

//Chamando a função de extração das tabelas e filtrando apenas os registros que encontram tableas com "DB2<SIGLA>." no sql
val df_hive__ranger_audits_2 = df_hive__ranger_audits_1.withColumn("Tabelas", regexp_extractAll(new Column("SQL"), lit("DB2[a-zA-Z][a-zA-Z][a-zA-Z].\\w+"), lit(0))).filter(length($"Tabelas") > 0)

//Colocando uma tabela por linha
val df_hive__ranger_audits_3 = df_hive__ranger_audits_2.withColumn("lista_tabelas", split($"Tabelas", ",")).withColumn("table", explode($"lista_tabelas")).drop($"Tabelas").drop($"lista_tabelas")

//Limpando colunas
val df_hive__ranger_audits_4 = df_hive__ranger_audits_3.select($"id", $"result", $"reqUser", $"evtTime", $"table")


//Limitando as tabelas do ranger_audits àquelas que fazem parte do universo da replicação
val df_post__tabelas_replicadas = df_post__hst_prct_repl_tab.withColumn("tabela", concat($"nm_db", lit("."), $"nm_tab")).select($"tabela").distinct

val df_hive__ranger_audits_5 = df_hive__ranger_audits_4.as("A").join(df_post__tabelas_replicadas.as("B"), df_hive__ranger_audits_4("table") === df_post__tabelas_replicadas("tabela")
                                                                      , "Inner").select(df_hive__ranger_audits_4("id")
                                                                                      , df_hive__ranger_audits_4("result")
                                                                                      , df_hive__ranger_audits_4("reqUser")
                                                                                      , df_hive__ranger_audits_4("evtTime")
                                                                                      , df_hive__ranger_audits_4("table")
                                                                                       )


//Tabelas do RANGER AUDITS utilizadas nos ultimos X dias
val df_tabelas_mais_utilizadas = limiteDiasUtilizacao(qtd_dias, df_hive__ranger_audits_5).select($"table").groupBy($"table").agg(sum(lit(1)).as("Qtd_acesso")).orderBy(desc("Qtd_acesso"))
// Tempo de Osciosidade das tabelas
val df_tabelas_n_acessadas = limiteDiasOciosidade(df_hive__ranger_audits_5)


////////////////////////////////////////////////////////////////////////////
// Tabelas diárias com mais de 3 dias sem replicação finalizadas com sucesso
////////////////////////////////////////////////////////////////////////////
//Universo de tabelas sendo replicadas diariamente
val df_tab_diarias = df_controle_tabelas_replicadas.filter($"tp_replicacao"===1).select($"nm_db", $"nm_tabela")

// Das tabelas diárias, quais não foram replicadas há mais de 3 dias?
val teste = df_post__hst_prct_repl_tab_02.filter($"ds_est_crga" != "SUCESSO").select



/*
  ____    _____   _          _      _____    ___    ____    ___    ___    ____  
 |  _ \  | ____| | |        / \    |_   _|  / _ \  |  _ \  |_ _|  / _ \  / ___| 
 | |_) | |  _|   | |       / _ \     | |   | | | | | |_) |  | |  | | | | \___ \ 
 |  _ <  | |___  | |___   / ___ \    | |   | |_| | |  _ <   | |  | |_| |  ___) |
 |_| \_\ |_____| |_____| /_/   \_\   |_|    \___/  |_| \_\ |___|  \___/  |____/ 
*/

//Tabelas do RANGER AUDITS utilizadas nos ultimos X dias
df_tabelas_mais_utilizadas.show(10, false)

// Tempo de Osciosidade das tabelas
df_tabelas_n_acessadas.show(30, false)





