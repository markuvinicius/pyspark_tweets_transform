# PySpark Tweets Transformer
Programas PySpark responsáveis pela extração dos dados brutos da Landing Zone (lz.tweets) e carga nas tabelas de consumo na Consumption Zone (cz.[tabelas])

## Features
Realiza a extração de dados do twitter persistidos em banco de dados NoSQL Cassandra, aplica transformações e agregações e os disponibiliza para consumo.

- **Quantidade de Postagens por Tag:** Programa pySpark que sumariza o universo de dados coletados do twitter pelo programa `twitter-collector` por idioma e por tag de pesquisa.
- **Usuários com mais seguidores:** Programa pySpark que processa o universo de dados coletados do twitter pelo programa `twitter-collector` e perfila os usuários com maior número de seguidores agrupados por data de processamento. <br><br>
Mais informações sobre o programa `twitter-collector` <a href=https://github.com/markuvinicius/twitter-collector>aqui</a>

## Set-Up
### Apache Cassandra
A execução desta aplicação pré-supõe que o Apache Cassandra já esteja instalado e operante em ambiente local ou remoto.<br><br>
Para maiores informações sobre como realizar a instalação do cassandra, consulte <a href=http://cassandra.apache.org/doc/latest/getting_started/installing.html> aqui </a><br><br>
Para maiores informações sobre como criar o banco de dados utilizado por este projeto bem como para realizar uma coleta de dados do twitter, consulte <a href=https://github.com/markuvinicius/twitter-collector>aqui</a> 

### Python
Esta aplicação foi construída/testada utilizando a versão 2.7 do Python.<br>
<a href=https://www.python.org/download/releases/2.7/>Instalação Python 2.7 </a>

### Spark
Esta aplicação foi construída/testada utilizando `spark 2.2.1` <br>
<a href=https://spark.apache.org/downloads.html> Instalação Apache Spark </a>

### PySpark Cassandra
Esta aplição utiliza um driver para conexão do Spark com o Cassandra via Python. O `.jar` necessário para execução já se encontra disponível na pasta **lib**. <br>
Mais informações sobre o driver podem ser obtidas <a href=https://github.com/anguenot/pyspark-cassandra>aqui</a>
    

## Execução
Para facilitar a execução do projeto, são fornecidos dois script shell em <br>
- `scripts/exec_count_by_tag.sh` Executa o processamento para contagem de postagens por tag
- `scripts/exec_most_followed.sh` Executa o processamento para contabilizar os usuários com mais seguidores por data.