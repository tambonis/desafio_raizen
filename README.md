## Resolução do desafio proposto pela Raízen para avaliação técnica

## Descrição do Projeto

O teste consiste em desenvolver um pipeline ETL para extrair caches de pivô internos de relatórios consolidados disponibilizados pela agência reguladora de petróleo/combustíveis do governo brasileiro, ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis).
A partir deste relatório, deve-se construir um pipeline para realização de ETL para exibir as duas seguintes propostas:

1ª) Vendas de combustíveis derivados de petróleo por UF e produto;

2ª) Vendas de diesel por UF e tipo.

Os dados devem ser armazenados nos formatos a seguir:

| Column | Type |
| ------------ | ----------- |
| 'year_month' | 'date' |
| 'uf' | 'string' |
| 'product' | 'string' |
| 'unit' | 'string' |
| 'volume' | 'double' |
| 'created_at' | 'timestamp' |

Por fim um schema de particionamento ou indexação é requerido.

Para trazer mais valor agregado, a solução proposta complementa esses ítens adicionando data quality e Continuous Integration (CI) por meio do Github Actions. Dentro da DAG get_transform existe um data quality que mostra os devios padrões e as médias dos últimos 2 meses, assim como a comparação de modo a analisar se a diferença entre os 2 últimos meses é maior que 10% da média dos 2 últimos meses. Por fim, quando for realizado um pull request o Github Actions será acionado e realizará o build do projeto assim auxiliando na CI do projeto.

Fontes: próprio teste e Wikepedia.

## Tecnologias utilizadas

- 'Github': plataforma de hospedagem de código-fonte e arquivos com controle de versão usando o Git;
- 'Docker': conjunto de produtos de plataforma como serviço que usam virtualização de nível de sistema operacional para entregar software em pacotes chamados contêineres;
- 'Docker-compose': ferramenta para definir e executar aplicativos Docker de vários contêineres;
- 'Airflow': plataforma de gerenciamento de fluxo de trabalho de código aberto para pipelines de engenharia de dados;
- 'Python': linguagem de programação de alto nível, dinâmica, interpretada, modular, multiplataforma e orientada a objetos.

## Resolução

Na resolução do desafio, o primeiro passo realizado foi o acesso as abas que contém as informações necessárias para solução do problema. Nessa etapa o desfio foi propriamente o acesso, com a extensão .xls não foi possível concluir a tarefa sendo necessário recorrer a uma transformação via libreoffice. A conclusão desta etapa gera 2 arquivos: "oil_deritavite.xlsx" e "diesel.xlsx". Em seguida, os arquivos foram lidos e connvertidos em pandas dataframe e execução da solução base foi realizada. A etapa de ETL é concluída com gravração desses dados no formato parquet, sendo particionado por "product" e "year_month".
Para o gerenciamento do fluxo de trabalho optou-se pela utilização da ferramenta Airflow pela familiaridade, a qual executa sua função através de DAGs (Directed Acyclic Graph). A utilização desta ferramenta foi realizada por meio do docker-compose pelo motivo da praticidade, onde a partir da execução do docker-compose, é possível gerenciar o fluxo de trabalho por meio da porta 8080 na interface de usuário do Airflow.

A fim de agregar valor a resolução, implementou-se data quality e CI por meio do Github Actions. Dentro da DAG get_transform existe um data quality que mostra os devios padrões e as médias dos últimos 2 meses, assim como a comparação de modo a analisar se a diferença entre os 2 últimos meses é maior que 10% da média dos 2 últimos meses. Por fim, quando for realizado um pull request o Github Actions será acionado e realizará o build do projeto assim auxiliando no CI do projeto.

## Pontos de melhorias

Dentro da etapa de acesso as abas necessárias para resolução utilizou conversão de .xls para .xlsx. Esta conversão traz entrave de performance por gerar complexidade nos comandos executados dentro do dockerfile assim como tempo de execução não otimizado.

É possível melhorar o processo de gravação dos dados, a fim de que não seja necessário grava-los totalmente sempre que houver uma atualização. Complementarmente, é necessário pensar em uma abordagem para backup.

O data quality está simples, porém, a intenção era mostrar ser possível utilizar esta tecnica para auxliar na análise da saúde dos dados.

A melhoria associada ao GitHub Actions está associada a criação de branchs dev e master, assim como alguns detalhes para auxiliar no processo CI/CD.

## Execução do projeto

Executar o comando abaixo para clonar o repositório:
- git clone https://github.com/tambonis/desafio_raizen.git

Mover até o diretório do projeto e realizar a subida do conteiner airflow contendo as dependências do projeto:
- docker-compose up airflow-init
- docker-compose up

Acessar a porta no 8080 através do navegador web:
- localhost:8080

Caso seja requerido usuário e senha, digite airlfow e airflow, respectivamente. Na interface do usuário é possível visualizar ETL_raizen, caso não seja iniciada automaticamente clique no botão play e a execução se iniciará.

Caso for realizado um pull request é possível acompanhar o build pelo Gihub Actions.

## Conclusão

O teste consistia em realizar ETL utilizando estrutura de orquestração e tecnologia de conteinerização e foi concluído, porém, algumas com melhorias mencioanadas.
Finalizando, desde já agradeço a Raizen pela oportunidade cedida e fico à disposição para quaisquer questionamentos.

### Autor

Tiago Tambonis

