# O que é?

O caso de uso de algumas pessoas se baseia no uso em massa dos dados extraidos pelo scraper do cnpjpw, como eu [não estou disposto a abrir o código do scraper](https://github.com/cnpjpw/cnpjpw/issues/2), armazenei todas as suas execuções e disponibilizei no https://archive.cnpj.pw para tentar evitar carga desnecessária na API e disponibilizar mais facilmente os dados obtidos para essas pessoas. Esse repositório se trata então de um exemplo de client que consume o archive.

## Como usar

Atualmente você tem que alterar a chamada da funcao carregar\_principais\_banco() na main.py para os dias(só funciona com os dias passados, por enquanto) que você quer, então será carregado no cnpjpw.db no mesmo diretório do arquivo um sqlite análogo a base do cnpjpw para operações locais.
A única dependência externa do script é a biblioteca requests.


