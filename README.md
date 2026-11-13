# Bibous

Sistema distribuído de armazenamento de arquivos. Sua arquitetura é composta por três componentes independentes: cliente, mestre e nós de armazenamento.

O objetivo do sistema é permitir o armazenamento e a recuperação de arquivos distribuídos entre diferentes nós, com replicação, verificação de integridade e recuperação automática diante de falhas.

## Arquitetura

Adota uma arquitetura de coordenação centralizada com armazenamento distribuído. O mestre gerencia os metadados e determina a localização das réplicas; os nós armazenam os dados; o cliente solicita operações e transfere os arquivos diretamente para os nós indicados pelo mestre.

![Diagrama de alto nível](/img/master-slave-diagram.png)

### Mestre

O mestre é o coordenador central do sistema. Suas principais responsabilidades são:
 
* Receber o registro de novos nós e acompanhar sua disponibilidade por meio de heartbeats.
* Manter metadados dos arquivos e diretórios, incluindo tamanho, hash e localização das réplicas.
* Selecionar os nós de destino de novos arquivos, considerando capacidade disponível e fator de replicação.
* Coordenar operações solicitadas pelo cliente, como envio, consulta, listagem, remoção e movimentação de arquivos.
* Detectar nós indisponíveis e solicitar a criação de novas réplicas quando necessário.
* Persistir os metadados para possibilitar a recuperação do estado após uma reinicialização.

O mestre não atua como intermediário do conteúdo dos arquivos: sua função é coordenar as operações, evitando que o tráfego de dados se concentre em um único processo.

## Nó

O nó é o componente que disponibiliza espaço em disco para armazenar arquivos ou blocos de arquivos. O mesmo programa pode ser executado em várias instâncias, cada uma com identidade, endereço e diretório de dados próprios.

Entre suas responsabilidades estão:

* Enviar heartbeats periódicos ao mestre.
* Receber, armazenar e disponibilizar dados para leitura por clientes autorizados.
* Verificar a integridade dos dados por hash.
* Transferir réplicas diretamente para outros nós quando solicitado pelo mestre.
* Informar alterações de capacidade e reconciliar os dados locais após reinicializações.

## Cliente

O cliente é a interface de linha de comando (CLI) utilizada para interagir com o sistema.

Para operações de arquivos, o cliente consulta inicialmente o mestre, que informa os nós responsáveis pelos dados. Com essas informações, o cliente realiza upload e download diretamente nos nós de armazenamento, sem encaminhar o conteúdo pelo mestre.

A interface prevista inclui operações de envio, download, remoção, renomeação, movimentação, criação e listagem de diretórios, além de consulta de metadados e do estado de replicação.
