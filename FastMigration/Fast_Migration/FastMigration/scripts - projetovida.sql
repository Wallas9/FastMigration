-- UNIDADE ESCOLAR 
-- cuidado pois existe a tabela unidade e empresa. as contas estão relacionadas a empresa.
-- OBS: gerei um insert somente da unidade 2 
select u.unidade as codunidade,
u.nome as dscunidade,
u.fantasia as nomefantasia,
replace(replace(replace(replace(u.cnpj,'.',''),'-',''),'/',''),')','') as cnpj,
u.endereco as endereco,
u.bairro as bairro,
u.cidade,
replace(replace(replace(replace(u.cep,'.',''),'-',''),'/',''),')','') as cep,
replace(replace(replace(replace(replace(u.telefone,'(',''),'.',''),'-',''),'/',''),')','') as telefone,
u.a_email as email,
left(u.nome,10) as dscunidadeabrev
from sigunida u;

-- inseri as unidade da tabela sigempre
select
e.codigo as codunidade,
e.descricao as dscunidade,
left(e.resumo,10) as dscunidadeabrev,
e.descricao as nomefantasia,
e.cnpj as cnpj,
e.endereco as endereco
from sigempre e;

ALTER TABLE unidadeescolar ADD COLUMN codunidadesiga2 INTEGER;

INSERT INTO unidadeescolar(codunidadesiga2,dscunidade,dscunidadeabrev,nomefantasia,cnpj,endereco)

SELECT * FROM unidadeescolar_siga2 
WHERE cnpj NOT IN (SELECT cnpj FROM unidadeescolar)
GROUP BY cnpj;




-- DISCIPLINAS
-- Fiz o select abaixo e gerei insert
-- OBS: O siga cadastra disciplina por unidade, fiz um script para unificar pelo nome
select
d.unidade as codunidade,
d.codigo as codigo,
d.resumo as sigla,
d.descricao as DESCRICAO,
'S' as ativo
from sigdisci d;

CREATE TABLE DISCIPLINA_SIGA (
    CODIGO integer,
    SIGLA VARCHAR(4),
    DESCRICAO VARCHAR(82),
    ATIVO CHAR(1),
	 coddisciplina_tella INTEGER);

INSERT INTO disciplina (dscabreviada,dscdisciplina,ativo)
SELECT left(sigla,5),descricao,ativo FROM disciplina_siga GROUP BY descricao;

UPDATE disciplina_siga ds JOIN disciplina d ON d.dscdisciplina = ds.descricao SET ds.coddisciplina_tella = d.coddisciplina;

-- CONTAS
-- OBS: reparei que não existe codempresa para as contas desse cliente, alterei o codbanco 356 para 33 pois o Banco real virou santander
select c.banco as codbanco,
c.nome as dscconta,
c.conta as numconta,
c.carteira as carteira,
c.sequencia as sequencialremessa,
c.agencia as agencia
from sigbccta c;

INSERT INTO conta (codbanco,dscconta,numconta,carteira,sequencialremessa,agencia)
SELECT right(if(codbanco = '0356','033',codbanco),3),dscconta,numconta,carteira,sequencialremessa,agencia FROM conta_siga;

-- CENTRO DE RECEITA
-- os codreceita estavam duplicados pois o sistema cria por unidade, eu concatenei o codunidade com o codreceita e importei para uma tabela auxiliar
select
(cr.unidade || cr.codigo) as codreceita,
cr.nome as dscreceita
from sigparce cr;

create table centroreceita_siga(
codreceita int(10),
dscreceita int(10),
codreceita_tella INTEGER);

INSERT INTO centroreceita (codreceita,dscreceita,ativo,multa,juros)
SELECT codreceita,left(dscreceita,20),'S','0.02','0.00033' FROM centroreceita_siga GROUP BY dscreceita;

UPDATE centroreceita_siga ca JOIN centroreceita c ON left(ca.dscreceita,20) = left(c.dscreceita,20)  SET ca.codreceita_tella = c.codreceita;

-- CURSO 
-- concatenado o codigo com a unidade para separar os cursos.
SELECT (C.unidade || C.codigo) as codcurso,
max(C.descricao) as dsccurso,
max(C.resumo) as dscabreviada,
max(0) as cargahoraria,
max('S') as ativo,
max(IIF(c.tipo = 'L', 3, IIF(c.descricao like '%FUNDAM%', 1,2))) as codnivel
FROM  sigcurso C
group BY C.unidade, C.codigo;

CREATE TABLE CURSO_SIGA (
    CODCURSO VARCHAR(5),
    DSCCURSO VARCHAR(40),
    DSCABREVIADA VARCHAR(10),
    CARGAHORARIA INTEGER,
    ATIVO CHAR(1),
    CODNIVEL INTEGER,
	 codcurso_tella INTEGER);
	 
INSERT INTO curso(codcurso,dsccurso,dscabreviada,cargahoraria,ativo,codnivel)
SELECT codcurso,dsccurso,dscabreviada,cargahoraria,ativo,codnivel FROM curso_siga;

-- SERIECURSO
select  (s.unidade || s.curso || s.serie) as codseriecurso,
s.unidade as codunidade,
(s.unidade || s.curso) as codcurso,
s.serie as codserie,
max(IIF(c.tipo = 'L', (s.serie||'º UNICA') , IIF(c.descricao like '%ANO%', (s.serie||'º ANO'),(s.serie||'º SERIE')))) as dscserie,
max('S') as ativo,
 s.serie as ORDEM,
 max('N') as CONCLUINTE,
'MIGRACAO2020' as cadastradopor
 FROM  sigserie s
 join   sigcurso c on s.curso = c.codigo
-- where  s.unidade in ('01', '02')
group by s.unidade, s.curso, s.serie;

INSERT INTO seriecurso(codseriecurso,codunidade,codcurso,codserie,dscserie,ativo,ordem,concluinte,cadastradopor)
SELECT codseriecurso,codunidade,codcurso,codserie,dscserie,ativo,ordem,concluinte,cadastradopor FROM seriecurso_siga;

-- CURSOS UNIDADE
DELETE FROM cursosunidade;
insert into cursosunidade(codunidade, codcurso, portalsimplificado)
(SELECT CODUNIDADE, CODCURSO, 'N' FROM seriecurso GROUP BY CODUNIDADE, CODCURSO) ;




-- CRIAR TABELA CIDADE DO ESCOLAWEB NO BANCO DO CLIENTE
CREATE TABLE CIDADE_TELLA (
    cod_cidade INT,
    cod_estado SMALLINT,
    nom_cidade VARCHAR(50) ,
    municipioibge INT
)

-- FUNCIONARIO
-- insere os funcionários setando o cargo apenas do professor. tive que agrupar por nome por conta de várias duplicidade. filtor por unidade.
select
min((p.unidade || p.codigo)) as funcodigo,
max(p.nome) as nome,
max(p.sexo) as sexo,
max(p.email) as email,
max(p.datanasc) as dtnascimento,
max(p.naturalidade) as naturalidade,
max((cast(p.endereco as varchar(100)) || cast(', ' as varchar(2))|| cast(COALESCE(p.numero,'') as varchar(10)))) as endereco,
left(max(p.complemento),10) as complemento,
max(p.bairro) as bairro,
max(p.cidade) as cidade,
max(replace(replace(replace(replace(replace(p.cep,'(',''),'.',''),'-',''),'/',''),')','')) as cep,
1 as codcargo,
max('S') as ativo
from sigprofe p
group by p.nome;

INSERT INTO funcionario(funcodigo,nome,sexo,email,dtnascimento,naturalidade,endereco,complemento,bairro,cidade,cep,codcargo,ativo)
SELECT funcodigo,nome,sexo,email,dtnascimento,naturalidade,endereco,complemento,bairro,c.cod_cidade,cep,codcargo,ativo FROM funcionario_siga fs 
LEFT JOIN cidade c ON c.nom_cidade = fs.CIDADE GROUP BY fs.FUNCODIGO;

INSERT INTO funcionariounidade (codfuncionario,codunidade) (SELECT funcodigo, 1 FROM funcionario);

-- TURMA
-- Rodar select e inserir na tabela turma do escolaweb
select (COALESCE(s.classe,'X') || COALESCE(s.turno,'Y')),null from sigclass s group by (COALESCE(s.classe,'X') || COALESCE(s.turno,'Y'));

-- criar na base do cliente e inserir os dados de turma do escolaweb na base do cliente
 create table turma(
 codturma integer,
 dscturma varchar(50),
 turno char(1)
 )
 commit;
 
 -- CONFIGTURMA
select right(sc.anoletivo,2)||lpad((sc.unidade || sc.curso || sc.serie),6,0)||lpad(t.codturma,2,0) as codconfturma,
max((sc.unidade || sc.curso || sc.serie)) as codseriecurso,
max(sc.descricao) as dscturma,
max(cast(coalesce(sc.datainicio,sc.anoletivo || '-02-03')as date)) as dtinicio,
max(cast(coalesce(sc.datafim,sc.anoletivo || '-12-18')as date)) as dtfim,
max(sc.anoletivo) as anoletivo,
max(sc.turno) as turno,
max(current_timestamp) as dtcadastro,
max('MIGRACAO 2020') as cadastradopor,
max(t.codturma) as codturma,
max(sc.alunomax) as numvagas
from sigclass sc
LEFT join turma t on t.dscturma = (COALESCE(sc.classe,'X') || COALESCE(sc.turno,'Y'))
group by  right(sc.anoletivo,2)||lpad((sc.unidade || sc.curso || sc.serie),6,0)||lpad(t.codturma,2,0);
 
INSERT INTO configturma(codconfturma,codconfturma_aux,codseriecurso,dscturma,dtinicio,dtfim,anoletivo,turno,dtcadastro,cadastradopor,codturma,numvagas)
SELECT codconfturma_aux,codconfturma,codseriecurso,dscturma,dtinicio,dtfim,anoletivo,turno,dtcadastro,cadastradopor,codturma,numvagas FROM configturma_siga2;

-- ALUNO
-- No siga o seguinte: 
-- O prontuario é o codaluno
-- O codaluno se repete por unidade, tem que ficar experto, por isso eu concatenei o codunidade com o prontuario
-- Tem muito aluno repetido, quando o aluno muda de unidade, eles cadastram de novo com outro codaluno
-- select na base do siga
select
(g.unidade || replace(g.prontuario,'-','')) as codaluno,
upper(max((rtrim(g.nome)||' '||ltrim(g.sobrenome)))) as nomealuno,
upper(max((coalesce(g.endereco, '')||' '||coalesce(g.numero, '')||' '||coalesce(g.complement, '')))) as endereco,
upper(max(g.bairro)) as bairro,
max(g.cidade) as cidade,
max(replace(g.cep,'-','')) as cep,
max(g.foneres) as telresid,
max(g.fonecel) as celular,
max(iif(g.email = '',null,g.email)) as email,
max(g.sexo) as sexo,
max(g.nascimento) dtnascimento,
max(g.cidade) as cidadenaturalidade,
upper(max(g.pai)) as pai,
upper(max(left(g.profi_pai, 40))) as profpai,
max(g.fonepai) as telpai,
max(g.celpai) as celularpai,
max(g.emailpai) as emailpai,
max(g.mae) as mae,
max(g.profi_mae) as profmae,
max(g.fonemae) as telmae,
max(g.celmae) as celularmae,
max(g.emailmae) as emailmae,
max(g.cpf) as cpf,
max(g.rg) as rg,
'' as certidaonumero,
'' as certidaolivrofolha,
'' as certidaocartorio,
max(iif(g.end_pai is null, '', g.end_pai) || iif(g.num_pai is null, '', g.num_pai)) as enderecopai,
max(g.bair_pai) as bairropai,
max(g.cep_pai) as ceppai,
max(iif(g.end_mae is null, '', g.end_mae) || iif(g.num_mae is null, '', g.num_mae)) as enderecomae,
max(g.bair_mae) as bairromae,
max(g.cep_mae) as cepmae,
max(na.descricao) as nacionalidade,
max(np.descricao) as nacionalidadepai,
max(nm.descricao) as nacionalidademae,
max(g.nascpai) as dtnascimentopai,
max(g.nascmae) as dtnascimentomae,
max(g.estcivpai) as estadocivilpai,
max(g.estcivmae) as estadocivilmae,
max(g.cidadenasc) as naturalidade,
max(g.cpfpai) as cpfpai,
max(g.rgpai) as rgpai,
max(g.cpfmae) as cpfmae,
max(g.rgmae) as rgmae
from siggeral g
left join signacio na on na.codigo = g.nacio
left join signacio np on np.codigo = g.nacio_pai
left join signacio nm on nm.codigo = g.nacio_mae
group by (g.unidade || replace(g.prontuario,'-',''));

-- tabela auxiliar no escolaweb
CREATE TABLE ALUNO_SIGA (
    CODALUNO VARCHAR(7),
    NOMEALUNO VARCHAR(511),
    ENDERECO VARCHAR(107),
    BAIRRO VARCHAR(50),
    CIDADE VARCHAR(40),
    CEP VARCHAR(9),
    TELRESID VARCHAR(30),
    CELULAR VARCHAR(30),
    EMAIL VARCHAR(100),
    SEXO VARCHAR(1),
    DTNASCIMENTO DATE,
    CIDADENATURALIDADE VARCHAR(40),
    PAI VARCHAR(60),
    PROFPAI VARCHAR(50),
    TELPAI VARCHAR(30),
    CELULARPAI VARCHAR(35),
    EMAILPAI VARCHAR(100),
    MAE VARCHAR(60),
    PROFMAE VARCHAR(50),
    TELMAE VARCHAR(30),
    CELULARMAE VARCHAR(35),
    EMAILMAE VARCHAR(100),
    CPF VARCHAR(16),
    RG VARCHAR(50),
    CERTIDAONUMERO CHAR(0),
    CERTIDAOLIVROFOLHA CHAR(0),
    CERTIDAOCARTORIO CHAR(0),
    ENDERECOPAI VARCHAR(55),
    BAIRROPAI VARCHAR(50),
    CEPPAI VARCHAR(9),
    ENDERECOMAE VARCHAR(55),
    BAIRROMAE VARCHAR(50),
    CEPMAE VARCHAR(9),
    NACIONALIDADE VARCHAR(30),
    NACIONALIDADEPAI VARCHAR(30),
    NACIONALIDADEMAE VARCHAR(30),
    DTNASCIMENTOPAI DATE,
    DTNASCIMENTOMAE DATE,
    ESTADOCIVILPAI VARCHAR(15),
    ESTADOCIVILMAE VARCHAR(15),
    NATURALIDADE VARCHAR(50),
    CPFPAI VARCHAR(14),
    RGPAI VARCHAR(15),
    CPFMAE VARCHAR(14),
    RGMAE VARCHAR(15));

-- ajuste de cpf em branco
UPDATE aluno_siga SET cpf = NULL WHERE cpf = '';
UPDATE aluno_siga SET cpfpai = NULL WHERE cpfpai = '';
UPDATE aluno_siga SET cpfmae = NULL WHERE cpfmae = '';

-- insert da tabela auxiliar no escolaweb formatando os devidos campos
INSERT INTO aluno (CODALUNO, NOMEALUNO, ENDERECO, BAIRRO, CIDADE, CEP, TELRESID, CELULAR, EMAIL, SEXO, DTNASCIMENTO, CIDADENATURALIDADE, PAI, PROFPAI, TELPAI, CELULARPAI, EMAILPAI, MAE, PROFMAE, TELMAE, CELULARMAE, EMAILMAE, 
CPF, RG, CERTIDAONUMERO, CERTIDAOLIVROFOLHA, CERTIDAOCARTORIO, ENDERECOPAI, BAIRROPAI, CEPPAI, ENDERECOMAE, BAIRROMAE, CEPMAE, NACIONALIDADE, NACIONALIDADEPAI, NACIONALIDADEMAE, DTNASCIMENTOPAI, DTNASCIMENTOMAE, 
ESTADOCIVILPAI, ESTADOCIVILMAE, NATURALIDADE, CPFPAI, RGPAI, CPFMAE, RGMAE,obs )

SELECT 
max(CODALUNO), 
NOMEALUNO, 
ENDERECO, 
BAIRRO, 
c.cod_cidade, 
left(REMOVEcaractere(cep),8) AS cep,
if(length(REMOVEcaractere(telresid)) IN (8,10),REMOVEcaractere(telresid),if(length(REMOVEcaractere(celular)) IN (8,10),REMOVEcaractere(celular),NULL) ) AS telresid,
if(length(REMOVEcaractere(celular)) IN (9,11),REMOVEcaractere(celular),if(length(REMOVEcaractere(telresid)) IN (9,11),REMOVEcaractere(telresid),NULL)) AS celular,
if(validaemail(email) = 'V', email, NULL) AS email,
SEXO, 
DTNASCIMENTO, 
c.cod_cidade,
PAI, 
upper(left(PROFPAI,40)) AS profepai, 
if(length(REMOVEcaractere(telpai)) IN (8,10),REMOVEcaractere(telpai),if(length(REMOVEcaractere(celularpai)) IN (8,10),REMOVEcaractere(celularpai),NULL) ) AS telpai, 
if(length(REMOVEcaractere(celularpai)) IN (9,11),REMOVEcaractere(celularpai),if(length(REMOVEcaractere(telpai)) IN (9,11),REMOVEcaractere(telpai),NULL)) AS celularpai,
if(validaemail(emailpai) = 'V', emailpai, NULL) AS emailpai, 
MAE, 
UPPER(left(PROFMAE,40)) AS profemae, 
if(length(REMOVEcaractere(telmae)) IN (8,10),REMOVEcaractere(telmae),if(length(REMOVEcaractere(celularmae)) IN (8,10),REMOVEcaractere(celularmae),NULL) ) AS telmae, 
if(length(REMOVEcaractere(celularmae)) IN (9,11),REMOVEcaractere(celularmae),if(length(REMOVEcaractere(telmae)) IN (9,11),REMOVEcaractere(telmae),NULL)) AS celularmae,
if(validaemail(emailmae) = 'V', emailmae, NULL) AS emailmae,
lpad(REMOVEcaractere(cpf),11,'0') AS cpf,
RG, 
CERTIDAONUMERO, 
CERTIDAOLIVROFOLHA, 
CERTIDAOCARTORIO, 
ENDERECOPAI, 
BAIRROPAI, 
left(REMOVEcaractere(ceppai),8) AS ceppai,
ENDERECOMAE, 
BAIRROMAE, 
left(REMOVEcaractere(cepmae),8) AS cepmae, 
NACIONALIDADE, 
NACIONALIDADEPAI, 
NACIONALIDADEMAE, 
DTNASCIMENTOPAI, 
DTNASCIMENTOMAE, 
ESTADOCIVILPAI, 
ESTADOCIVILMAE, 
NATURALIDADE,
lpad(REMOVEcaractere(CPFPAI),11,'0') AS CPFPAI,
RGPAI, 
lpad(REMOVEcaractere(CPFMAE),11,'0') AS CPFMAE,
RGMAE,
UPPER(CONCAT('TELEFONES DO ALUNO: ',ifnull(celular,''),' | ',IFNULL(telresid,''),' / TELEFONES DO PAI ',IFNULL(celularpai,''),' | ', IFNULL(telpai,''),' / TELEFONES DA MÃE ',IFNULL(celularmae,''),' | ', IFNULL(telmae,''),' / EMAIL ALUNO: ',IFNULL(email,''),' / EMAIL ´PAI: ',IFNULL(emailpai,''),' / EMAIL MAE: ',IFNULL(emailmae,''))) AS obs 
FROM aluno_siga a
LEFT JOIN cidade c ON a.cidade = c.nom_cidade
GROUP BY nomealuno;

-- insere codunificado
ALTER TABLE aluno_siga ADD COLUMN codaluno_unificado INTEGER;
UPDATE aluno_siga aa JOIN aluno a ON a.nomealuno = aa.NOMEALUNO SET aa.codaluno_unificado = aa.CODALUNO;

-- Alunos com o cpf repetido

-- consulta para verificar se existe algum caso
SELECT COUNT(a.codaluno),a.cpf,a.* from aluno a
WHERE a.cpf IS NOT NULL
GROUP BY a.cpf HAVING COUNT(1)>1 
ORDER BY COUNT(a.codaluno) ;

-- Updates para corrigir o caso 
UPDATE aluno a
SET a.obs = CONCAT(ifnull(a.obs,''),' | CPF DO ALUNO: ',ifnull(a.cpf,'')),
a.cpf = NULL
WHERE a.cpf IN (SELECT tab.cpf FROM (SELECT aa.cpf from aluno_siga aa
WHERE aa.cpf IS NOT NULL
GROUP BY aa.cpf HAVING COUNT(1)>1) AS tab ) 
AND LENGTH(CONCAT(ifnull(a.obs,''),' | CPF DO ALUNO: ',ifnull(a.cpf,''))) <= 600 ;

-- Alunos com o mesmo cpf da mãe

-> consulta para verificar se existe algum caso
SELECT a.cpf , a.cpfmae,a.* FROM aluno a
WHERE a.cpf = a.cpfmae
AND a.cpf IS NOT NULL;

-> Update para corrigir o caso 
UPDATE aluno a
SET a.obs = CONCAT(IFNULL(a.obs,''),' | CPF DO ALUNO: ',IFNULL(a.cpf,'')),
a.cpf = NULL
WHERE a.cpf = a.cpfmae
AND a.cpf IS NOT NULL;

-- Alunos com o mesmo cpf do pai

-> consulta para verificar se existe algum caso
SELECT a.cpf , a.cpfpai,a.* FROM aluno a
WHERE a.cpf = a.cpfpai
AND a.cpf IS NOT NULL;

-> Update para corrigir o caso 
UPDATE aluno a
SET a.obs = CONCAT(IFNULL(a.obs,''),' | CPF DO ALUNO: ',IFNULL(a.cpf,'')),
a.cpf = NULL
WHERE a.cpf = a.cpfpai
AND a.cpf IS NOT NULL;

-- RESPONSAVEL
-- criar tabela auxiliar no escolaweb
CREATE TABLE `responsavel_siga` (
	`codsoc` INT(11) NOT NULL AUTO_INCREMENT,
	`cpf` VARCHAR(14) NOT NULL,
	`nomeresponsavel` VARCHAR(100) NOT NULL,
	`profissao` VARCHAR(100) NULL DEFAULT NULL,
	`localtrabalho` VARCHAR(100) NULL DEFAULT NULL,
	`teltrabalho` VARCHAR(50) NULL DEFAULT NULL,
	`telresid` VARCHAR(50) NULL DEFAULT NULL,
	`celular` VARCHAR(50) NULL DEFAULT NULL,
	`email` VARCHAR(100) NULL DEFAULT NULL,
	`rg` VARCHAR(50) NULL DEFAULT NULL,
	`endereco` VARCHAR(120) NULL DEFAULT NULL,
	`bairro` VARCHAR(60) NULL DEFAULT NULL,
	`cidade` VARCHAR(50) NULL DEFAULT NULL,
	`uf` INT(10) UNSIGNED NULL DEFAULT NULL,
	`cep` VARCHAR(8) NULL DEFAULT NULL,
	`rendaliquida` DECIMAL(10,2) NULL DEFAULT NULL,
	`sexo` CHAR(1) NULL DEFAULT NULL,
	`estadocivil` VARCHAR(20) NULL DEFAULT NULL,
	`dtnascimento` DATE NULL DEFAULT NULL,
	`escolaridade` VARCHAR(40) NULL DEFAULT NULL,
	`pai` VARCHAR(100) NULL DEFAULT NULL,
	`mae` VARCHAR(100) NULL DEFAULT NULL,
	`enderecocom` VARCHAR(120) NULL DEFAULT NULL,
	`bairrocom` VARCHAR(30) NULL DEFAULT NULL,
	`cepcom` VARCHAR(8) NULL DEFAULT NULL,
	`codcidadecom` INT(11) NULL DEFAULT NULL,
	`cargo` VARCHAR(30) NULL DEFAULT NULL,
	`senha` VARCHAR(15) NULL DEFAULT NULL,
	`login` VARCHAR(20) NULL DEFAULT NULL,
	`fotoportal` VARCHAR(120) NULL DEFAULT NULL,
	`nacionalidade` VARCHAR(50) NULL DEFAULT NULL,
	`naturalidade` VARCHAR(50) NULL DEFAULT NULL,
	`obs` VARCHAR(200) NULL DEFAULT NULL,
	`rgorgaoemissor` INT(2) NULL DEFAULT NULL COMMENT 'Orgao Emissor da indentidade. Relaciona com a tabela orgaoemissoridentidade',
	`rgestado` SMALLINT(6) NULL DEFAULT NULL COMMENT 'Estado Identidade',
	`rgdataemissao` DATE NULL DEFAULT NULL COMMENT 'Data de Emissao RG',
	`codpaisorigem` SMALLINT(6) NULL DEFAULT NULL,
	`codcidadenaturalidade` INT(11) NULL DEFAULT NULL,
	`codescolaridade` INT(11) NULL DEFAULT NULL,
	`codfatura` INT(11) NULL DEFAULT NULL,
	`agrupamentofatura` CHAR(1) NULL DEFAULT 'R' COMMENT 'R=responsavel; A=aluno;',
	`placa` VARCHAR(50) NULL DEFAULT NULL,
	`veiculo` VARCHAR(50) NULL DEFAULT NULL,
	`fotoblob` BLOB NULL,
	`dtcadastro` DATETIME NULL DEFAULT NULL,
	`dtalteracao` DATETIME NULL DEFAULT NULL,
	`cadastradopor` VARCHAR(30) NULL DEFAULT NULL,
	`alteradopor` VARCHAR(30) NULL DEFAULT NULL,
	`codbanco` INT(11) NULL DEFAULT NULL,
	`agencia` VARCHAR(5) NULL DEFAULT NULL,
	`conta` VARCHAR(10) NULL DEFAULT NULL,
	`debitoautomaticoativo` CHAR(1) NULL DEFAULT 'N',
	`clienteiugu` VARCHAR(40) NULL DEFAULT NULL COMMENT 'Id do responsavel no cadastro de clientes no iugu',
	`inscricaoestadual` VARCHAR(20) NULL DEFAULT NULL COMMENT 'Inscrição Estadual da empresa',
	`tokenfirebase` VARCHAR(256) NULL DEFAULT NULL,
	PRIMARY KEY (`codsoc`) USING BTREE
)

-- select da base do cliente: resp academico
select
max(replace(replace(a.cpf_rp ,'.',''),'-','')) as cpf ,
upper(max(a.resp_ped)) as nomeresponsavel,
upper(max(a.profi_rp)) as profissao,
max(a.foner_rp) as telresid,
max(a.fonecelrp) as celular,
max(a.email_rp) as email,
max(a.rg_rp) as rg,
upper(max(coalesce(a.ender_rp,'') || ' ' || coalesce(a.compl_rp,''))) as endereco,
upper(max(a.bairr_rp)) as bairro,
max(a.cidade_rp) as cidade,
max(left(replace(a.cepresp,'-',''),8)) as cep,
max(a.estcivrp) as estadocivil,
max(a.nascresp_p) as dtnascimento,
max('BRASILEIRA') as nacionalidade,
max(current_timestamp) as dtcadastro,
max('MIGRACAO') as cadastradopor
from sigaluno a
where a.cpf_rp is not null
and a.cpf_rp not in ('')
group by a.cpf_rp;

-- select da base do cliente: resp financeiro
select
max(replace(replace(a.cpfresp ,'.',''),'-','')) as cpf ,
upper(max(a.pagtoresp)) as nomeresponsavel,
upper(max(a.profiresp)) as profissao,
max(a.foner_rp) as telresid,
max(a.fonecelrp) as celular,
max(a.emailresp) as email,
max(a.rgresp) as rg,
upper(max(coalesce(a.endresp,'') || ' ' || coalesce(a.compl_rp,''))) as endereco,
upper(max(a.bairesp)) as bairro,
max(a.cidresp) as cidade,
max(left(replace(a.cepresp,'-',''),8)) as cep,
max(a.estcivresp) as estadocivil,
max(a.nascresp_p) as dtnascimento,
max('BRASILEIRA') as nacionalidade,
max(current_timestamp) as dtcadastro,
max('MIGRACAO') as cadastradopor
from sigaluno a
where a.cpfresp is not null
and a.cpfresp not in ('')
group by a.cpfresp;

-- insere os responsaveis
INSERT INTO responsavel (cpf,nomeresponsavel,profissao,telresid,celular,rg,endereco,bairro,cidade,cep,estadocivil,dtnascimento,nacionalidade,dtcadastro,cadastradopor)
SELECT 
cpf,
nomeresponsavel,
left(profissao,40),
if(length(REMOVEcaractere(telresid)) IN (8,10),REMOVEcaractere(telresid),if(length(REMOVEcaractere(celular)) IN (8,10),REMOVEcaractere(celular),NULL) ) AS telresid,
if(length(REMOVEcaractere(celular)) IN (9,11),REMOVEcaractere(celular),if(length(REMOVEcaractere(telresid)) IN (9,11),REMOVEcaractere(telresid),NULL)) AS celular,
rg,
endereco,
bairro,
c.cod_cidade,
cep,
estadocivil,
dtnascimento,
nacionalidade,
dtcadastro,
cadastradopor 
FROM responsavel_siga rs
left JOIN cidade c ON c.nom_cidade = rs.cidade
WHERE LENGTH(cpf) = 11 AND cpf != '00000000000'
GROUP BY cpf;

ALTER TABLE responsavel_siga ADD COLUMN codsoc_unificado INTEGER;
UPDATE responsavel_siga rs JOIN responsavel r ON r.cpf = r.cpf SET rs.codsoc_unificado = r.codsoc;

INSERT INTO responsavel (cpf,nomeresponsavel,celular,telresid,rg,email,endereco,bairro,cep,estadocivil,dtnascimento)
SELECT 
a.cpfpai,
a.pai,
a.celularpai,
a.telpai,
a.rgpai,
a.emailpai,
a.enderecopai,
a.bairropai,
a.ceppai,
a.estadocivilpai,
a.dtnascimentopai
FROM aluno a 
WHERE a.cpfpai NOT IN (SELECT cpf FROM responsavel)
GROUP BY a.cpfpai;

INSERT INTO responsavel (cpf,nomeresponsavel,celular,telresid,rg,email,endereco,bairro,cep,estadocivil,dtnascimento)
SELECT 
a.cpfmae,
a.mae,
a.celularmae,
a.telmae,
a.rgmae,
a.emailmae,
a.enderecomae,
a.bairromae,
a.cepmae,
a.estadocivilmae,
a.dtnascimentomae
FROM aluno a 
WHERE a.cpfmae NOT IN (SELECT cpf FROM responsavel)
GROUP BY a.cpfmae;

INSERT INTO responsavel (cpf,nomeresponsavel,profissao,telresid,celular,rg,endereco,bairro,cidade,cep,estadocivil,dtnascimento,nacionalidade,dtcadastro,cadastradopor)
SELECT 
lpad(cpf,11,'0'),
nomeresponsavel,
left(profissao,40),
if(length(REMOVEcaractere(telresid)) IN (8,10),REMOVEcaractere(telresid),if(length(REMOVEcaractere(celular)) IN (8,10),REMOVEcaractere(celular),NULL) ) AS telresid,
if(length(REMOVEcaractere(celular)) IN (9,11),REMOVEcaractere(celular),if(length(REMOVEcaractere(telresid)) IN (9,11),REMOVEcaractere(telresid),NULL)) AS celular,
rg,
endereco,
bairro,
c.cod_cidade,
cep,
estadocivil,
dtnascimento,
nacionalidade,
dtcadastro,
cadastradopor 
FROM responsavel_siga rs
left JOIN cidade c ON c.nom_cidade = rs.cidade
WHERE LENGTH(cpf) != 11 
and LPAD(cpf,11,'0') NOT IN ('0000000000=','0000000000N','000N CONSTA','00000000102')
AND LPAD(cpf,11,'0') NOT IN (SELECT cpf FROM responsavel)
GROUP BY cpf;


-- MATRICULA
-- select da matricula
select
a.codigo as codmatricula,
(a.unidade || replace(a.prontuario,'-','')) as codaluno,
right(sc.anoletivo,2)||lpad((sc.unidade || sc.curso || sc.serie),6,0)||lpad(t.codturma,2,0) as codconfturma_aux,
(sc.unidade || sc.curso || sc.serie) as codseriecurso,
t.codturma as codturma,
cast(a.datamatric as date) as dtmatricula,
sc.anoletivo as anoletivo,
iif(replace(replace(iif(a.cpfresp='',null, a.cpfresp),'-',''),'.','') = ' ','00000000000',replace(replace(iif(a.cpfresp='',null, a.cpfresp),'-',''),'.','')) as cpfrespfinanceiro,
iif(replace(replace(iif(a.cpf_rp ='',null, cpf_rp),'-',''),'.','') = ' ','00000000000', replace(replace(iif(a.cpf_rp ='',null, cpf_rp),'-',''),'.',''))  as cpfrespacademico,
CASE a.situacao when 'C' then 'I' when 'A' then 'C' when'T' then 'T' when 'F' then 'F'  END as situacao,
'2020-03-17' as dtcadastro,
'1' as cadastradopor,
(sc.unidade || sc.curso) as codcurso,
a.unidade as codunidade,
iif(a.situacao = 'C' or (a.situacao = 'T'),a.datasit,null) as dttransferencia,
a.turno,
'S' as rematricula
from sigaluno a
left join sigclass sc on (sc.anoletivo = a.anoletivo
                       and sc.unidade = a.unidade
                       and sc.curso = a.curso
                       and sc.serie = a.serie
                       and sc.classe = iif(a.classe = '', 'A', a.classe)
                       and sc.turno = a.turno)
left join turma t on t.dscturma = COALESCE(sc.classe || sc.turno,'A')
where t.codturma is not null;

-- ajuste para os codconfturma nulos
UPDATE matricula_siga SET codconfturma_aux = concat(right(anoletivo,2),lpad((codseriecurso),6,0),lpad(codturma,2,0))

UPDATE matricula_siga m
JOIN aluno_siga a ON a.codaluno = m.codaluno
SET m.codaluno = a.codaluno_unificado;

UPDATE matricula_siga m
SET  m.cpfrespfinanceiro = '00000000000'
WHERE m.cpfrespfinanceiro IS NULL;

UPDATE matricula_siga m
SET  m.cpfrespacademico = '00000000000'
WHERE m.cpfrespacademico IS NULL;

ALTER TABLE matricula_siga ADD COLUMN codconfturma INTEGER;
UPDATE matricula_siga m
JOIN configturma_siga2 c ON c.codconfturma = m.codconfturma_aux
SET m.codconfturma = c.codconfturma_aux;

UPDATE matricula_siga m SET m.CPFRESPFINANCEIRO = '00000000000' WHERE m.CPFRESPFINANCEIRO = '0' OR m.CPFRESPFINANCEIRO = 'N' OR m.CPFRESPFINANCEIRO = 'N CONSTA' 
OR m.CPFRESPFINANCEIRO = '000' OR m.CPFRESPFINANCEIRO = '=';
UPDATE matricula_siga m SET m.cpfrespacademico = '00000000000' WHERE m.cpfrespacademico = '0' OR m.cpfrespacademico = 'N' OR m.cpfrespacademico = 'N CONSTA' 
OR m.cpfrespacademico = '000' OR m.cpfrespacademico = '=';

ALTER TABLE matricula_siga ADD COLUMN codaluno_tella INTEGER;
ALTER TABLE matricula_siga ADD COLUMN codconfturma_tella INTEGER;

UPDATE matricula_siga ms JOIN aluno_siga2 a ON a.CODALUNO = ms.CODALUNO SET ms.codaluno_tella = a.codaluno_tella;
UPDATE matricula_siga ms JOIN configturma c ON concat(right(c.anoletivo,2),LPAD(c.codseriecurso,6,0),lpad(c.codturma,2,0))
 = ms.CODCONFTURMA_AUX SET ms.codconfturma_tella = c.codconfturma;

INSERT INTO matricula (codaluno,codconfturma,codseriecurso,codturma,dtmatricula,anoletivo,cpfrespfinanceiro,cpfrespacademico,situacao,dtcadastro,cadastradopor,codcurso,codunidade,dttransferencia,rematricula)

SELECT 
m.codaluno_tella,
m.codconfturma_tella,
m.codseriecurso,
m.codturma,
m.dtmatricula,
m.anoletivo,
LPAD(m.cpfrespfinanceiro,11,'0'),
LPAD(m.cpfrespacademico,11,'0'),
m.situacao,
m.dtcadastro,
m.cadastradopor,
m.codcurso,
m.codunidade,
m.dttransferencia,
m.rematricula
FROM matricula_siga2 m
WHERE codmatricula IS NOT NULL
AND m.codconfturma_tella IS NOT NULL;

-- MAIS ALGUNS AJUSTES
ALTER TABLE matricula_siga2 ADD COLUMN codmatricula_tella INTEGER;

UPDATE matricula_siga2 ms JOIN matricula m ON m.codaluno = ms.codaluno_tella and m.codconfturma = ms.codconfturma_tella
SET ms.codmatricula_tella = m.codmatricula;

-- LOGINS
-- Rodar os scripts abaixo para gerar os logins dos funcionarios, alunos e responsáveis.
UPDATE aluno a SET senha = gerasenha(6);
UPDATE responsavel a SET senha = gerasenha(6);
UPDATE funcionario a SET senha = gerasenha(6); 


-- INSERE OS ALUNOS PENDENTES NA TABELA LOGIN. 
INSERT INTO login (codaluno, login2, perfil, senha)
SELECT a.codaluno, a.codaluno, 'A', IFNULL(a.senha, gerasenha(6)) AS senha
FROM aluno a
WHERE a.codaluno NOT IN (
SELECT L.codaluno AS contador
FROM login L
WHERE L.codaluno = a.codaluno); 

-- insere responsaveis 
INSERT INTO login (login2, perfil, senha, codsoc)
SELECT r.cpf, 'R', IFNULL(r.senha, gerasenha(6)) AS senha, r.codsoc
FROM responsavel r
WHERE r.codsoc NOT IN (
SELECT L.codsoc AS contador
FROM login L
WHERE L.codsoc = r.codsoc); 

-- insere funcionarios 
INSERT INTO login (login2, perfil, senha, funcodigo)
SELECT r.LOGIN, 'P', IFNULL(r.senha, gerasenha(6)) AS senha, r.funcodigo
FROM funcionario r
WHERE r.funcodigo NOT IN (
SELECT L.funcodigo AS contador
FROM login L
WHERE L.funcodigo = r.funcodigo);
UPDATE login, empresa SET login = CAST(CONCAT(login.perfil, SUBSTRING(empresa.codempresatella,3,4),empresa.codunidadetella, IFNULL(IFNULL(login.id,999),999)) AS CHAR);

UPDATE login l JOIN aluno a ON a.codaluno = l.codaluno SET l.senha = a.senha;
UPDATE login l JOIN funcionario f ON f.FUNCODIGO = l.funcodigo SET l.senha = f.SENHA;
UPDATE login l JOIN responsavel r ON r.codsoc = l.codsoc SET l.senha = r.senha;

SELECT * FROM login;




























/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */

                                                                                /* FINANCEIRO*/

/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */

-- CONTAS A RECEBER
-- Rodei o script abaixo no IBexpert, usei a propria ferramenta de exportar insert do IB, lá tem uma opção chamada Create Table, ele monta um código create perfeiro para o script
-- chamei a tabela de contasreceber_siga
 
 -- rodar no banco do cliente
 update sigpagtos s set s.estorno = null where s.estorno = '';
select
        sa.codigo as codrec,
        max(sa.codaluno) as codmatricula,
        max(sa.numero) as parcela,
        max((select Max(iif(pp.parcelas=0,1,pp.parcelas)) from sigparce pp where pp.codigo = sa.tipo)) as totparcela,
        max(sa.valortit) as valorparcela,
        max(sa.vencto1) as vencimento,
        max((sa.valortit - sa.valor1)) as desconto,
        max(0) as carenciadesconto,
        max((sa.vencto1)) as vencimentodesconto,
        max((sa.valortit - sa.valor2)) as desconto2,
        max(0) as carenciadesconto2,
        max(sa.vencto2) as vencimentodesconto2,
        max(sa.multa) as multa,
        max(sa.jurosdia) as juros,
        max(0) as carenciamulta,
        max(coalesce(sa.vencto2, sa.vencto1)) as vencimentomulta,
        max(iif(sp.estorno = 'S', NULL, sp.data)) as dtpagamento,
        max((select sum(spp.valor) from sigpagtos spp where spp.codparcela = sa.codigo and spp.estorno is null)) as valorpago,
        max(sa.unidade) as codunidade,
        max(sa.anoletivo) as anoletivo,
        max((sa.unidade || sa.tipo)) as codreceita,
        max(iif(st.agencia is null, 'N', 'S')) as cobbancaria,
        max((sa.unidade || replace(sa.prontuario,'-',''))) as codaluno,
        max(st.bolnossonumero) as nossonumero,
        max('N') as avulsa,
        max('MIGRACAO2020') as cadastradopor,
        max(current_timestamp) as dtcadastro,
        max(sa.valor1) as valorparcelaoriginal,
        max((sa.valortit - sa.valor1)) as descontooriginal,
        max(sa.vencto1) as vencimentooriginal,
        max(sa.valoreal) as valorbruto,
        max(sa.tipo) as receitaoriginal,
        max(sa.valortit) as valor,
        max(sa.situacao) as statusparcela,
        max(sa.titulo) as codfatura_aux,
        max(st.agencia) as agencia_aux,
        max(st.conta) as conta_aux
from sigalucx sa
left join sigtitulos st on st.codparcela = sa.codigo
left join sigpagtos sp on sp.codparcela = sa.codigo
where sa.anoletivo > 2016
group by sa.codigo;

--Obs: eu fiz por ano pois a querry retornava uma quantidade absurda de registro

-- UPDATE PARA FAZER ALGUNS ACERTOS
ALTER table contasreceber_siga ADD COLUMN codconta INTEGER;

UPDATE contasreceber_siga c
JOIN conta ct ON ct.agencia = c.agencia_aux AND ct.numconta = c.conta_aux
SET c.codconta = ct.codconta;

-- IDENTIFICAR OS CODCONTAS DAS PARCELAS
SELECT c.agencia_aux, c.conta_aux FROM contasreceber_siga c GROUP BY c.agencia_aux, c.conta_aux;

SELECT * FROM conta c WHERE c.agencia = 09366 AND c.numconta = 003656; -- 102 
SELECT * FROM conta c WHERE c.agencia = 09366 AND c.numconta = 003672; -- 104 
SELECT * FROM conta c WHERE c.agencia = 09366 AND c.numconta = 004787; -- 106
SELECT * FROM conta c WHERE c.agencia = 09366 AND c.numconta = 005123; -- 108
SELECT * FROM conta c WHERE c.agencia = 09366 AND c.numconta = 156991; -- 113
SELECT * FROM conta c WHERE c.agencia = 09366 AND c.numconta = 159466; -- 114
SELECT * FROM conta c WHERE c.agencia = 09366 AND c.numconta = 177070; -- 115

-- SCRIPT ADICIONADO PELO WALLAS
###############################################################################
UPDATE mig_contasreceber_siga cr
JOIN conta c ON c.agencia = cr.AGENCIA_AUX AND c.numconta = cr.CONTA_AUX
SET cr.CODCONTA = 102
WHERE c.agencia = 09366 AND c.numconta = 003656;

UPDATE mig_contasreceber_siga cr
JOIN conta c ON c.agencia = cr.AGENCIA_AUX AND c.numconta = cr.CONTA_AUX
SET cr.CODCONTA = 104
WHERE c.agencia = 09366 AND c.numconta = 003672;

UPDATE mig_contasreceber_siga cr
JOIN conta c ON c.agencia = cr.AGENCIA_AUX AND c.numconta = cr.CONTA_AUX
SET cr.CODCONTA = 106
WHERE c.agencia = 09366 AND c.numconta = 004787;

UPDATE mig_contasreceber_siga cr
JOIN conta c ON c.agencia = cr.AGENCIA_AUX AND c.numconta = cr.CONTA_AUX
SET cr.CODCONTA = 108
WHERE c.agencia = 09366 AND c.numconta = 005123;

UPDATE mig_contasreceber_siga cr
JOIN conta c ON c.agencia = cr.AGENCIA_AUX AND c.numconta = cr.CONTA_AUX
SET cr.CODCONTA = 113
WHERE c.agencia = 09366 AND c.numconta = 156991;

UPDATE mig_contasreceber_siga cr
JOIN conta c ON c.agencia = cr.AGENCIA_AUX AND c.numconta = cr.CONTA_AUX
SET cr.CODCONTA = 114
WHERE c.agencia = 09366 AND c.numconta = 159466;

UPDATE mig_contasreceber_siga cr
JOIN conta c ON c.agencia = cr.AGENCIA_AUX AND c.numconta = cr.CONTA_AUX
SET cr.CODCONTA = 115
WHERE c.agencia = 09366 AND c.numconta = 177070;
###############################################################################

-- ACERTAR O CODUNIDADE DAS PARCELAS
UPDATE contasreceber_siga c
JOIN conta ct ON ct.codconta = c.codconta
SET c.codunidade = ct.codempresa;

-- SE A CONTA FOR ITAU UTILIZAR ESSE UPDATE
UPDATE contasreceber_siga c
SET c.nossonumero = RIGHT(c.nossonumero,10)
WHERE c.codconta IN (SELECT codconta FROM conta WHERE codbanco = 341);

-- SETANDO O CPF DO RESPONSAVEL FINANCEIRO NA TABELA DE CONTASRECEBER_SIGA
ALTER TABLE contasreceber_siga ADD COLUMN cpf VARCHAR(14);

UPDATE contasreceber_siga c
JOIN matricula_siga2 m ON m.codmatricula = c.codmatricula
SET c.cpf = m.cpfrespfinanceiro;

-- INSERINDO NO CONTAS RECEBER DO ESCOLAWEB
INSERT INTO contasreceber (codrec, 
									codmatricula, 
									parcela, 
									totparcela, 
									valorparcela, 
									vencimento, 
									desconto, 
									carenciadesconto, 
									vencimentodesconto, 
									desconto2, 
									carenciadesconto2, 
									vencimentodesconto2,
									multa, 
									juros, 
									carenciamulta, 
									vencimentomulta, 
									dtpagamento, 
									valorpago,
									codunidade, 
									anoletivo, 
									codreceita,
									cobbancaria, 
									codaluno, 
									nossonumero, 
									avulsa, 
									cadastradopor, 
									dtcadastro,
									valorparcelaoriginal, 
									descontooriginal, 
									vencimentooriginal,
									valorbruto, 
									receitaoriginal,
									valor,
									statusparcela,
									codconta, 
									cpf,
									descricao)
(SELECT 
									c.codrec, 
									ms.codmatricula_tella, 
									c.parcela, 
									c.totparcela, 
									c.valorparcela, 
									c.vencimento, 
									c.desconto, 
									c.carenciadesconto, 
									c.vencimentodesconto, 
									c.desconto2, 
									c.carenciadesconto2, 
									c.vencimentodesconto2,
									c.multa, 
									c.juros, 
									c.carenciamulta, 
									c.vencimentomulta, 
									c.dtpagamento, 
									c.valorpago,
									c.codunidade, 
									c.anoletivo, 
									cr.codreceita_tella,
									c.cobbancaria, 
									ms.CODALUNO, 
									c.nossonumero, 
									c.avulsa, 
									c.cadastradopor, 
									c.dtcadastro,
									c.valorparcelaoriginal, 
									c.descontooriginal, 
									c.vencimentooriginal,
									c.valorbruto, 
									cr.codreceita_tella,
									c.valor,
									c.statusparcela,
									c.codconta, 
									c.cpf,
									cr.dscreceita	
 FROM contasreceber_siga c
 LEFT JOIN centroreceita_siga cr ON cr.codreceita = c.CODRECEITA
 JOIN matricula_siga3 ms ON ms.CODMATRICULA = c.CODMATRICULA);

--*************************************AGRUPANDO PARCELAS *********************************************

-------------------- 1º criar uma tabela para receber os registros que virarão fautas 
CREATE TABLE `fatura_AUX` (
	`codrec` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`codmatricula` INT(10) UNSIGNED NULL DEFAULT NULL,
	`parcela` INT(10) UNSIGNED NULL DEFAULT NULL,
	`totparcela` INT(10) UNSIGNED NULL DEFAULT NULL,
	`valorparcela` DECIMAL(17,2) NULL DEFAULT NULL,
	`vencimento` DATE NULL DEFAULT NULL,
	`desconto` DECIMAL(10,2) NULL DEFAULT NULL,
	`carenciadesconto` INT(11) NULL DEFAULT NULL,
	`vencimentodesconto` DATE NULL DEFAULT NULL,
	`desconto2` DECIMAL(10,2) NULL DEFAULT NULL,
	`carenciadesconto2` INT(11) NULL DEFAULT NULL,
	`vencimentodesconto2` DATE NULL DEFAULT NULL,
	`desconto3` DECIMAL(10,2) NULL DEFAULT NULL,
	`carenciadesconto3` INT(11) NULL DEFAULT NULL,
	`vencimentodesconto3` DATE NULL DEFAULT NULL,
	`multa` DECIMAL(10,2) UNSIGNED NULL DEFAULT NULL,
	`juros` DECIMAL(10,2) UNSIGNED NULL DEFAULT NULL,
	`carenciamulta` INT(11) NULL DEFAULT NULL,
	`vencimentomulta` DATE NULL DEFAULT NULL,
	`dtpagamento` DATE NULL DEFAULT NULL,
	`valorpago` DECIMAL(10,2) UNSIGNED NULL DEFAULT NULL,
	`recebidopor` VARCHAR(30) NULL DEFAULT NULL,
	`codunidade` INT(11) NULL DEFAULT NULL,
	`anoletivo` INT(11) NOT NULL,
	`codreceita` INT(11) NULL DEFAULT NULL,
	`codcontapago` INT(11) NULL DEFAULT NULL,
	`codaluno` INT(11) NOT NULL,
	`nossonumero` VARCHAR(20) NULL DEFAULT NULL,
	`linha` VARCHAR(60) NULL DEFAULT NULL,
	`barra` VARCHAR(50) NULL DEFAULT NULL,
	`cobbancaria` CHAR(1) NULL DEFAULT NULL,
	`codconta` INT(11) NULL DEFAULT NULL,
	`incluidotabrps` CHAR(1) NULL DEFAULT NULL,
	`codliquidacao` INT(11) NULL DEFAULT NULL,
	`avulsa` CHAR(1) NULL DEFAULT NULL,
	`cadastradopor` VARCHAR(30) NULL DEFAULT NULL,
	`dtcadastro` DATETIME NULL DEFAULT NULL,
	`valorparcelaoriginal` DECIMAL(17,2) NULL DEFAULT NULL,
	`descontooriginal` DECIMAL(17,2) NULL DEFAULT NULL,
	`vencimentooriginal` DATE NULL DEFAULT NULL,
	`enviaremessa` CHAR(1) NULL DEFAULT 'N',
	`codremessa` INT(11) NULL DEFAULT NULL,
	`cobregistradabanco` CHAR(1) NULL DEFAULT NULL COMMENT 'confirmação do registro da cobranca no banco',
	`alteradopor` VARCHAR(30) NULL DEFAULT NULL,
	`codnegociacao` INT(11) NULL DEFAULT NULL,
	`valornegociado` DECIMAL(10,2) NULL DEFAULT NULL,
	`abatimento` DECIMAL(10,2) NULL DEFAULT NULL,
	`valorbruto` DECIMAL(10,2) NULL DEFAULT NULL,
	`codcontrato` INT(11) NULL DEFAULT NULL,
	`codfornecedor` INT(11) NULL DEFAULT NULL,
	`outrasreceitas` CHAR(1) NULL DEFAULT NULL,
	`descricao` VARCHAR(100) NULL DEFAULT NULL,
	`codrecfatura` INT(11) NULL DEFAULT NULL,
	`codlotefatura` INT(11) NULL DEFAULT NULL,
	`cpf` VARCHAR(14) NULL DEFAULT NULL,
	`vencfatura` DATETIME NULL DEFAULT NULL,
	`vencfaturadesc` DATETIME NULL DEFAULT NULL,
	`vencfaturadesc2` DATETIME NULL DEFAULT NULL,
	`vencfaturamulta` DATETIME NULL DEFAULT NULL,
	`receitaoriginal` INT(11) NULL DEFAULT NULL,
	`codinscricaoevento` INT(11) NULL DEFAULT NULL,
	`codremessaalteracao` INT(11) NULL DEFAULT NULL,
	`faturaunica` CHAR(1) NULL DEFAULT NULL COMMENT 'S=SE PARCELA FOR UMA FATURA SEM FILHOS',
	`outrosacrescimos` DECIMAL(10,2) NULL DEFAULT NULL,
	`outrosabatimentos` DECIMAL(10,2) NULL DEFAULT NULL,
	`valor` DECIMAL(17,2) NULL DEFAULT NULL,
	`codsolicitacao` INT(11) NULL DEFAULT NULL,
	`carenciadesconto1original` INT(11) NULL DEFAULT NULL,
	`carenciadesconto2original` INT(11) NULL DEFAULT NULL,
	`carenciamultaoriginal` INT(11) NULL DEFAULT NULL,
	`registroconfirmado` CHAR(1) NULL DEFAULT NULL,
	`carenciadesconto3original` INT(11) NULL DEFAULT NULL,
	`idfaturaiugu` VARCHAR(100) NULL DEFAULT NULL,
	`statusparcela` VARCHAR(20) NULL DEFAULT NULL,
	`parcelaremessasicoob` SMALLINT(4) NULL DEFAULT '1',
	`tipocobrancaiugu` CHAR(1) NULL DEFAULT 'B' COMMENT 'A=CARTAO E BOLETO; B=SOMENTE BOLETO; C-SOMENTE CARTAO',
	`tipocobrancaiuguoriginal` CHAR(1) NULL DEFAULT NULL,
	`dtcompetencia` DATE NULL DEFAULT NULL,
	`nossonumeroantigo_migracao` VARCHAR(50) NULL DEFAULT NULL,
	`tipo_parcela` VARCHAR(50) NULL DEFAULT NULL,
	PRIMARY KEY (`codrec`)
	)
COLLATE='latin1_swedish_ci'
ENGINE=InnoDB
AUTO_INCREMENT=112233
;


----------------------- 2º localizar os registros que virarão faturas e inserir eles na tabela acima
SELECT * FROM contasreceber cr WHERE cr.nossonumero IN (
SELECT c.nossonumero FROM contasreceber c
WHERE c.dtpagamento IS NULL 
AND c.nossonumero IS NOT NULL
GROUP BY c.nossonumero HAVING COUNT(1)> 1 );

INSERT INTO fatura_aux    (codrec, 
									codmatricula, 
									parcela, 
									totparcela, 
									valorparcela, 
									vencimento, 
									desconto, 
									carenciadesconto, 
									vencimentodesconto, 
									desconto2, 
									carenciadesconto2, 
									vencimentodesconto2,
									multa, 
									juros, 
									carenciamulta, 
									vencimentomulta, 
									dtpagamento, 
									valorpago,
									codunidade, 
									anoletivo, 
									codreceita,
									cobbancaria, 
									codaluno, 
									nossonumero, 
									avulsa, 
									cadastradopor, 
									dtcadastro,
									valorparcelaoriginal, 
									descontooriginal, 
									vencimentooriginal,
									valorbruto, 
									receitaoriginal,
									valor,
									statusparcela,
									codconta, 
									cpf,
									descricao)
(SELECT 
									c.codrec, 
									c.codmatricula, 
									c.parcela, 
									c.totparcela, 
									c.valorparcela, 
									c.vencimento, 
									c.desconto, 
									c.carenciadesconto, 
									c.vencimentodesconto, 
									c.desconto2, 
									c.carenciadesconto2, 
									c.vencimentodesconto2,
									c.multa, 
									c.juros, 
									c.carenciamulta, 
									c.vencimentomulta, 
									c.dtpagamento, 
									c.valorpago,
									c.codunidade, 
									c.anoletivo, 
									c.codreceita,
									c.cobbancaria, 
									C.CODALUNO, 
									c.nossonumero, 
									c.avulsa, 
									c.cadastradopor, 
									c.dtcadastro,
									c.valorparcelaoriginal, 
									c.descontooriginal, 
									c.vencimentooriginal,
									c.valorbruto, 
									c.codreceita,
									c.valor,
									c.statusparcela,
									c.codconta, 
									c.cpf,
									c.descricao	
FROM contasreceber c WHERE c.nossonumero IN (
SELECT cr.nossonumero FROM contasreceber cr
WHERE cr.dtpagamento IS NULL 
AND cr.nossonumero IS NOT NULL
GROUP BY cr.nossonumero HAVING COUNT(1)> 1));

----------------------- 3º apagar as parcelas do contasreceber

SELECT * FROM contasreceber c WHERE c.nossonumero IN (
SELECT cr.nossonumero FROM contasreceber cr
WHERE cr.dtpagamento IS NULL 
AND cr.nossonumero IS NOT NULL
GROUP BY cr.nossonumero HAVING COUNT(1)> 1);

---------------------- 4º inserir as parcelas filhas 
ALTER TABLE contasreceber ADD COLUMN nossonumeroantigo_migracao VARCHAR(50);
ALTER TABLE contasreceber ADD COLUMN tipoparcela_migracao VARCHAR(50);

INSERT INTO contasreceber(
   codrec
  ,codmatricula
  ,parcela
  ,totparcela
  ,valorparcela
  ,vencimento
  ,desconto
  ,carenciadesconto
  ,vencimentodesconto
  ,desconto2
  ,carenciadesconto2
  ,vencimentodesconto2
  ,desconto3
  ,carenciadesconto3
  ,vencimentodesconto3
  ,multa
  ,juros
  ,carenciamulta
  ,vencimentomulta
  ,dtpagamento
  ,valorpago
  ,recebidopor
  ,codunidade
  ,anoletivo
  ,codreceita
  ,codcontapago
  ,codaluno
  ,nossonumero
  ,linha
  ,barra
  ,cobbancaria
  ,codconta
  ,incluidotabrps
  ,codliquidacao
  ,avulsa
  ,cadastradopor
  ,dtcadastro
  ,valorparcelaoriginal
  ,descontooriginal
  ,vencimentooriginal
  ,enviaremessa
  ,codremessa
  ,cobregistradabanco
  ,alteradopor
  ,codnegociacao
  ,valornegociado
  ,abatimento
  ,valorbruto
  ,codcontrato
  ,codfornecedor
  ,outrasreceitas
  ,descricao
  ,codlotefatura
  ,cpf
  ,vencfatura
  ,vencfaturadesc
  ,vencfaturadesc2
  ,vencfaturamulta
  ,receitaoriginal
  ,codinscricaoevento
  ,codremessaalteracao
  ,faturaunica
  ,outrosacrescimos
  ,outrosabatimentos
  ,valor
  ,codsolicitacao
  ,carenciadesconto1original
  ,carenciadesconto2original
  ,carenciamultaoriginal
  ,registroconfirmado
  ,carenciadesconto3original
  ,idfaturaiugu
  ,statusparcela
  ,parcelaremessasicoob
  ,tipocobrancaiugu
  ,tipocobrancaiuguoriginal
  ,dtcompetencia
  ,nossonumeroantigo_migracao
   ,tipoparcela_migracao
)
SELECT codrec, codmatricula, parcela, totparcela, valorparcela, vencimento, desconto, carenciadesconto, vencimentodesconto, desconto2, carenciadesconto2, vencimentodesconto2, desconto3, carenciadesconto3, vencimentodesconto3, multa, juros, carenciamulta, vencimentomulta, dtpagamento, valorpago, recebidopor, codunidade, anoletivo, codreceita, codcontapago, codaluno, '-1', linha, barra, 'N', codconta, incluidotabrps, codliquidacao, avulsa, cadastradopor, dtcadastro, valorparcelaoriginal, descontooriginal, vencimentooriginal, enviaremessa, codremessa, cobregistradabanco, alteradopor, codnegociacao, valornegociado, abatimento, valorbruto, codcontrato, codfornecedor, outrasreceitas, descricao, 
 1, cpf, vencimento, vencimentodesconto, vencimentodesconto2, vencimentomulta, receitaoriginal, codinscricaoevento, codremessaalteracao, 'N', outrosacrescimos, outrosabatimentos, valor, codsolicitacao, carenciadesconto1original, carenciadesconto2original, carenciamultaoriginal, registroconfirmado, carenciadesconto3original, idfaturaiugu, statusparcela,  parcelaremessasicoob, tipocobrancaiugu, tipocobrancaiuguoriginal, dtcompetencia, nossonumero AS 'nossonumeroantigo_migracao', 'FILHA' 
FROM fatura_AUX  where dtpagamento is NULL;

--------------------------- 5º inserir os pais 
INSERT INTO contasreceber(
  codmatricula
  ,parcela
  ,totparcela
  ,valorparcela
  ,vencimento
  ,desconto
  ,carenciadesconto
  ,vencimentodesconto
  ,desconto2
  ,carenciadesconto2
  ,vencimentodesconto2
  ,desconto3
  ,carenciadesconto3
  ,vencimentodesconto3
  ,multa
  ,juros
  ,carenciamulta
  ,vencimentomulta
  ,dtpagamento
  ,valorpago
  ,recebidopor
  ,codunidade
  ,anoletivo
  ,codreceita
  ,codcontapago
  ,codaluno
  ,nossonumero
  ,linha
  ,barra
  ,cobbancaria
  ,codconta
  ,incluidotabrps
  ,codliquidacao
  ,avulsa
  ,cadastradopor
  ,dtcadastro
  ,valorparcelaoriginal
  ,descontooriginal
  ,vencimentooriginal
  ,enviaremessa
  ,codremessa
  ,cobregistradabanco
  ,alteradopor
  ,codnegociacao
  ,valornegociado
  ,abatimento
  ,valorbruto
  ,codcontrato
  ,codfornecedor
  ,outrasreceitas
  ,descricao
  ,codrecfatura
  ,codlotefatura
  ,cpf
  ,vencfatura
  ,vencfaturadesc
  ,vencfaturadesc2
  ,vencfaturamulta
  ,receitaoriginal
  ,codinscricaoevento
  ,codremessaalteracao
  ,faturaunica
  ,outrosacrescimos
  ,outrosabatimentos
  ,valor
  ,codsolicitacao
  ,carenciadesconto1original
  ,carenciadesconto2original
  ,carenciamultaoriginal
  ,registroconfirmado
  ,carenciadesconto3original
  ,idfaturaiugu
  ,statusparcela
  ,parcelaremessasicoob
  ,tipocobrancaiugu
  ,tipocobrancaiuguoriginal
  ,dtcompetencia
  ,nossonumeroantigo_migracao
  ,tipoparcela_migracao
)
SELECT NULL, parcela, totparcela, sum(ifnull(valorparcela,0)), vencimento, sum(ifnull(desconto,0)), carenciadesconto, vencimentodesconto, desconto2, carenciadesconto2, vencimentodesconto2, desconto3, carenciadesconto3, vencimentodesconto3, multa, juros, carenciamulta, vencimentomulta, dtpagamento, valorpago, recebidopor, codunidade, anoletivo, codreceita, codcontapago, 0, nossonumero, linha, barra, cobbancaria, codconta, incluidotabrps, codliquidacao, avulsa, cadastradopor, dtcadastro,sum(ifnull(valorparcelaoriginal,0)), sum(ifnull(descontooriginal,0)), vencimentooriginal, enviaremessa, codremessa, cobregistradabanco, alteradopor, codnegociacao, valornegociado, abatimento, sum(ifnull(valorbruto,0)), codcontrato, codfornecedor, outrasreceitas, CONCAT('FATURA',' ',month(vencimento),'/',YEAR(vencimento)), 
NULL , 1, cpf, vencimento, vencimentodesconto, vencimentodesconto2, vencimentomulta, receitaoriginal, codinscricaoevento, codremessaalteracao, 'N', outrosacrescimos, outrosabatimentos, sum(ifnull(valorparcela,0)) , codsolicitacao, carenciadesconto1original, carenciadesconto2original, carenciamultaoriginal, registroconfirmado, carenciadesconto3original, idfaturaiugu, statusparcela,  parcelaremessasicoob, tipocobrancaiugu, tipocobrancaiuguoriginal, dtcompetencia, nossonumero AS  'nossonumeroantigo_migracao',  'PAI' 
FROM fatura_AUX  where dtpagamento is null GROUP BY nossonumero ;

------------------------------------------- 6º PASSO : INSERIR OS REGISTROS NA TABELA FATURA 

-> inserir o lote 6 na tabela 'faturalote'

SELECT * FROM faturalote;

INSERT INTO fatura (codrecfatura, codrecfilho, codlote, dtcadastro, cadastradopor)
select
cc.codrec  as 'codrecfatura',
c.codrec,
1 as codlote,
now(),
'MIGRACAO'
from contasreceber c 
JOIN contasreceber cc ON cc.nossonumeroantigo_migracao  = c.nossonumeroantigo_migracao and cc.tipoparcela_migracao = 'PAI'
where c.tipoparcela_migracao = 'FILHA';

--------------------------------------------- 7º PASSO :setar o codrec fatura no contasreceber
UPDATE contasreceber c
JOIN fatura f ON (c.codrec = f.codrecfilho)
SET c.codrecfatura = f.codrecfatura
WHERE c.tipoparcela_migracao = 'FILHA';




-- INSERINDO AS LIQUIDACOES
 
 INSERT INTO liquidacaoboleto(codreceita, valorpago, dtpagamento, codaluno, anoletivo, parcela, conta, vencimento, nossonumero, valorapagar, desconto, valorparcela, dtprocessamento, codrec, usuario)
(SELECT c.codreceita, c.valorpago, c.dtpagamento, c.codaluno, c.anoletivo, c.parcela, c.codconta, c.vencimento, c.nossonumero, c.valorparcela, (c.desconto) as desconto, c.valorparcela, c.dtpagamento, c.codrec, 'MIGRACAO2019'
 FROM contasreceber c
 WHERE c.dtpagamento IS NOT NULL);
 
-- INSERINDO O PAGAMENTO E O PAGAMENTO FORMA
 
-- PAGAMENTO 
INSERT INTO pagamento(codpagamento ,dtpagamento ,dtregistro ,registradopor ,codunidaderec ,totalpago ,obs ,codaluno ,cpfresp) SELECT l.codliquidacao AS codpagamento, l.dtpagamento, l.dtprocessamento , l.usuario, c.codunidade, l.valorpago, l.obspagamento, l.codaluno, c.cpf FROM liquidacaoboleto l JOIN contasreceber c ON c.codrec = l.codrec WHERE l.codpagamento IS NULL; 
-- PAGAMENTO FORMA -- PRA INSERIR PAGAMENTOFORMA É NECESSARIO INFORMAR UMA CONTA DE PAGAMENTO. ASSIM RESOLVI CRIAR UMA CONTA INATIVA CHAMADA MIGRACAO. 
INSERT INTO pagamentoforma(codpagamento ,codconta ,valorpago ,codmeiopagamentoitem ,codforma) 
SELECT l.codliquidacao AS codpagamento, 1 AS codconta, l.valorpago, NULL, (CASE l.formapagamento WHEN 'DI' THEN 1 WHEN 'LI' THEN 2 WHEN 'CD' THEN 4 WHEN 'CC' THEN 5 ELSE 2 END) AS codforma 
FROM liquidacaoboleto l JOIN contasreceber c ON c.codrec = l.codrec WHERE l.codpagamento IS NULL; 

-- SETAR CODPAGAMENTO NA LIQUIDACAOBOLETO 
UPDATE liquidacaoboleto l SET l.codpagamento = l.codliquidacao WHERE l.codpagamento IS NULL;

















/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */

                                                                                /* ACADEMICO*/

/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */


-- ACUMULADO
-- Rodar select na base do cliente, exportar dados com o codigo create table, inserir no acumulado_siga no escolaweb
-- Nessa escola existia apenas notas conceituais (A,B,C,D)

select
sa.codigo as matricula,
n.disciplina as codmateria,
n.unidade as codunidade,
n.anoletivo as ano,
t.codturma,
(n.unidade || n.curso || n.serie) as codserie,
n.mediabim1 as soma1,
n.recupbim1 as rec1,
n.resultbim1 as media1,
n.faltabim1 as faltas1,
n.mediabim2 as soma2,
n.recupbim2 as rec2,
n.resultbim2 as media2,
n.faltabim2 as faltas2,
n.mediabim3 as soma3,
n.recupbim3 as rec3,
n.resultbim3 as media3,
n.faltabim3 as faltas3,
(n.faltabim1+n.faltabim2+n.faltabim3) as totfaltas,
n.mediabim as somaetapas,
n.notarecup as recfinal,
n.mediafinal as resultfinal,
'MIGRACAO2020' as usuario
from signotfa n
join sigaluno sa on sa.prontuario = n.prontuario and sa.anoletivo = n.anoletivo
join sigclass sc on (sc.anoletivo = sa.anoletivo
                        and sc.unidade = sa.unidade
                        and sc.curso = sa.curso
                        and sc.serie = sa.serie
                        and sc.classe = iif(sa.classe = '', 'A', sa.classe)
                        and sc.turno = sa.turno)
join turma t on t.dscturma = (COALESCE(sc.classe,'X') || COALESCE(sc.turno,'Y'));

CREATE TABLE ACUMULADO_SIGA (
	codacumulado integer auto_increment not null primary key,
    MATRICULA INTEGER,
    CODMATERIA VARCHAR(3),
    CODUNIDADE VARCHAR(2),
    ANO VARCHAR(4),
    CODTURMA INTEGER,
    CODSERIE VARCHAR(6),
    SOMA1 VARCHAR(7),
    REC1 VARCHAR(7),
    MEDIA1 VARCHAR(7),
    FALTAS1 INTEGER,
    SOMA2 VARCHAR(7),
    REC2 VARCHAR(7),
    MEDIA2 VARCHAR(7),
    FALTAS2 INTEGER,
    SOMA3 VARCHAR(7),
    REC3 VARCHAR(7),
    MEDIA3 VARCHAR(7),
    FALTAS3 INTEGER,
    TOTFALTAS INTEGER,
    SOMAETAPAS VARCHAR(5),
    RECFINAL VARCHAR(5),
    RESULTFINAL VARCHAR(5),
    USUARIO CHAR(12),
	 codmatricula_tella INTEGER,
	 coddisciplina_tella INTEGER);


-- colunas auxiliares que receberão os conceitos
ALTER TABLE acumulado ADD COLUMN conceito1 CHAR(1);
ALTER TABLE acumulado ADD COLUMN conceito2 CHAR(1);
ALTER TABLE acumulado ADD COLUMN conceito3 CHAR(1);
ALTER TABLE acumulado ADD COLUMN conceitoFinal CHAR(1);



-- INSERT ACUMULADO_SIGA PARA A TABELA ACUMULADO

ALTER TABLE acumulado_siga ADD COLUMN codmatricula_tella INTEGER;
ALTER TABLE acumulado_siga ADD COLUMN coddisciplina_tella INTEGER;

UPDATE acumulado_siga a 
JOIN matricula_siga3 m ON a.MATRICULA = m.CODMATRICULA 
SET a.codmatricula_tella = m.codmatricula_tella;

UPDATE acumulado_siga a
JOIN disciplina_siga d ON d.CODIGO = a.CODMATERIA AND d.CODUNIDADE = a.CODUNIDADE
SET a.coddisciplina_tella = d.coddisciplina_tella;


INSERT INTO acumulado (
      codacumulado,
		MATRICULA,
		CODMATERIA,
		ANO,
		CODSERIE,
		CODTURMA,
		FALTAS1,
		FALTAS2,
		FALTAS3,
		TOTFALTAS,
		conceito1,
		conceito2,
		conceito3,
		conceitofinal
		)
SELECT
      a.codacumulado,
		a.codmatricula_tella,
		a.coddisciplina_tella,
		a.ANO,
		a.codserie,
		a.CODTURMA,
		a.FALTAS1,
		a.FALTAS2,
		a.FALTAS3,
		a.TOTFALTAS,
		a.MEDIA1,
		a.MEDIA2,
		a.MEDIA3,
		a.RESULTFINAL
FROM acumulado_siga a;

UPDATE acumulado a 
SET a.TOTFALTAS = IFNULL(a.FALTAS1, 0.0) + IFNULL(a.FALTAS2, 0.0) + IFNULL(a.FALTAS3, 0.0);

UPDATE acumulado a
JOIN matricula m ON m.codmatricula = a.MATRICULA
SET a.CODSERIE = m.codseriecurso;


-- INSERT PROFESSORTURMA

insert into professorturma (  CODSERIE  ,CODTURMA  ,ANO  ,CODMATERIA, CODMATERIAGRADE ,reprova  ,codconfturma  ,reprovafalta  ,percentualnota )
(select a.codserie, a.CODTURMA, a.Ano, a.codmateria, a.codmateria AS codmateriagrade,'S' AS reprova, c.codconfturma ,'N' AS reprovafalta, 1 AS percentualnota  
from acumulado a
join matricula m on m.codmatricula = a.MATRICULA
join configturma c on c.codconfturma = m.codconfturma
where a.codserie = c.codseriecurso 
group by c.codconfturma, a.codmateria)

-- SETAR CODPROFTURMA NO ACUMULADO

update acumulado a join professorturma p on a.codserie = p.codserie and a.ano = p.ano and a.codturma = p.codturma and a.codmateria = p.codmateria
set a.codprofturma = p.codprofturma
where  a.codprofturma is null;


-- Transformando os conceitos em notas, nesse caso a escola que passou as notas de referencia para os conceitos
-- MEDIA 1
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.MEDIA1 = case
	when aa.MEDIA1 = 'A' then 10
	when aa.MEDIA1 = 'B' then 8
	when aa.MEDIA1 = 'C' then 5
	when aa.MEDIA1 = 'D' then 3
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- MEDIA 2
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.MEDIA2 = case
	when aa.MEDIA2 = 'A' then 10
	when aa.MEDIA2 = 'B' then 8
	when aa.MEDIA2 = 'C' then 5
	when aa.MEDIA2 = 'D' then 3
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- MEDIA 3
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.MEDIA3 = case
	when aa.MEDIA3 = 'A' then 10
	when aa.MEDIA3 = 'B' then 8
	when aa.MEDIA3 = 'C' then 5
	when aa.MEDIA3 = 'D' then 3
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- SOMA 1
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.soma1 = case
	when aa.soma1 = 'A' then 10
	when aa.soma1 = 'B' then 8
	when aa.soma1 = 'C' then 5
	when aa.soma1 = 'D' then 3
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- SOMA 2
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.soma2 = case
	when aa.soma2 = 'A' then 10
	when aa.soma2 = 'B' then 8
	when aa.soma2 = 'C' then 5
	when aa.soma2 = 'D' then 3
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- SOMA 3
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.soma3 = case
	when aa.soma3 = 'A' then 10
	when aa.soma3 = 'B' then 8
	when aa.soma3 = 'C' then 5
	when aa.soma3 = 'D' then 3
	ELSE NULL
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- REC 1
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.rec1 = case
	when aa.rec1 = 'A' then 10
	when aa.rec1 = 'B' then 8
	when aa.rec1 = 'C' then 5
	when aa.rec1 = 'D' then 3
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- REC 2
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.rec2 = case
	when aa.rec2 = 'A' then 10
	when aa.rec2 = 'B' then 8
	when aa.rec2 = 'C' then 5
	when aa.rec2 = 'D' then 3
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- REC 3
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.rec3 = case
	when aa.rec3 = 'A' then 10
	when aa.rec3 = 'B' then 8
	when aa.rec3 = 'C' then 5
	when aa.rec3 = 'D' then 3
	ELSE NULL
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- REC FINAL
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.RECFINAL = case
	when aa.RECFINAL = 'A' then 10
	when aa.RECFINAL = 'B' then 8
	when aa.RECFINAL = 'C' then 5
	when aa.RECFINAL = 'D' then 3
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- RESULT FINAL
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.RESULTFINAL = case
	when aa.RESULTFINAL = 'A' then 10
	when aa.RESULTFINAL = 'B' then 8
	when aa.RESULTFINAL = 'C' then 5
	when aa.RESULTFINAL = 'D' then 3
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2901);

-- Transformando os conceitos em notas, nesse caso a escola que passou as notas de referencia para os conceitos
-- MEDIA 1
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.MEDIA1 = case
	when aa.MEDIA1 = 'A' then 4
	when aa.MEDIA1 = 'B' then 3
	when aa.MEDIA1 = 'C' then 2
	when aa.MEDIA1 = 'D' then 1
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);

-- MEDIA 2
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.MEDIA2 = case
	when aa.MEDIA2 = 'A' then 4
	when aa.MEDIA2 = 'B' then 3
	when aa.MEDIA2 = 'C' then 2
	when aa.MEDIA2 = 'D' then 1
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);

-- MEDIA 3
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.MEDIA3 = case
	when aa.MEDIA3 = 'A' then 4
	when aa.MEDIA3 = 'B' then 3
	when aa.MEDIA3 = 'C' then 2
	when aa.MEDIA3 = 'D' then 1
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);

-- SOMA 1
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.soma1 = case
	when aa.soma1 = 'A' then 4
	when aa.soma1 = 'B' then 3
	when aa.soma1 = 'C' then 2
	when aa.soma1 = 'D' then 1
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);

-- SOMA 2
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.soma2 = case
	when aa.soma2 = 'A' then 4
	when aa.soma2 = 'B' then 3
	when aa.soma2 = 'C' then 2
	when aa.soma2 = 'D' then 1
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);

-- SOMA 3
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.soma3 = case
	when aa.soma3 = 'A' then 4
	when aa.soma3 = 'B' then 3
	when aa.soma3 = 'C' then 2
	when aa.soma3 = 'D' then 1
	ELSE NULL
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);

-- REC 1
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.rec1 = case
	when aa.rec1 = 'A' then 4
	when aa.rec1 = 'B' then 3
	when aa.rec1 = 'C' then 2
	when aa.rec1 = 'D' then 1
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);

-- REC 2
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.rec2 = case
	when aa.rec2 = 'A' then 4
	when aa.rec2 = 'B' then 3
	when aa.rec2 = 'C' then 2
	when aa.rec2 = 'D' then 1
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);

-- REC 3
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.rec3 = case
	when aa.rec3 = 'A' then 4
	when aa.rec3 = 'B' then 3
	when aa.rec3 = 'C' then 2
	when aa.rec3 = 'D' then 1
	ELSE NULL
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);

-- REC FINAL
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.RECFINAL = case
	when aa.RECFINAL = 'A' then 4
	when aa.RECFINAL = 'B' then 3
	when aa.RECFINAL = 'C' then 2
	when aa.RECFINAL = 'D' then 1
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);

-- RESULT FINAL
UPDATE acumulado a 
JOIN acumulado_siga aa ON a.CODACUMULADO = aa.codacumulado
SET a.RESULTFINAL = case
	when aa.RESULTFINAL = 'A' then 4
	when aa.RESULTFINAL = 'B' then 3
	when aa.RESULTFINAL = 'C' then 2
	when aa.RESULTFINAL = 'D' then 1
	ELSE null
	END 
WHERE a.codserie IN (SELECT codseriecurso FROM seriecurso WHERE codcurso = 2902);


-- INSERT DE ACUMULADO PARA ACUMULADOAGRUPADO

INSERT INTO acumuladoagrupado(matricula,anoletivo,codseriecurso,codturma,codmateriagrade,media1,faltas1,rec1,media2,
faltas2,rec2,media3,faltas3,rec3,media4,faltas4,rec4,resultadofinal,TOTFALTAS,soma1,soma2,soma3,soma4,somaetapas, conceito1, conceito2, conceito3, conceitofinal)
(SELECT a.matricula,
a.ano,
a.codserie,
a.codturma,
a.codmateria,
a.media1,
a.faltas1,
a.rec1,
a.media2,
a.faltas2,
a.rec2,
a.media3,
a.faltas3,
a.rec3,
a.media4,
a.faltas4,
a.rec4,
a.resultfinal,
a.TOTFALTAS,
a.soma1,
a.soma2,
a.soma3,
a.soma4,
a.somaetapas,
a.conceito1,
a.conceito2,
a.conceito3,
a.conceitofinal
FROM acumulado a where a.MATRICULA IS not null)

-- RODAR UPDATE ABAIXO

UPDATE acumuladoagrupado a
JOIN matricula m ON m.codmatricula = a.matricula
SET a.codaluno = m.codaluno
WHERE a.codaluno IS NULL;

-- INSERINDO TURMA DIARIO
insert into turmadiario (codmatricula, codaluno, anoletivo, codseriecurso, codturma, codconfturma, dtsaida)
(select m.codmatricula, m.codaluno, m.anoletivo, m.codseriecurso, m.codturma, m.codconfturma, m.dtTransferencia
from matricula m );

-- INSERIR REGISTROS NA TABELA CHAMADAAULA

INSERT INTO chamadaaula(codserie, codturma, codmateria, matricula, codano, codetapa, codprofturma) (
SELECT a.CODSERIE, a.CODTURMA, a.CODMATERIA, a.MATRICULA, a.ANO,1, a.codprofturma
FROM acumulado a);

INSERT INTO chamadaaula(codserie, codturma, codmateria, matricula, codano, codetapa, codprofturma) (
SELECT a.CODSERIE, a.CODTURMA, a.CODMATERIA, a.MATRICULA, a.ANO,2, a.codprofturma
FROM acumulado a);
INSERT INTO chamadaaula(codserie, codturma, codmateria, matricula, codano, codetapa, codprofturma) (
SELECT a.CODSERIE, a.CODTURMA, a.CODMATERIA, a.MATRICULA, a.ANO,3, a.codprofturma
FROM acumulado a); 

-- INSERIR REGISTROS NA TABELA NOTAS

INSERT INTO notas(codserie, codturma, codmateria, matricula, ano, codetapa, codprofturma) (
SELECT a.CODSERIE, a.CODTURMA, a.CODMATERIA, a.MATRICULA, a.ANO,1, a.codprofturma
FROM acumulado a);
INSERT INTO notas(codserie, codturma, codmateria, matricula, ano, codetapa, codprofturma) (
SELECT a.CODSERIE, a.CODTURMA, a.CODMATERIA, a.MATRICULA, a.ANO,2, a.codprofturma
FROM acumulado a);
INSERT INTO notas(codserie, codturma, codmateria, matricula, ano, codetapa, codprofturma) (
SELECT a.CODSERIE, a.CODTURMA, a.CODMATERIA, a.MATRICULA, a.ANO,3, a.codprofturma
FROM acumulado a);









/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */

                                                                                /* BIBLIOTECA*/

/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */






-- BIBLIOTECA

-- TODAS AS TABELAS ABAIXO, EU PRECISEI CRIAR UMA COLUNA COM CODIGO_AUX

-- bi_tipoacervo

select
        (s.unidade || s.codigo) as codtipoacervo,
        upper(s.descricao) as dsctipoacervo
from sigmidia s;

-- bi_acervocategoria

select
        (s.unidade || s.codigo) as codcategoria,
        upper(s.descricao) as dsccategoria
from sigacerv s;

-- bi_editora

select
        (s.unidade || s.codigo) as codeditora,
        upper(s.nome) as dsceditora
from sigedito s
where s.codigo is not null;

-- bi_autor

select
        (s.unidade || s.codigo) as codautor,
        upper(s.nome) as nomeautor
from sigautor s
where s.codigo is not null;

-- Criei uma tabela auxiliar a partir do script abaixo
-- SCRIPT

select
        replace(s.codigo,'-','') as codacervo_aux,
        upper(s.titulo) as dscacervo,
        (sa.unidade || sa.codigo) as codcategoria,
        (sm.unidade || sm.codigo) as codtipoacervo,
        (se.unidade || se.codigo) as codeditora,
        s.numero as numeroexemplar_aux,
        upper(sau.nome) as autor,
        'MIGRACAO' as cadastradorpor,
        '2020-04-07' as dtcadastro,
        s.anopubli as anopublicacao,
        s.unidade as codunidade,
        upper(s.localp) as localpublicacao,
        s.chamada as cdd,
        s.chamada2 as cutter
from siglivro s
left join sigacerv sa on sa.codigo = s.acervo and sa.unidade = s.unidade
left join sigmidia sm on sm.codigo = s.midia and sm.unidade = s.unidade
left join sigedito se on se.codigo = s.editora and se.unidade = s.unidade
left join sigautor sau on sau.codigo = s.codautor and sau.unidade = s.unidade
where s.codigo is not null;

-- APÓS PEGAR OS DADOS, INSERIR NA TABELA AUX CRIADA LOGO ACIMA

-- APÓS INSERIR OS DADOS, RODAR SCRIPT ABAIXO PARA PEGAR AS INFORMAÇÕES DA TABELA AUX E INSERIR NA TABELA DE ACERVO

INSERT INTO bi_acervo(codacervo,dscacervo,codcategoria,codtipoacervo,codeditora,numexemplar,cadastradorpor,dtcadastro,autor,anopublicacao,localpublicacao,codunidade,cdd,cutter)
SELECT
		b.CODACERVO_AUX,
		left(b.dscacervo,150) AS dscacervo,
		b.codcategoria AS codcategoria,
		b.codtipoacervo AS codtipoacervo,
		b.codeditora AS codeditora,
		coalesce(b.numeroexemplar_aux,1) AS numexemplar,
		b.cadastradorpor,
		b.dtcadastro,
		b.autor,
		b.anopublicacao,
		b.localpublicacao,
		b.codunidade,
		b.cdd,
		b.cutter 
FROM bi_acervo_siga b
GROUP BY b.CODACERVO_AUX;

-- INSERIR ACERVO EXEMPLAR

select
      st.tombo as codacervoexemplar,
      iif(st.exemplar = '', 1, st.exemplar) as codexemplar,
      replace(st.livro,'-','') as codacervo,
      st.publicacao as anopublicacao
from sigtombo st
join siglivro s on s.codigo = st.livro;

INSERT INTO bi_acervoexemplar (codacervoexemplar,codexemplar,codacervo,anopublicacao)
SELECT codacervoexemplar,codexemplar,codacervo,anopublicacao FROM bi_acervoexemplar_siga GROUP BY codacervoexemplar;

-- INSERIR ACERVO AUTOR
INSERT INTO bi_acervoautor(codacervo,codautor,ativo)
SELECT ac.codacervo,au.codautor,'S' FROM bi_acervo ac JOIN bi_autor au ON au.nomeautor = ac.autor;

/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */

                                                                                /* DESPESAS*/

/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */


-- FORNCEDOR
-- Criar tabela auxiliar

CREATE TABLE FORNECEDOR_SIGA (
    CODFORNECEDOR VARCHAR(4),
    NOMEFORNECEDOR VARCHAR(80),
    RAZAOSOCIAL VARCHAR(40),
    ENDERECO VARCHAR(45),
    BAIRRO VARCHAR(20),
    CIDADE VARCHAR(20),
    CEP VARCHAR(9),
    CONTATO VARCHAR(30),
    TELEFONE VARCHAR(30),
    CELULAR VARCHAR(15),
    FAX VARCHAR(30),
    EMAIL VARCHAR(40),
    CPFCNPJ VARCHAR(19),
    OBS VARCHAR(250),
	COD_CIDADE INTEGER);

-- Rodar select na base do cliente e gerar insert para nossa tabela auxiliar
select
s.codigo as codfornecedor,
coalesce(s.fantasia, s.nome) as nomefornecedor,
s.nome as razaosocial,
s.endereco,
s.bairro,
s.cidade,
s.cep,
s.contato,
s.telefone,
s.celular,
s.fax,
s.email,
s.cnpj as cpfcnpj,
s.observacao as obs
from sigforne s;

-- rodar update para acertar as cidades
UPDATE fornecedor_siga fs JOIN cidade c ON c.nom_cidade = fs.CIDADE SET fs.cod_cidade = c.cod_cidade;

-- insert dos fornecedores da tabela auxiliar para a tabela fornecedor_siga
INSERT INTO fornecedor (CODFORNECEDOR,NOMEFORNECEDOR,RAZAOSOCIAL,ENDERECO,BAIRRO,CODCIDADE,CEP,CONTATO,TELFONE,CELULAR,email,CPFCNjp,OBS)
SELECT 
CODFORNECEDOR,
NOMEFORNECEDOR,
RAZAOSOCIAL,
ENDERECO,
BAIRRO,
cod_cidade,
REMOVEcaractere(CEP),
CONTATO,
if(length(REMOVEcaractere(TELEFONE)) IN (8,10),REMOVEcaractere(TELEFONE),if(length(REMOVEcaractere(celular)) IN (8,10),REMOVEcaractere(celular),NULL) ) AS TELEFONE,
if(length(REMOVEcaractere(celular)) IN (9,11),REMOVEcaractere(celular),if(length(REMOVEcaractere(TELEFONE)) IN (9,11),REMOVEcaractere(TELEFONE),NULL)) AS celular,
email,
REMOVEcaractere(CPFCNPJ),
left(OBS,150)
FROM fornecedor_siga;

-- NATUREZA
-- Rodar na base do cliente e gerar insert e codigo create para tabela auxiliar
-- Devido a demora na entrega do cliente, eu criei uma natureza chamada MIGRAÇÃO para as despesas e importei todas as naturezas de despesa do cliente como filhos dessa que eu criei
-- Depois instrui o cliente a usar a rotina de transferir plano de contas para corrigir no sistema.
select
max(p.nome) as dscnatureza,
p.codigo codtext
from sigplano p inner join sigdebto d on d.debito = p.codigo group by p.codigo;

INSERT INTO natureza (dscnatureza,codnaturezapai,codext,tiponatureza)
SELECT CONCAT('MIG_',dscnatureza), 395, codtext, 'D' FROM natureza_siga; 

-- DESPESA
CREATE TABLE DESPESA_SIGA (
    CODDESPESA VARCHAR(8),
    DSCDESPESA VARCHAR(70),
    CODNATUREZA VARCHAR(15),
    VENCIMENTO DATE,
    VALORPAGAR DOUBLE PRECISION,
    CODUNIDADE VARCHAR(3),
    DTPAGAMENTO DATE,
    VALORPAGO DOUBLE PRECISION,
    CADASTRADOPOR VARCHAR(15),
    DTCADASTRO DATE,
    NUMDOCUMENTO VARCHAR(20),
    DTPROCESSAMENTO DATE,
    AGENCIA VARCHAR(5),
    NUMCONTA VARCHAR(10),
    OBS VARCHAR(40));
-- Rodar na base do cliente e gerar insert e codigo create para tabela auxiliar
select
d.controle                                                                      as coddespesa,
d.historico                                                                     as dscdespesa,
d.debito                                                                        as codnatureza,
d.datav                                                                         as vencimento,
d.valorv                                                                        as valorpagar,
d.empresa                                                                       as codunidade,
d.datap                                                                         as dtpagamento,
iif(d.datap is null, null, d.valorv)                                            as valorpago,
d.usuario                                                                       as cadastradopor,
d.datacad                                                                       as dtcadastro,
d.documento                                                                     as numdocumento,
d.dtinclusao                                                                    as dtprocessamento,
d.agencia                                                                       as agencia,
d.cc                                                                            as numconta
from sigdebto d;

-- depois de inserir no escolaweb, alguns updates para ajuste
UPDATE despesa_siga SET numdocumento = NULL WHERE numdocumento = '';
UPDATE despesa_siga SET agencia = NULL WHERE agencia = '';
UPDATE despesa_siga SET numconta = NULL WHERE numconta = '';
UPDATE despesa_siga SET obs = NULL WHERE obs = '';

-- Rodar no nosso BD
insert into despesa(coddespesa,dscdespesa,codnatureza,vencimento,valorpagar,codunidade,dtpagamento,valorpago,cadastradopor,dtcadastro,numdocumento,dtprocessamento,codconta,obs)
select
coddespesa,
left(dscdespesa,80) AS dscdespesa,
IFNULL(n.codnatureza,1419),
vencimento,
valorpagar,
us.CODUNIDADE_tella,
dtpagamento,
valorpago,
1 AS cadastradopor,
dtcadastro,
left(numdocumento,20) AS numdocumento,
dtprocessamento,
cs.codconta_tella,
left(CONCAT('cadastradopor: ',cadastradopor,' Favorecido: ', obs),200) AS obs 
from despesa_siga d
left JOIN natureza n ON d.CODNATUREZA = n.codext
LEFT JOIN conta_siga cs ON cs.AGENCIA = d.AGENCIA AND cs.NUMCONTA = d.NUMCONTA
LEFT JOIN unidadeescolar_siga2 us ON us.CODUNIDADE = d.CODUNIDADE;






















































