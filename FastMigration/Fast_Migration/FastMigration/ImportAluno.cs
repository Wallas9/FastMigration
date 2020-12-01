using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Isql;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Aluno")]
    public class ImportAluno : IImportArgs
    {
        
        public void ExecutarProcedimento(params object[] args)
        {
            RecordLog r = new RecordLog();

            MySqlCommand log = new MySqlCommand();

            string MySQL = $@"server = {args[1]}; user id = {args[2]}; database = {args[0]}; password = {args[7]};";
            MySqlConnection conn = new MySqlConnection(MySQL);
            conn.Open();

            log.Connection = conn;

            string FbConn = $@"DataSource = {args[4]}; Database = {args[3]}; username = {args[5]}; password = {args[6]}; CHARSET = NONE;";
            FbConnection conn2 = new FbConnection(FbConn);
            conn2.Open();

            MySqlCommand insert = new MySqlCommand("", conn);
            insert.CommandTimeout = 86400;

            try
            {
                DataTable dtable = new DataTable();

                MySqlCommand create = new MySqlCommand(
                $@"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS mig_aluno_siga;
                DELETE FROM aluno;
                CREATE TABLE `mig_aluno_siga` (
	            `codaluno` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	            `nomealuno` VARCHAR(100) NOT NULL,
	            `endereco` VARCHAR(200) NULL DEFAULT NULL,
	            `bairro` VARCHAR(60) NULL DEFAULT NULL,
	            `uf` SMALLINT(6) NULL DEFAULT NULL,
	            `cidade` varchar(100) NULL DEFAULT NULL,
	            `cep` VARCHAR(20) NULL DEFAULT NULL,
	            `telresid` VARCHAR(20) NULL DEFAULT NULL,
	            `celular` VARCHAR(20) NULL DEFAULT NULL,
	            `email` VARCHAR(100) NULL DEFAULT NULL,
	            `sexo` CHAR(1) NULL DEFAULT NULL,
	            `dtnascimento` varchar(100) NULL DEFAULT NULL,
	            `ufnaturalidade` SMALLINT(6) UNSIGNED NULL DEFAULT NULL,
	            `cidadenaturalidade` varchar(100) NULL DEFAULT NULL,
	            `cor` VARCHAR(13) NULL DEFAULT NULL,
	            `pai` VARCHAR(100) NULL DEFAULT NULL,
	            `trabpai` varchar(100) NULL DEFAULT NULL,
	            `profpai` VARCHAR(40) NULL DEFAULT NULL,
	            `telpai` VARCHAR(11) NULL DEFAULT NULL,
	            `celularpai` VARCHAR(11) NULL DEFAULT NULL,
	            `emailpai` VARCHAR(100) NULL DEFAULT NULL,
	            `mae` VARCHAR(100) NULL DEFAULT NULL,
	            `trabmae` varchar(100) NULL DEFAULT NULL,
	            `profmae` VARCHAR(40) NULL DEFAULT NULL,
	            `telmae` VARCHAR(11) NULL DEFAULT NULL,
	            `celularmae` VARCHAR(11) NULL DEFAULT NULL,
	            `emailmae` VARCHAR(100) NULL DEFAULT NULL,
	            `obs` VARCHAR(600) NULL DEFAULT NULL,
	            `cpf` VARCHAR(11) NULL DEFAULT NULL,
	            `rg` varchar(100) NULL DEFAULT NULL,
	            `foto` VARCHAR(100) NULL DEFAULT NULL,
	            `senha` VARCHAR(8) NULL DEFAULT NULL,
	            `nomeabrev` VARCHAR(40) NULL DEFAULT NULL,
	            `certidaonumero` VARCHAR(40) NULL DEFAULT NULL,
	            `certidaolivrofolha` VARCHAR(30) NULL DEFAULT NULL,
	            `certidaocartorio` varchar(100) NULL DEFAULT NULL,
	            `religiao` VARCHAR(40) NULL DEFAULT NULL,
	            `codeducacaoespecial` SMALLINT(6) NULL DEFAULT NULL,
	            `obseducacaoespecial` VARCHAR(500) NULL DEFAULT NULL,
	            `codsocio` INT(11) NULL DEFAULT NULL,
	            `vivopai` CHAR(1) NULL DEFAULT 'S',
	            `instrucaopai` VARCHAR(40) NULL DEFAULT NULL,
	            `enderecopai` VARCHAR(200) NULL DEFAULT NULL,
	            `bairropai` VARCHAR(60) NULL DEFAULT NULL,
	            `codcidadepai` varchar(100) NULL DEFAULT NULL,
	            `codufpai` SMALLINT(6) NULL DEFAULT NULL,
	            `ceppai` VARCHAR(8) NULL DEFAULT NULL,
	            `vivomae` CHAR(1) NULL DEFAULT 'S',
	            `instrucaomae` VARCHAR(40) NULL DEFAULT NULL,
	            `enderecomae` VARCHAR(200) NULL DEFAULT NULL,
	            `bairromae` VARCHAR(60) NULL DEFAULT NULL,
	            `cepmae` VARCHAR(8) NULL DEFAULT NULL,
	            `codufmae` SMALLINT(6) NULL DEFAULT NULL,
	            `codcidademae` varchar(100) NULL DEFAULT NULL,
	            `ativo` CHAR(1) NULL DEFAULT 'S',
	            `fotoPortal` VARCHAR(100) NULL DEFAULT NULL,
	            `foneportal` VARCHAR(10) NULL DEFAULT NULL,
	            `emailportal` VARCHAR(60) NULL DEFAULT NULL,
	            `titeleitor` VARCHAR(15) NULL DEFAULT NULL,
	            `zona` VARCHAR(10) NULL DEFAULT NULL,
	            `dtcadastro` varchar(100) NULL DEFAULT NULL,
	            `usercadastro` INT(11) NULL DEFAULT NULL,
	            `dtalteracao` varchar(100) NULL DEFAULT NULL,
	            `useralteracao` INT(11) NULL DEFAULT NULL,
	            `nacionalidade` VARCHAR(30) NULL DEFAULT NULL,
	            `bloqueado` CHAR(1) NULL DEFAULT NULL,
	            `obshistfundamental` VARCHAR(3000) NULL DEFAULT NULL,
	            `obshistmedio` VARCHAR(3000) NULL DEFAULT NULL,
	            `estadocivilpai` varchar(100) NULL DEFAULT NULL,
	            `estadocivilmae` varchar(100) NULL DEFAULT NULL,
	            `dtnascimentopai` varchar(100) NULL DEFAULT NULL,
	            `dtnascimentomae` varchar(100) NULL DEFAULT NULL,
	            `codpaisnaturalidade` INT(11) NULL DEFAULT NULL,
	            `alunoadotado` CHAR(1) NULL DEFAULT NULL,
	            `sabeadocao` CHAR(1) NULL DEFAULT NULL,
	            `paisseparados` CHAR(1) NULL DEFAULT NULL,
	            `tiposanguineo` varchar(100) NULL DEFAULT NULL,
	            `planosaude` VARCHAR(100) NULL DEFAULT NULL,
	            `alergia` VARCHAR(100) NULL DEFAULT NULL,
	            `doencacongenita` varchar(100) NULL DEFAULT NULL,
	            `deficiencia` varchar(100) NULL DEFAULT NULL,
	            `cuidadoespecial` VARCHAR(100) NULL DEFAULT NULL,
	            `locomocaoida` VARCHAR(40) NULL DEFAULT NULL,
	            `locomocaovolta` VARCHAR(40) NULL DEFAULT NULL,
	            `obsguiatransferencia` VARCHAR(500) NULL DEFAULT NULL,
	            `naturalidade` VARCHAR(100) NULL DEFAULT NULL,
	            `cpfpai` VARCHAR(11) NULL DEFAULT NULL,
	            `rgpai` varchar(100) NULL DEFAULT NULL,
	            `cpfmae` VARCHAR(11) NULL DEFAULT NULL,
	            `rgmae` varchar(100) NULL DEFAULT NULL,
	            `fotoblob` BLOB NULL,
	            `estadocivil` varchar(100) NULL DEFAULT NULL,
	            `saisozinho` CHAR(1) NULL DEFAULT NULL,
	            `tipocertidao` CHAR(1) NULL DEFAULT 'N',
	            `certreservista` VARCHAR(40) NULL DEFAULT NULL,
	            `profissao` VARCHAR(40) NULL DEFAULT NULL,
	            `localtrabalho` VARCHAR(100) NULL DEFAULT NULL,
	            `teltrabalho` VARCHAR(10) NULL DEFAULT NULL,
	            `numcartao` varchar(100) NULL DEFAULT NULL,
	            `enviarsms` CHAR(1) NULL DEFAULT NULL,
	            `celularsms` VARCHAR(11) NULL DEFAULT NULL,
	            `operadorasms` CHAR(1) NULL DEFAULT NULL,
	            `hospital` VARCHAR(100) NULL DEFAULT NULL,
	            `pne` CHAR(1) NULL DEFAULT 'N' COMMENT 'Aluno portado de necessidades especiais (S/N)',
	            `pnedescricao` VARCHAR(100) NULL DEFAULT NULL COMMENT 'Descricao na necessidade especial',
	            `pneavaliado` CHAR(1) NULL DEFAULT 'S' COMMENT 'Aluno é avalidado com notas como os outros alunos',
	            `alunofalecido` CHAR(1) NULL DEFAULT 'N',
	            `codcenso` varchar(100) NULL DEFAULT NULL,
	            `codportaria` INT(11) NULL DEFAULT NULL,
	            `codpaisorigem` SMALLINT(6) NULL DEFAULT NULL,
	            `zonaresidencia7024` INT(1) NULL DEFAULT NULL COMMENT '1-Urbana/2-Rural',
	            `rgorgaoemissor` INT(2) NULL DEFAULT NULL COMMENT 'Orgao Emissor da indentidade. Relaciona com a tabela orgaoemissoridentidade',
	            `rgestado` SMALLINT(6) NULL DEFAULT NULL COMMENT 'Estado Identidade',
	            `rgdataemissao` DATE NULL DEFAULT NULL COMMENT 'Data de Emissao RG',
	            `rgcomplemento` VARCHAR(4) NULL DEFAULT NULL COMMENT 'Complemento RG',
	            `modelocertidao` INT(1) NULL DEFAULT NULL COMMENT '1-Modelo Antigo / 2-Modelo Novo',
	            `codcartoriocertidao` INT(11) NULL DEFAULT NULL COMMENT 'codigo do cartorio que emitiu a certidao',
	            `dtemissaocertidao` DATE NULL DEFAULT NULL COMMENT 'Data da emissão da certidao',
	            `rgorgaoemissorpai` INT(2) NULL DEFAULT NULL COMMENT 'Orgao Emissor da indentidade. Relaciona com a tabela orgaoemissoridentidade',
	            `rgestadopai` SMALLINT(6) NULL DEFAULT NULL COMMENT 'Estado Identidade',
	            `rgdataemissaopai` DATE NULL DEFAULT NULL COMMENT 'Data de Emissao RG',
	            `nacionalidadepai` varchar(100) NULL DEFAULT NULL,
	            `codpaisorigempai` SMALLINT(6) NULL DEFAULT NULL,
	            `codcidadenaturalidadepai` varchar(100) NULL DEFAULT NULL,
	            `codescolaridadepai` INT(11) NULL DEFAULT NULL,
	            `rgorgaoemissormae` INT(2) NULL DEFAULT NULL COMMENT 'Orgao Emissor da indentidade. Relaciona com a tabela orgaoemissoridentidade',
	            `rgestadomae` SMALLINT(6) NULL DEFAULT NULL COMMENT 'Estado Identidade',
	            `rgdataemissaomae` DATE NULL DEFAULT NULL COMMENT 'Data de Emissao RG',
	            `nacionalidademae` varchar(100) NULL DEFAULT NULL,
	            `codpaisorigemmae` SMALLINT(6) NULL DEFAULT NULL,
	            `codcidadenaturalidademae` varchar(100) NULL DEFAULT NULL,
	            `codescolaridademae` INT(11) NULL DEFAULT NULL,
	            `codrendafamiliar` INT(11) NULL DEFAULT NULL,
	            `obsfinanceiro` VARCHAR(600) NULL DEFAULT NULL,
	            `obsacademico` VARCHAR(600) NULL DEFAULT NULL,
	            `sexomae` CHAR(1) NULL DEFAULT 'F',
	            `sexopai` CHAR(1) NULL DEFAULT 'M',
	            `codbolsapadrao` INT(11) NULL DEFAULT NULL,
	            `diavencpadrao` INT(11) NULL DEFAULT NULL,
	            `ra` varchar(100) NULL DEFAULT NULL,
	            `localbatalhaomilitar` VARCHAR(100) NULL DEFAULT NULL,
	            `categoriamilitar` varchar(100) NULL DEFAULT NULL,
	            `situacaomilitar` varchar(100) NULL DEFAULT NULL,
	            `dataexpedicaomilitar` DATE NULL DEFAULT NULL,
	            `secaoeleitor` VARCHAR(10) NULL DEFAULT NULL,
	            `dataexpedicaoeleitor` DATE NULL DEFAULT NULL,
	            `avatoken` VARCHAR(200) NULL DEFAULT NULL COMMENT 'token de autenticação do aluno no ava json (login,senha) convertido em base64',
	            `avauserid` INT(11) NULL DEFAULT NULL,
	            `obshistinfantil` VARCHAR(3000) NULL DEFAULT NULL,
	            `idgoogledrive` VARCHAR(100) NULL DEFAULT NULL,
	            `codescolaridadealuno` INT(11) NULL DEFAULT NULL,
	            `codaluno_tella` INT (11) NULL DEFAULT NULL,
	            INDEX `idx_codaluno` (`codaluno`) USING BTREE,
	            PRIMARY KEY (`codaluno`)
                )
                COLLATE='latin1_swedish_ci'
                ENGINE=InnoDB
                AUTO_INCREMENT=1
                ;", conn);
                create.ExecuteNonQuery();

                //cria a funcao REMOVEcaractere e validaemail
                MySqlCommand functions = new MySqlCommand($@"
                DROP FUNCTION IF EXISTS REMOVEcaractere;
                CREATE DEFINER=`root`@`%` FUNCTION `REMOVEcaractere`(`P_TEXTO` VARCHAR(200))
                RETURNS char(50) CHARSET latin1
                LANGUAGE SQL
                NOT DETERMINISTIC
                CONTAINS SQL
                SQL SECURITY DEFINER
                COMMENT ''
                RETURN replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(P_TEXTO,'.',''),'-',''),'/',''),' ',''),'_',''),';',''),')',''),'(',''), ' ', ''),'a',''),'b',''),'c',''),'d',''),'e',''),'f',''),'g',''),'h',''),'i',''),'j',''),'k',''),'l',''),'m',''),'n',''),'o',''),'p',''),'r',''),'s',''),'t',''),'u',''),'x',''),'z',''),'w',''),'y',''),'|',''),'\'',''),',',''),'$',''),'!',''),'@',''),'+',''),'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'R',''),'S',''),'T',''),'U',''),'V',''),'X',''),'Z',''),'W',''),'Y',''),'á',''),'à',''),'ã',''),'â',''),'Á',''),'À',''),'Ã',''),'Â',''),'é',''),'è',''),'ê',''),'É',''),'È',''),'Ê',''),'í',''),'ì',''),'î',''),'Í',''),'Ì',''),'Î',''),'ó',''),'ò',''),'ô',''),'õ',''),'Ó',''),'Ò',''),'Ô',''),'Õ',''),'ú',''),'ù',''),'û',''),'Ú',''),'Ù',''),'Û',''),'v',''),'q',''),'Q','');

                DROP FUNCTION IF EXISTS VALIDAEMAIL;
                CREATE DEFINER=`root`@`%` FUNCTION `VALIDAEMAIL`(`P_EMAIL` VARCHAR(200))
                RETURNS char(1) CHARSET latin1
                LANGUAGE SQL
                NOT DETERMINISTIC
                CONTAINS SQL
                SQL SECURITY DEFINER
                COMMENT ''
                BEGIN
                DECLARE V_AUX INT;
                DECLARE V_AUX2 CHAR(1);
                SET V_AUX = 0;
                SET V_AUX = (SELECT COUNT(1) FROM (select P_EMAIL from dual
                where P_EMAIL not regexp '[^a-z,0-9,@,.,_,-]'
                and P_EMAIL like '%_@_%_.__%'
                and P_EMAIL not like '%@@%'
                and P_EMAIL not like '%@'
                and P_EMAIL not like '%.'
                and P_EMAIL not like '%\_'
                and P_EMAIL not like '%-'
                and P_EMAIL not like '-%'
                and P_EMAIL not like '%_@_%_..%' ) AS T);
                IF(V_AUX = 0) THEN
                SET V_AUX2 = 'F';
                ELSE
                SET V_AUX2 = 'V';
                END IF;
                RETURN V_AUX2;
                END", conn);
                functions.ExecuteNonQuery();

                FbCommand MySelect = new FbCommand(@"select
                replace(g.prontuario,'-','') as codaluno,
                replace(upper(max((rtrim(g.nome)||' '||ltrim(g.sobrenome)))), '''', '') as nomealuno,
                replace(replace(upper(max((coalesce(g.endereco, '')||' '||coalesce(g.numero, '')||' '||coalesce(g.complement, '')))),';',','), '''', '') as endereco,
                replace(upper(max(g.bairro)), '''', '') as bairro,
                max(tt.cod_cidade) as cidade,
                max(replace(g.cep,'-','')) as cep,
                max(iif((char_length(replace(replace(g.foneres, '-', ''),'.','')) = 8), ('11' || replace(replace(g.foneres, '-', ''),'.','')), null))  as telresid,
                max(iif(char_length(replace(replace(replace(replace(replace(g.fonecel,'-',''),'.',''),'Pai',''),'Mãe',''),'Gabriel','')) = 9, '11' || replace(replace(replace(replace(replace(g.fonecel,'-',''),'.',''),'Pai',''),'Mãe',''),'Gabriel',''), null)) as celular,
                max(iif(g.email = '',null,g.email)) as email,
                max(g.sexo) as sexo,
                max(EXTRACT(YEAR FROM g.nascimento) || '-' || EXTRACT(MONTH FROM g.nascimento) || '-' || EXTRACT(DAY FROM g.nascimento)) as dtnascimento,
                max(tt.cod_cidade) as cidadenaturalidade,
                replace(upper(max(g.pai)), '''', '') as pai,
                replace(replace(upper(max(replace(left(g.profi_pai, 40),'/',''))), '''', ''), '\', '') as profpai,
                replace(max(iif(char_length(replace(replace(replace(replace(replace(g.fonepai,'-',''),'.',''),'COML',''),'R',''),'COMERCIAL','')) = 8, '11' || replace(replace(replace(replace(replace(g.fonepai,'-',''),'.',''),'COML',''),'R',''),'COMERCIAL',''), null)), '''', '') as telpai,
                replace(max(iif(char_length(replace(replace(replace(replace(g.celpai, '-',''),'.',''),'BRASIL',''),'COML','')) = 9, '11' || replace(replace(replace(replace(g.celpai, '-',''),'.',''),'BRASIL',''),'COML',''), null)), '''', '') as celularpai,
                replace(max(g.emailpai),';','|') as emailpai,
                replace(max(g.mae), '''', '') as mae,
                replace(replace(left(max(g.profi_mae),40), '''', ''), '\', '') as profmae,
                replace(max(iif(char_length(replace(replace(replace(replace(replace(replace(g.fonemae,'-',''),'.',''),'COML',''),'COKM',''),' ',''),'/','')) = 8, '11' || replace(replace(replace(replace(replace(replace(g.fonemae,'-',''),'.',''),'COML',''),'COKM',''),' ',''),'/',''), null)), '''', '') as telmae,
                replace(max(iif(char_length(replace(replace(replace(replace(replace(g.celmae,'-',''),'.',''),'COML',''),'(',''),')','')) = 9, '11' || replace(replace(replace(replace(replace(g.celmae,'-',''),'.',''),'COML',''),'(',''),')',''), iif(char_length(replace(replace(replace(replace(replace(g.celmae,'-',''),'.',''),'COML',''),'(',''),')','')) = 11, replace(replace(replace(replace(replace(g.celmae,'-',''),'.',''),'COML',''),'(',''),')',''), null))), '''', '') as celularmae,
                replace(max(g.emailmae),';','|') as emailmae,
                max(g.cpf) as cpf,
                replace(max(g.rg), '''', '') as rg,
                '' as certidaonumero,
                '' as certidaolivrofolha,
                '' as certidaocartorio,
                replace(replace(max(iif(g.end_pai is null, '', g.end_pai) || iif(g.num_pai is null, '', g.num_pai)),';',','), '''', '') as enderecopai,
                replace(max(g.bair_pai), '''', '') as bairropai,
                max(left(replace(replace(g.cep_pai,'-',''),'.',''),8)) as ceppai,
                replace(replace(max(iif(g.end_mae is null, '', g.end_mae) || iif(g.num_mae is null, '', g.num_mae)),';',','), '''', '') as enderecomae,
                replace(replace(max(g.bair_mae),';',','), '''', '') as bairromae,
                max(left(replace(replace(g.cep_mae,'-',''),'.',''),8)) as cepmae,
                max(na.descricao) as nacionalidade,
                max(np.descricao) as nacionalidadepai,
                max(nm.descricao) as nacionalidademae,
                max(EXTRACT(YEAR FROM g.nascpai) || '-' || EXTRACT(MONTH FROM g.nascpai) || '-' || EXTRACT(DAY FROM g.nascpai)) as dtnascimentopai,
                max(EXTRACT(YEAR FROM g.nascmae) || '-' || EXTRACT(MONTH FROM g.nascmae) || '-' || EXTRACT(DAY FROM g.nascmae)) as dtnascimentomae,
                max(g.estcivpai) as estadocivilpai,
                max(g.estcivmae) as estadocivilmae,
                max(g.cidadenasc) as naturalidade,
                max(g.cpfpai) as cpfpai,
                replace(max(g.rgpai), '''', '') as rgpai,
                max(g.cpfmae) as cpfmae,
                replace(max(g.rgmae), '''', '') as rgmae,
                replace(max('TEL: ' || coalesce(g.foneres, ' ') || ' CEL: ' || coalesce(g.fonecel,' ') || ' TELPAI: ' || coalesce(g.fonepai, ' ') || ' CELPAI: ' || coalesce(g.celpai, ' ') || ' TELMAE: ' || coalesce(g.fonemae,' ') || ' CELMAE: ' || coalesce(g.celmae, ' ') ), '''', '') as obs
                from siggeral g
                left join cidade_tella tt on tt.nom_cidade = upper(g.cidade)
                left join signacio na on na.codigo = g.nacio
                left join signacio np on np.codigo = g.nacio_pai
                left join signacio nm on nm.codigo = g.nacio_mae
                group by replace(g.prontuario,'-','');", conn2);

                FbDataAdapter adapter = new FbDataAdapter(MySelect);

                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();

                try
                {
                    queryBuilder.Append("INSERT INTO mig_aluno_siga (CODALUNO, NOMEALUNO, ENDERECO, BAIRRO, CIDADE, CEP, TELRESID, CELULAR, EMAIL, SEXO, DTNASCIMENTO, CIDADENATURALIDADE, PAI, PROFPAI, TELPAI, CELULARPAI, EMAILPAI, MAE, PROFMAE, TELMAE, CELULARMAE, EMAILMAE, CPF, RG, CERTIDAONUMERO, CERTIDAOLIVROFOLHA, CERTIDAOCARTORIO, ENDERECOPAI, BAIRROPAI, CEPPAI, ENDERECOMAE, BAIRROMAE, CEPMAE, NACIONALIDADE, NACIONALIDADEPAI, NACIONALIDADEMAE, DTNASCIMENTOPAI, DTNASCIMENTOMAE, ESTADOCIVILPAI, ESTADOCIVILMAE, NATURALIDADE, CPFPAI, RGPAI, CPFMAE, RGMAE, OBS) VALUES ");

                    for (int i = 0; i < dtable.Rows.Count; i++)
                    {
                        queryBuilder.Append($@"('{dtable.Rows[i]["CODALUNO"]}' , '{dtable.Rows[i]["NOMEALUNO"]}' , '{dtable.Rows[i]["ENDERECO"]}' , '{dtable.Rows[i]["BAIRRO"]}' , '{dtable.Rows[i]["CIDADE"]}' , '{dtable.Rows[i]["CEP"]}' , '{dtable.Rows[i]["TELRESID"]}' , '{dtable.Rows[i]["CELULAR"]}' , '{dtable.Rows[i]["EMAIL"]}' , '{dtable.Rows[i]["SEXO"]}' , '{dtable.Rows[i]["DTNASCIMENTO"]}' , '{dtable.Rows[i]["CIDADENATURALIDADE"]}' , '{dtable.Rows[i]["PAI"]}' , '{dtable.Rows[i]["PROFPAI"]}' , '{dtable.Rows[i]["TELPAI"]}' , '{dtable.Rows[i]["CELULARPAI"]}' , '{dtable.Rows[i]["EMAILPAI"]}' , '{dtable.Rows[i]["MAE"]}' , '{dtable.Rows[i]["PROFMAE"]}' , '{dtable.Rows[i]["TELMAE"]}' , '{dtable.Rows[i]["CELULARMAE"]}' , '{dtable.Rows[i]["EMAILMAE"]}' , '{dtable.Rows[i]["CPF"]}' , '{dtable.Rows[i]["RG"]}' , '{dtable.Rows[i]["CERTIDAONUMERO"]}' , '{dtable.Rows[i]["CERTIDAOLIVROFOLHA"]}' , '{dtable.Rows[i]["CERTIDAOCARTORIO"]}' , '{dtable.Rows[i]["ENDERECOPAI"]}' , '{dtable.Rows[i]["BAIRROPAI"]}' , '{dtable.Rows[i]["CEPPAI"]}' , '{dtable.Rows[i]["ENDERECOMAE"]}' , '{dtable.Rows[i]["BAIRROMAE"]}' , '{dtable.Rows[i]["CEPMAE"]}' , '{dtable.Rows[i]["NACIONALIDADE"]}' , '{dtable.Rows[i]["NACIONALIDADEPAI"]}' , '{dtable.Rows[i]["NACIONALIDADEMAE"]}' , '{dtable.Rows[i]["DTNASCIMENTOPAI"]}' , '{dtable.Rows[i]["DTNASCIMENTOMAE"]}' , '{dtable.Rows[i]["ESTADOCIVILPAI"]}' , '{dtable.Rows[i]["ESTADOCIVILMAE"]}' , '{dtable.Rows[i]["NATURALIDADE"]}' , '{dtable.Rows[i]["CPFPAI"]}' , '{dtable.Rows[i]["RGPAI"]}' , '{dtable.Rows[i]["CPFMAE"]}' , '{dtable.Rows[i]["RGMAE"]}' , '{dtable.Rows[i]["OBS"]}'), ");
                    }
                    queryBuilder.Remove(queryBuilder.Length - 2, 2);

                    insert = new MySqlCommand(queryBuilder.ToString(), conn);
                    insert.ExecuteNonQuery();
                }

                catch (Exception err)
                {
                    if (err.Message == "Packets larger than max_allowed_packet are not allowed.")
                    {
                        for (int i = 0; i < dtable.Rows.Count; i++)
                        {
                            insert.CommandText = $@"INSERT INTO mig_aluno_siga (CODALUNO, NOMEALUNO, ENDERECO, BAIRRO, CIDADE, CEP, TELRESID, CELULAR, EMAIL, SEXO, DTNASCIMENTO, CIDADENATURALIDADE, PAI, PROFPAI, TELPAI, CELULARPAI, EMAILPAI, MAE, PROFMAE, TELMAE, CELULARMAE, EMAILMAE, CPF, RG, CERTIDAONUMERO, CERTIDAOLIVROFOLHA, CERTIDAOCARTORIO, ENDERECOPAI, BAIRROPAI, CEPPAI, ENDERECOMAE, BAIRROMAE, CEPMAE, NACIONALIDADE, NACIONALIDADEPAI, NACIONALIDADEMAE, DTNASCIMENTOPAI, DTNASCIMENTOMAE, ESTADOCIVILPAI, ESTADOCIVILMAE, NATURALIDADE, CPFPAI, RGPAI, CPFMAE, RGMAE, OBS) VALUES 
                                                     ('{dtable.Rows[i]["CODALUNO"]}' , '{dtable.Rows[i]["NOMEALUNO"]}' , '{dtable.Rows[i]["ENDERECO"]}' , '{dtable.Rows[i]["BAIRRO"]}' , '{dtable.Rows[i]["CIDADE"]}' , '{dtable.Rows[i]["CEP"]}' , '{dtable.Rows[i]["TELRESID"]}' , '{dtable.Rows[i]["CELULAR"]}' , '{dtable.Rows[i]["EMAIL"]}' , '{dtable.Rows[i]["SEXO"]}' , '{dtable.Rows[i]["DTNASCIMENTO"]}' , '{dtable.Rows[i]["CIDADENATURALIDADE"]}' , '{dtable.Rows[i]["PAI"]}' , '{dtable.Rows[i]["PROFPAI"]}' , '{dtable.Rows[i]["TELPAI"]}' , '{dtable.Rows[i]["CELULARPAI"]}' , '{dtable.Rows[i]["EMAILPAI"]}' , '{dtable.Rows[i]["MAE"]}' , '{dtable.Rows[i]["PROFMAE"]}' , '{dtable.Rows[i]["TELMAE"]}' , '{dtable.Rows[i]["CELULARMAE"]}' , '{dtable.Rows[i]["EMAILMAE"]}' , '{dtable.Rows[i]["CPF"]}' , '{dtable.Rows[i]["RG"]}' , '{dtable.Rows[i]["CERTIDAONUMERO"]}' , '{dtable.Rows[i]["CERTIDAOLIVROFOLHA"]}' , '{dtable.Rows[i]["CERTIDAOCARTORIO"]}' , '{dtable.Rows[i]["ENDERECOPAI"]}' , '{dtable.Rows[i]["BAIRROPAI"]}' , '{dtable.Rows[i]["CEPPAI"]}' , '{dtable.Rows[i]["ENDERECOMAE"]}' , '{dtable.Rows[i]["BAIRROMAE"]}' , '{dtable.Rows[i]["CEPMAE"]}' , '{dtable.Rows[i]["NACIONALIDADE"]}' , '{dtable.Rows[i]["NACIONALIDADEPAI"]}' , '{dtable.Rows[i]["NACIONALIDADEMAE"]}' , '{dtable.Rows[i]["DTNASCIMENTOPAI"]}' , '{dtable.Rows[i]["DTNASCIMENTOMAE"]}' , '{dtable.Rows[i]["ESTADOCIVILPAI"]}' , '{dtable.Rows[i]["ESTADOCIVILMAE"]}' , '{dtable.Rows[i]["NATURALIDADE"]}' , '{dtable.Rows[i]["CPFPAI"]}' , '{dtable.Rows[i]["RGPAI"]}' , '{dtable.Rows[i]["CPFMAE"]}' , '{dtable.Rows[i]["RGMAE"]}' , '{dtable.Rows[i]["OBS"]}');";
                            insert.ExecuteNonQuery();
                        }
                    }
                    else
                    {
                        MessageBox.Show(err.Message);
                        conn2.Close();
                        conn.Close();
                    }
                    
                }

                MySqlCommand update = new MySqlCommand(@"
                update mig_aluno_siga set dtnascimento = null where dtnascimento = '';
                update mig_aluno_siga set dtnascimentopai = null where dtnascimentopai = ''; 
                update mig_aluno_siga set dtnascimentomae = null where dtnascimentomae = '';
                update mig_aluno_siga set dtcadastro = null where dtcadastro = ''; 
                update mig_aluno_siga set dtalteracao = null where dtalteracao = '';
                update mig_aluno_siga set cidade = null where cidade = '';
                update mig_aluno_siga set cidadenaturalidade = null where cidadenaturalidade = '';
                update mig_aluno_siga set codcidadenaturalidadepai = null where codcidadenaturalidadepai = '';
                update mig_aluno_siga set codcidadenaturalidademae = null where codcidadenaturalidademae = '';
                update mig_aluno_siga set codcidadepai = null where codcidadepai = '';
                update mig_aluno_siga set codcidademae = null where codcidademae = '';", conn);
                update.ExecuteNonQuery();

                MySqlCommand insertAluno = new MySqlCommand($@"SET FOREIGN_KEY_CHECKS = 0; 
                INSERT INTO aluno (codaluno, nomealuno, endereco, bairro, uf, cidade, cep, telresid, celular, email, sexo, dtnascimento, ufnaturalidade, cidadenaturalidade, cor, pai, trabpai, profpai, telpai, celularpai, emailpai, mae, trabmae, profmae, telmae, celularmae, emailmae, obs, cpf, rg, foto, senha, nomeabrev, certidaonumero, certidaolivrofolha, certidaocartorio, religiao, codeducacaoespecial, obseducacaoespecial, codsocio, vivopai, instrucaopai, enderecopai, bairropai, codcidadepai, codufpai, ceppai, vivomae, instrucaomae, enderecomae, bairromae, cepmae, codufmae, codcidademae, ativo, fotoPortal, foneportal, emailportal, titeleitor, zona, dtcadastro, usercadastro, dtalteracao, useralteracao, nacionalidade, bloqueado, obshistfundamental, obshistmedio, estadocivilpai, estadocivilmae, dtnascimentopai, dtnascimentomae, codpaisnaturalidade, alunoadotado, sabeadocao, paisseparados, tiposanguineo, planosaude, alergia, doencacongenita, deficiencia, cuidadoespecial, locomocaoida, locomocaovolta, obsguiatransferencia, naturalidade, cpfpai, rgpai, cpfmae, rgmae, fotoblob, estadocivil, saisozinho, tipocertidao, certreservista, profissao, localtrabalho, teltrabalho, numcartao, enviarsms, celularsms, operadorasms, hospital, pne, pnedescricao, pneavaliado, alunofalecido, codcenso, codportaria, codpaisorigem, zonaresidencia7024, rgorgaoemissor, rgestado, rgdataemissao, rgcomplemento, modelocertidao, codcartoriocertidao, dtemissaocertidao, rgorgaoemissorpai, rgestadopai, rgdataemissaopai, nacionalidadepai, codpaisorigempai, codcidadenaturalidadepai, codescolaridadepai, rgorgaoemissormae, rgestadomae, rgdataemissaomae, nacionalidademae, codpaisorigemmae, codcidadenaturalidademae, codescolaridademae, codrendafamiliar, obsfinanceiro, obsacademico, sexomae, sexopai, codbolsapadrao, diavencpadrao, ra, localbatalhaomilitar, categoriamilitar, situacaomilitar, dataexpedicaomilitar, secaoeleitor, dataexpedicaoeleitor, avatoken, avauserid, obshistinfantil, idgoogledrive, codescolaridadealuno)
                (SELECT 
                MAX(codaluno) AS codaluno, 
                UPPER(nomealuno) AS nomealuno, 
                UPPER(endereco) AS endereco, 
                UPPER(bairro) AS bairro, 
                uf,
                cidade,
                left(REMOVEcaractere(cep),8) AS cep,
                if(length(REMOVEcaractere(telresid)) IN (8,10),REMOVEcaractere(telresid),if(length(REMOVEcaractere(celular)) IN (8,10),REMOVEcaractere(celular),NULL) ) AS telresid, 
                if(length(REMOVEcaractere(celular)) IN (9,11),REMOVEcaractere(celular),if(length(REMOVEcaractere(telresid)) IN (9,11),REMOVEcaractere(telresid),NULL)) AS celular, 
                if(validaemail(email) = 'V', email, NULL) AS email,
                UPPER(sexo) AS sexo,
                dtnascimento,
                ufnaturalidade,
                cidadenaturalidade,
                UPPER(cor) AS cor,
                UPPER(pai) AS pai,
                UPPER(trabpai) AS trabpai,
                left(UPPER(profpai),40) AS profpai,
                if(length(REMOVEcaractere(telpai)) IN (8,10),REMOVEcaractere(telpai),if(length(REMOVEcaractere(celularpai)) IN (8,10),REMOVEcaractere(celularpai),NULL) ) AS telpai, 
                if(length(REMOVEcaractere(celularpai)) IN (9,11),REMOVEcaractere(celularpai),if(length(REMOVEcaractere(telpai)) IN (9,11),REMOVEcaractere(telpai),NULL)) AS celularpai,
                if(validaemail(emailpai) = 'V', emailpai, NULL) AS emailpai,
                UPPER(mae) AS mae,
                UPPER(trabmae) AS trabmae,
                UPPER(profmae) AS profmae,
                if(length(REMOVEcaractere(telmae)) IN (8,10),REMOVEcaractere(telmae),if(length(REMOVEcaractere(celularmae)) IN (8,10),REMOVEcaractere(celularmae),NULL) ) AS telmae, 
                if(length(REMOVEcaractere(celularmae)) IN (9,11),REMOVEcaractere(celularmae),if(length(REMOVEcaractere(telmae)) IN (9,11),REMOVEcaractere(telmae),NULL)) AS celularmae,
                if(validaemail(emailmae) = 'V', emailmae, NULL) AS emailmae,
                UPPER(CONCAT(ifnull(obs,''),' / ORGÃO EMISSOR DO RG DO ALUNO: ',IFNULL(rgorgaoemissor,''),' / ORGÃO EMISSOR DO RG DO PAI: ',IFNULL(rgorgaoemissorpai,''),' / ORGÃO EMISSOR DO RG DA MÃE: ',IFNULL(rgorgaoemissormae,''),' / EMAIL ALUNO: ',IFNULL(email,''),' / EMAIL ´PAI: ',IFNULL(emailpai,''),' / EMAIL MAE: ',IFNULL(emailmae,''))) AS obs,
                lpad(REMOVEcaractere(cpf),11,'0') AS cpf,
                if(LENGTH(a.rgorgaoemissor) <=5 , CONCAT(ifnull(a.rg,''),'/',ifnull(a.rgorgaoemissor,'')), a.rg ) AS rg,
                foto, 
                senha, 
                UPPER(nomeabrev) AS nomeabrev, 
                certidaonumero, 
                certidaolivrofolha, 
                certidaocartorio, 
                UPPER(religiao) AS religiao, 
                codeducacaoespecial,
                obseducacaoespecial, 
                codsocio,
                vivopai,
                instrucaopai,
                UPPER(enderecopai) AS enderecopai,
                UPPER(bairropai) AS bairropai,
                codcidadepai, 
                codufpai, 
                left(REMOVEcaractere(ceppai),8) AS ceppai,
                vivomae, 
                UPPER(instrucaomae) AS instrucaomae, 
                UPPER(enderecomae) AS enderecomae, 
                UPPER(bairromae) AS bairromae, 
                left(REMOVEcaractere(cepmae),8) AS cepmae,
                codufmae, 
                codcidademae, 
                UPPER(ativo) AS ativo, 
                fotoPortal, 
                foneportal, 
                emailportal, 
                titeleitor, 
                zona, 
                now() as dtcadastro, 
                usercadastro, 
                dtalteracao, 
                useralteracao, 
                UPPER(nacionalidade) AS nacionalidade, 
                bloqueado, 
                UPPER(obshistfundamental) AS obshistfundamental, 
                UPPER(obshistmedio) AS obshistmedio, 
                UPPER(estadocivilpai) AS estadocivilpai, 
                UPPER(estadocivilmae) AS estadocivilmae, 
                dtnascimentopai, 
                dtnascimentomae, 
                codpaisnaturalidade, 
                alunoadotado, 
                sabeadocao, 
                paisseparados, 
                tiposanguineo, 
                planosaude, 
                alergia, 
                doencacongenita, 
                deficiencia, 
                cuidadoespecial, 
                locomocaoida, 
                locomocaovolta, 
                obsguiatransferencia, 
                UPPER(naturalidade) AS naturalidade, 
                lpad(REMOVEcaractere(cpfpai),11,'0') AS cpfpai, 
                if(LENGTH(a.rgorgaoemissorpai) <=5 , CONCAT(ifnull(a.rgpai,''),'/',ifnull(a.rgorgaoemissorpai,'')), a.rgpai ) AS rgpai,
                lpad(REMOVEcaractere(cpfmae),11,'0') AS cpfmae, 
                if(LENGTH(a.rgorgaoemissormae) <=5 , CONCAT(ifnull(a.rgmae,''),'/',ifnull(a.rgorgaoemissormae,'')), a.rgmae ) AS rgmae,
                fotoblob, 
                UPPER(estadocivil) AS estadocivil, 
                saisozinho, 
                tipocertidao, 
                certreservista, 
                UPPER(profissao) AS profissao, 
                UPPER(localtrabalho) AS localtrabalho, 
                teltrabalho, 
                numcartao, 
                enviarsms, 
                celularsms, 
                operadorasms, 
                hospital, 
                pne, 
                pnedescricao, 
                pneavaliado, 
                alunofalecido, 
                codcenso, 
                codportaria, 
                codpaisorigem, 
                zonaresidencia7024, 
                NULL AS rgorgaoemissor, 
                rgea.cod_estado AS rgestado, 
                rgdataemissao, 
                rgcomplemento, 
                modelocertidao, 
                codcartoriocertidao, 
                dtemissaocertidao, 
                NULL AS rgorgaoemissorpai, 
                rgestadopai, 
                rgdataemissaopai, 
                upper(nacionalidadepai) AS nacionalidadepai, 
                codpaisorigempai, 
                codcidadenaturalidadepai, 
                codescolaridadepai, 
                NULL AS rgorgaoemissormae,
                rgestadomae, 
                rgdataemissaomae, 
                UPPER(nacionalidademae) AS nacionalidade, 
                codpaisorigemmae, 
                codcidadenaturalidademae,
                codescolaridademae,
                codrendafamiliar,
                UPPER(obsfinanceiro) AS obsfinanceiro,
                UPPER(obsacademico) AS obsacademico,
                sexomae,
                sexopai,
                codbolsapadrao,
                diavencpadrao,
                ra,
                localbatalhaomilitar,
                categoriamilitar,
                situacaomilitar,
                dataexpedicaomilitar,
                secaoeleitor,
                dataexpedicaoeleitor,
                avatoken,
                avauserid,
                UPPER(obshistinfantil) AS obshistinfantil,
                idgoogledrive,
                codescolaridadealuno
                FROM mig_aluno_siga a
                LEFT JOIN estado rgea ON (a.rgestado = rgea.sgl_estado)
                GROUP BY a.nomealuno);", conn);
                insertAluno.ExecuteNonQuery();

                MySqlCommand updates = new MySqlCommand($@"UPDATE mig_aluno_siga a
                JOIN aluno aa ON a.nomealuno = aa.nomealuno 
                SET a.codaluno_tella = aa.codaluno;

                -- Alunos com o cpf repetido
                UPDATE aluno a
                SET a.obs = CONCAT(ifnull(a.obs,''),' | CPF DO ALUNO: ',ifnull(a.cpf,'')),
                a.cpf = NULL
                WHERE a.cpf IN (SELECT tab.cpf FROM (SELECT aa.cpf from mig_aluno_siga aa
                WHERE aa.cpf IS NOT NULL
                GROUP BY aa.cpf HAVING COUNT(1)>1) AS tab ) 
                AND LENGTH(CONCAT(ifnull(a.obs,''),' | CPF DO ALUNO: ',ifnull(a.cpf,''))) <= 600;

                UPDATE aluno a
                SET a.obsfinanceiro = CONCAT(ifnull(a.obsfinanceiro,''),' | CPF DO ALUNO: ',ifnull(a.cpf,'')),
                a.cpf = NULL
                WHERE a.cpf IN (SELECT tab.cpf FROM (SELECT aa.cpf from mig_aluno_siga aa
                WHERE aa.cpf IS NOT NULL
                GROUP BY aa.cpf HAVING COUNT(1)>1) AS tab ) 
                AND LENGTH(CONCAT(ifnull(a.obsfinanceiro,''),' | CPF DO ALUNO: ',ifnull(a.cpf,''))) <= 600;

                -- Alunos com o mesmo cpf da mãe
                UPDATE aluno a
                SET a.obs = CONCAT(IFNULL(a.obs,''),' | CPF DO ALUNO: ',IFNULL(a.cpf,'')),
                a.cpf = NULL
                WHERE a.cpf = a.cpfmae
                AND a.cpf IS NOT NULL;

                -- Alunos com o mesmo cpf do pai
                UPDATE aluno a
                SET a.obs = CONCAT(IFNULL(a.obs,''),' | CPF DO ALUNO: ',IFNULL(a.cpf,'')),
                a.cpf = NULL
                WHERE a.cpf = a.cpfpai
                AND a.cpf IS NOT NULL;

                -- Pai e mãe com o mesmo cpf 
                -- onde a mãe é o responsável 
                UPDATE aluno a
                JOIN responsavel r ON (a.cpfmae = r.cpf)
                SET a.obs = CONCAT(ifnull(a.obs,''),' | CPF DO PAI:  ',a.cpfpai) ,
                a.cpfpai = NULL
                WHERE a.cpfpai = a.cpfmae
                AND a.cpfmae IS NOT NULL
                AND a.mae = r.nomeresponsavel ;

                -- onde o pai é o responsável
                UPDATE aluno a
                JOIN responsavel r ON (a.cpfpai = r.cpf)
                SET a.obs = CONCAT(ifnull(a.obs,''),' | CPF DA MÃE:  ',a.cpfmae) ,
                a.cpfmae = NULL
                WHERE a.cpfpai = a.cpfmae
                AND a.cpfpai IS NOT NULL
                AND a.pai = r.nomeresponsavel ;

                -- quando o nome de nenhum dos dois é igual o do responsável 
                UPDATE aluno a
                SET a.obs = CONCAT(ifnull(a.obs,''),' | CPF DA MÃE:  ',a.cpfmae,' | CPF DO PAI: ',a.cpfpai),
                a.cpfmae = NULL,
                a.cpfpai = NULL
                WHERE a.cpfpai = a.cpfmae
                AND a.cpfpai IS NOT NULL;", conn);
                updates.ExecuteNonQuery();

                MessageBox.Show("Importação concluída com sucesso!");
            }
            catch (Exception err)
            {

                MessageBox.Show(err.Message);

            }
            finally
            {
                log.CommandText = "select count(1) from aluno;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

        }
    }
}


