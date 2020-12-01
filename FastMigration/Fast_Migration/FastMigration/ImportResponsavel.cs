
using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using System.Data.Common;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Responsavel")]
    public class ImportResponsavel : IImportArgs
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
            try
            {
                MySqlCommand create = new MySqlCommand(@"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS mig_responsavel_siga;
                DELETE FROM responsavel;
                CREATE TABLE `mig_responsavel_siga` (
	            `cpf` VARCHAR(14) NOT NULL,
	            `nomeresponsavel` VARCHAR(100) NOT NULL,
	            `profissao` VARCHAR(40) NULL DEFAULT NULL,
	            `localtrabalho` VARCHAR(100) NULL DEFAULT NULL,
	            `teltrabalho` VARCHAR(11) NULL DEFAULT NULL,
	            `telresid` VARCHAR(11) NULL DEFAULT NULL,
	            `celular` VARCHAR(16) NULL DEFAULT NULL,
	            `email` VARCHAR(100) NULL DEFAULT NULL,
	            `rg` varchar(100) NULL DEFAULT NULL,
	            `endereco` VARCHAR(120) NULL DEFAULT NULL,
	            `bairro` VARCHAR(60) NULL DEFAULT NULL,
	            `cidade` varchar(100) NULL DEFAULT NULL,
	            `uf` INT(10) UNSIGNED NULL DEFAULT NULL,
	            `cep` VARCHAR(8) NULL DEFAULT NULL,
	            `rendaliquida` DECIMAL(10,2) NULL DEFAULT NULL,
	            `sexo` CHAR(1) NULL DEFAULT NULL,
	            `estadocivil` varchar(100) NULL DEFAULT NULL,
	            `dtnascimento` varchar(100) NULL DEFAULT NULL,
	            `escolaridade` VARCHAR(40) NULL DEFAULT NULL,
	            `pai` VARCHAR(100) NULL DEFAULT NULL,
	            `mae` VARCHAR(100) NULL DEFAULT NULL,
	            `enderecocom` VARCHAR(120) NULL DEFAULT NULL,
	            `bairrocom` VARCHAR(30) NULL DEFAULT NULL,
	            `cepcom` VARCHAR(8) NULL DEFAULT NULL,
	            `codcidadecom` INT(11) NULL DEFAULT NULL,
	            `cargo` VARCHAR(30) NULL DEFAULT NULL,
	            `senha` VARCHAR(15) NULL DEFAULT NULL,
	            `login` varchar(100) NULL DEFAULT NULL,
	            `fotoportal` VARCHAR(120) NULL DEFAULT NULL,
	            `nacionalidade` varchar(100) NULL DEFAULT NULL,
	            `naturalidade` varchar(100) NULL DEFAULT NULL,
	            `obs` VARCHAR(200) NULL DEFAULT NULL,
	            `rgorgaoemissor` INT(2) NULL DEFAULT NULL COMMENT 'Orgao Emissor da indentidade. Relaciona com a tabela orgaoemissoridentidade',
	            `rgestado` SMALLINT(6) NULL DEFAULT NULL COMMENT 'Estado Identidade',
	            `rgdataemissao` DATE NULL DEFAULT NULL COMMENT 'Data de Emissao RG',
	            `codpaisorigem` SMALLINT(6) NULL DEFAULT NULL,
	            `codcidadenaturalidade` INT(11) NULL DEFAULT NULL,
	            `codescolaridade` INT(11) NULL DEFAULT NULL,
	            `codfatura` INT(11) NULL DEFAULT NULL,
	            `agrupamentofatura` CHAR(1) NULL DEFAULT 'R' COMMENT 'R=responsavel; A=aluno;',
	            `placa` varchar(100) NULL DEFAULT NULL,
	            `veiculo` varchar(100) NULL DEFAULT NULL,
	            `fotoblob` BLOB NULL,
	            `dtcadastro` varchar(100) NULL DEFAULT NULL,
	            `dtalteracao` varchar(100) NULL DEFAULT NULL,
	            `cadastradopor` VARCHAR(30) NULL DEFAULT NULL,
	            `alteradopor` VARCHAR(30) NULL DEFAULT NULL,
	            `codbanco` INT(11) NULL DEFAULT NULL,
	            `agencia` VARCHAR(5) NULL DEFAULT NULL,
	            `conta` VARCHAR(10) NULL DEFAULT NULL,
	            `debitoautomaticoativo` CHAR(1) NULL DEFAULT 'N',
	            `clienteiugu` VARCHAR(40) NULL DEFAULT NULL COMMENT 'Id do responsavel no cadastro de clientes no iugu',
	            `inscricaoestadual` varchar(100) NULL DEFAULT NULL COMMENT 'Inscrição Estadual da empresa',
	            `tokenfirebase` VARCHAR(256) NULL DEFAULT NULL)
                COLLATE='latin1_swedish_ci'
                ENGINE=InnoDB
                AUTO_INCREMENT=1
                ;", conn);
                create.ExecuteNonQuery();

                FbCommand respacademico = new FbCommand(@"
				select
                max(replace(replace(a.cpf_rp ,'.',''),'-','')) as cpf ,
                replace(upper(max(a.resp_ped)), '''', '') as nomeresponsavel,
                left(replace(replace(upper(max(a.profi_rp)),';',','), '\', ''),40) as profissao,
                max(iif(char_length(left(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(a.foner_rp,'.',''),'-',''),'/',''),' ',''),'_',''),';',''),')',''),'(',''), ' ', ''),'a',''),'b',''),'c',''),'d',''),'e',''),'f',''),'g',''),'h',''),'i',''),'j',''),'k',''),'l',''),'m',''),'n',''),'o',''),'p',''),'r',''),'s',''),'t',''),'u',''),'x',''),'z',''),'w',''),'y',''),'|',''),'\',''),',',''),'$',''),'!',''),'@',''),'+',''),'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'R',''),'S',''),'T',''),'U',''),'V',''),'X',''),'Z',''),'W',''),'Y',''),'á',''),'à',''),'ã',''),'â',''),'Á',''),'À',''),'Ã',''),'Â',''),'é',''),'è',''),'ê',''),'É',''),'È',''),'Ê',''),'í',''),'ì',''),'î',''),'Í',''),'Ì',''),'Î',''),'ó',''),'ò',''),'ô',''),'õ',''),'Ó',''),'Ò',''),'Ô',''),'Õ',''),'ú',''),'ù',''),'û',''),'Ú',''),'Ù',''),'Û',''),'v',''),'q',''),'Q',''),10)) < 10, '', left(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(a.foner_rp,'.',''),'-',''),'/',''),' ',''),'_',''),';',''),')',''),'(',''), ' ', ''),'a',''),'b',''),'c',''),'d',''),'e',''),'f',''),'g',''),'h',''),'i',''),'j',''),'k',''),'l',''),'m',''),'n',''),'o',''),'p',''),'r',''),'s',''),'t',''),'u',''),'x',''),'z',''),'w',''),'y',''),'|',''),'\',''),',',''),'$',''),'!',''),'@',''),'+',''),'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'R',''),'S',''),'T',''),'U',''),'V',''),'X',''),'Z',''),'W',''),'Y',''),'á',''),'à',''),'ã',''),'â',''),'Á',''),'À',''),'Ã',''),'Â',''),'é',''),'è',''),'ê',''),'É',''),'È',''),'Ê',''),'í',''),'ì',''),'î',''),'Í',''),'Ì',''),'Î',''),'ó',''),'ò',''),'ô',''),'õ',''),'Ó',''),'Ò',''),'Ô',''),'Õ',''),'ú',''),'ù',''),'û',''),'Ú',''),'Ù',''),'Û',''),'v',''),'q',''),'Q',''),10))) as telresid,
                max(iif(char_length(left(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(a.fonecelrp,'.',''),'-',''),'/',''),' ',''),'_',''),';',''),')',''),'(',''), ' ', ''),'a',''),'b',''),'c',''),'d',''),'e',''),'f',''),'g',''),'h',''),'i',''),'j',''),'k',''),'l',''),'m',''),'n',''),'o',''),'p',''),'r',''),'s',''),'t',''),'u',''),'x',''),'z',''),'w',''),'y',''),'|',''),'\',''),',',''),'$',''),'!',''),'@',''),'+',''),'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'R',''),'S',''),'T',''),'U',''),'V',''),'X',''),'Z',''),'W',''),'Y',''),'á',''),'à',''),'ã',''),'â',''),'Á',''),'À',''),'Ã',''),'Â',''),'é',''),'è',''),'ê',''),'É',''),'È',''),'Ê',''),'í',''),'ì',''),'î',''),'Í',''),'Ì',''),'Î',''),'ó',''),'ò',''),'ô',''),'õ',''),'Ó',''),'Ò',''),'Ô',''),'Õ',''),'ú',''),'ù',''),'û',''),'Ú',''),'Ù',''),'Û',''),'v',''),'q',''),'Q',''),11)) < 11, '', left(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(a.fonecelrp,'.',''),'-',''),'/',''),' ',''),'_',''),';',''),')',''),'(',''), ' ', ''),'a',''),'b',''),'c',''),'d',''),'e',''),'f',''),'g',''),'h',''),'i',''),'j',''),'k',''),'l',''),'m',''),'n',''),'o',''),'p',''),'r',''),'s',''),'t',''),'u',''),'x',''),'z',''),'w',''),'y',''),'|',''),'\',''),',',''),'$',''),'!',''),'@',''),'+',''),'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'R',''),'S',''),'T',''),'U',''),'V',''),'X',''),'Z',''),'W',''),'Y',''),'á',''),'à',''),'ã',''),'â',''),'Á',''),'À',''),'Ã',''),'Â',''),'é',''),'è',''),'ê',''),'É',''),'È',''),'Ê',''),'í',''),'ì',''),'î',''),'Í',''),'Ì',''),'Î',''),'ó',''),'ò',''),'ô',''),'õ',''),'Ó',''),'Ò',''),'Ô',''),'Õ',''),'ú',''),'ù',''),'û',''),'Ú',''),'Ù',''),'Û',''),'v',''),'q',''),'Q',''),11))) as celular,
                max(a.email_rp) as email,
                max(a.rg_rp) as rg,
                replace(replace(upper(max(coalesce(a.ender_rp,'') || ' ' || coalesce(a.compl_rp,''))),';',','), '''', '') as endereco,
                replace(replace(upper(max(a.bairr_rp)),';',','), '''', '') as bairro,
                max(c.cod_cidade) as cidade,
                max(left(replace(a.cep_rp,'-',''),8)) as cep,
                max(a.estcivrp) as estadocivil,
                max(EXTRACT(YEAR FROM a.nascresp_p) || '-' || EXTRACT(MONTH FROM a.nascresp_p) || '-' || EXTRACT(DAY FROM a.nascresp_p)) as dtnascimento,
                max('BRASILEIRA') as nacionalidade,
                max(EXTRACT(YEAR FROM current_timestamp) || '-' || EXTRACT(MONTH FROM current_timestamp) || '-' || EXTRACT(DAY FROM current_timestamp)) as dtcadastro,
                max('MIGRACAO') as cadastradopor
                from sigaluno a
                left join cidade_tella c on c.nom_cidade = upper(a.cidade_rp)
                where a.cpf_rp is not null
                and a.cpf_rp not in ('')
                group by a.cpf_rp ;", conn2);

                DataTable dtablerespacad = new DataTable();

                FbDataAdapter adapterrespacad = new FbDataAdapter(respacademico);
                adapterrespacad.Fill(dtablerespacad);

                StringBuilder queryBuilderrespacad = new StringBuilder();
                queryBuilderrespacad.Append("INSERT INTO mig_responsavel_siga (cpf,nomeresponsavel,profissao,telresid,celular,rg,endereco,bairro,cidade,cep,estadocivil,dtnascimento,nacionalidade,dtcadastro,cadastradopor) VALUES ");

                for (int i = 0; i < dtablerespacad.Rows.Count; i++)
                {
                    queryBuilderrespacad.Append($@"('{dtablerespacad.Rows[i]["cpf"]}' , '{dtablerespacad.Rows[i]["nomeresponsavel"]}' , '{dtablerespacad.Rows[i]["profissao"]}' , '{dtablerespacad.Rows[i]["telresid"]}' , '{dtablerespacad.Rows[i]["celular"]}' , '{dtablerespacad.Rows[i]["rg"]}' , '{dtablerespacad.Rows[i]["endereco"]}' , '{dtablerespacad.Rows[i]["bairro"]}' , '{dtablerespacad.Rows[i]["cidade"]}' , '{dtablerespacad.Rows[i]["cep"]}' , '{dtablerespacad.Rows[i]["estadocivil"]}' , '{dtablerespacad.Rows[i]["dtnascimento"]}' , '{dtablerespacad.Rows[i]["nacionalidade"]}' , '{dtablerespacad.Rows[i]["dtcadastro"]}' , '{dtablerespacad.Rows[i]["cadastradopor"]}'), ");
                }
                queryBuilderrespacad.Remove(queryBuilderrespacad.Length - 2, 2);

                MySqlCommand queryrespacad = new MySqlCommand(queryBuilderrespacad.ToString(), conn);
                queryrespacad.ExecuteNonQuery();

                FbCommand respfinanceiro = new FbCommand(@"
				select
                max(replace(replace(a.cpfresp ,'.',''),'-','')) as cpf ,
                replace(upper(max(a.pagtoresp)),'''','') as nomeresponsavel,
                replace(left(replace(upper(max(a.profiresp)),';',','),40),'''','') as profissao,
                max(iif(char_length(left(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(a.foneresp,'.',''),'-',''),'/',''),' ',''),'_',''),';',''),')',''),'(',''), ' ', ''),'a',''),'b',''),'c',''),'d',''),'e',''),'f',''),'g',''),'h',''),'i',''),'j',''),'k',''),'l',''),'m',''),'n',''),'o',''),'p',''),'r',''),'s',''),'t',''),'u',''),'x',''),'z',''),'w',''),'y',''),'|',''),'\',''),',',''),'$',''),'!',''),'@',''),'+',''),'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'R',''),'S',''),'T',''),'U',''),'V',''),'X',''),'Z',''),'W',''),'Y',''),'á',''),'à',''),'ã',''),'â',''),'Á',''),'À',''),'Ã',''),'Â',''),'é',''),'è',''),'ê',''),'É',''),'È',''),'Ê',''),'í',''),'ì',''),'î',''),'Í',''),'Ì',''),'Î',''),'ó',''),'ò',''),'ô',''),'õ',''),'Ó',''),'Ò',''),'Ô',''),'Õ',''),'ú',''),'ù',''),'û',''),'Ú',''),'Ù',''),'Û',''),'v',''),'q',''),'Q',''),10)) < 10, '', left(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(a.foner_rp,'.',''),'-',''),'/',''),' ',''),'_',''),';',''),')',''),'(',''), ' ', ''),'a',''),'b',''),'c',''),'d',''),'e',''),'f',''),'g',''),'h',''),'i',''),'j',''),'k',''),'l',''),'m',''),'n',''),'o',''),'p',''),'r',''),'s',''),'t',''),'u',''),'x',''),'z',''),'w',''),'y',''),'|',''),'\',''),',',''),'$',''),'!',''),'@',''),'+',''),'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'R',''),'S',''),'T',''),'U',''),'V',''),'X',''),'Z',''),'W',''),'Y',''),'á',''),'à',''),'ã',''),'â',''),'Á',''),'À',''),'Ã',''),'Â',''),'é',''),'è',''),'ê',''),'É',''),'È',''),'Ê',''),'í',''),'ì',''),'î',''),'Í',''),'Ì',''),'Î',''),'ó',''),'ò',''),'ô',''),'õ',''),'Ó',''),'Ò',''),'Ô',''),'Õ',''),'ú',''),'ù',''),'û',''),'Ú',''),'Ù',''),'Û',''),'v',''),'q',''),'Q',''),10))) as telresid,
                max(iif(char_length(left(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(a.fonecelresp,'.',''),'-',''),'/',''),' ',''),'_',''),';',''),')',''),'(',''), ' ', ''),'a',''),'b',''),'c',''),'d',''),'e',''),'f',''),'g',''),'h',''),'i',''),'j',''),'k',''),'l',''),'m',''),'n',''),'o',''),'p',''),'r',''),'s',''),'t',''),'u',''),'x',''),'z',''),'w',''),'y',''),'|',''),'\',''),',',''),'$',''),'!',''),'@',''),'+',''),'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'R',''),'S',''),'T',''),'U',''),'V',''),'X',''),'Z',''),'W',''),'Y',''),'á',''),'à',''),'ã',''),'â',''),'Á',''),'À',''),'Ã',''),'Â',''),'é',''),'è',''),'ê',''),'É',''),'È',''),'Ê',''),'í',''),'ì',''),'î',''),'Í',''),'Ì',''),'Î',''),'ó',''),'ò',''),'ô',''),'õ',''),'Ó',''),'Ò',''),'Ô',''),'Õ',''),'ú',''),'ù',''),'û',''),'Ú',''),'Ù',''),'Û',''),'v',''),'q',''),'Q',''),11)) < 11, '', left(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(a.fonecelrp,'.',''),'-',''),'/',''),' ',''),'_',''),';',''),')',''),'(',''), ' ', ''),'a',''),'b',''),'c',''),'d',''),'e',''),'f',''),'g',''),'h',''),'i',''),'j',''),'k',''),'l',''),'m',''),'n',''),'o',''),'p',''),'r',''),'s',''),'t',''),'u',''),'x',''),'z',''),'w',''),'y',''),'|',''),'\',''),',',''),'$',''),'!',''),'@',''),'+',''),'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'R',''),'S',''),'T',''),'U',''),'V',''),'X',''),'Z',''),'W',''),'Y',''),'á',''),'à',''),'ã',''),'â',''),'Á',''),'À',''),'Ã',''),'Â',''),'é',''),'è',''),'ê',''),'É',''),'È',''),'Ê',''),'í',''),'ì',''),'î',''),'Í',''),'Ì',''),'Î',''),'ó',''),'ò',''),'ô',''),'õ',''),'Ó',''),'Ò',''),'Ô',''),'Õ',''),'ú',''),'ù',''),'û',''),'Ú',''),'Ù',''),'Û',''),'v',''),'q',''),'Q',''),11))) as celular,
                max(a.emailresp) as email,
                max(a.rgresp) as rg,
                replace(replace(upper(max(coalesce(a.endresp,'') || ' ' || coalesce(a.comresp,''))),';',','),'''','') as endereco,
                replace(replace(upper(max(a.bairesp)),';',','), '''', '') as bairro,
                max(c.cod_cidade) as cidade,
                max(left(replace(a.cepresp,'-',''),8)) as cep,
                max(a.estcivresp) as estadocivil,
                max(EXTRACT(YEAR FROM a.datnasresp) || '-' || EXTRACT(MONTH FROM a.datnasresp) || '-' || EXTRACT(DAY FROM a.datnasresp)) as dtnascimento,
                max('BRASILEIRA') as nacionalidade,
                max(EXTRACT(YEAR FROM current_timestamp) || '-' || EXTRACT(MONTH FROM current_timestamp) || '-' || EXTRACT(DAY FROM current_timestamp)) as dtcadastro,
                max('MIGRACAO') as cadastradopor
                from sigaluno a
                left join cidade_tella c on c.nom_cidade = upper(a.cidresp)
                where a.cpfresp is not null
                and a.cpfresp not in ('')
                group by a.cpfresp;", conn2);

                DataTable dtablerespfin = new DataTable();

                FbDataAdapter adapterrespfin = new FbDataAdapter(respfinanceiro);
                adapterrespfin.Fill(dtablerespfin);

                StringBuilder queryBuilderrespfin = new StringBuilder();
                queryBuilderrespfin.Append("INSERT INTO mig_responsavel_siga (cpf,nomeresponsavel,profissao,telresid,celular,rg,endereco,bairro,cidade,cep,estadocivil,dtnascimento,nacionalidade,dtcadastro,cadastradopor) VALUES ");

                for (int i = 0; i < dtablerespfin.Rows.Count; i++)
                {
                    queryBuilderrespfin.Append($@"('{dtablerespfin.Rows[i]["cpf"]}' , '{dtablerespfin.Rows[i]["nomeresponsavel"]}' , '{dtablerespfin.Rows[i]["profissao"]}' , '{dtablerespfin.Rows[i]["telresid"]}' , '{dtablerespfin.Rows[i]["celular"]}' , '{dtablerespfin.Rows[i]["rg"]}' , '{dtablerespfin.Rows[i]["endereco"]}' , '{dtablerespfin.Rows[i]["bairro"]}' , '{dtablerespfin.Rows[i]["cidade"]}' , '{dtablerespfin.Rows[i]["cep"]}' , '{dtablerespfin.Rows[i]["estadocivil"]}' , '{dtablerespfin.Rows[i]["dtnascimento"]}' , '{dtablerespfin.Rows[i]["nacionalidade"]}' , '{dtablerespfin.Rows[i]["dtcadastro"]}' , '{dtablerespfin.Rows[i]["cadastradopor"]}'), ");
                }
                queryBuilderrespfin.Remove(queryBuilderrespfin.Length - 2, 2);

                MySqlCommand queryrespfin = new MySqlCommand(queryBuilderrespfin.ToString(), conn);
                queryrespfin.ExecuteNonQuery();

                //responsaveis que nao entraram na query de cima
                DataTable dtable = new DataTable();

                FbCommand MySelect = new FbCommand(@"select
                s.cpfresp as cpf ,
                replace(replace(replace(s.pagtoresp,'''',''),'\',''),';','') as nomeresponsavel,
                replace(replace(replace(s.profiresp,'''',''),'\',''),';','') as profissao,
                replace(s.foneresp,';','|') as telresid,
                replace(s.fonecelresp,';','|') as celular,
                s.emailresp as email,
                s.rgresp as rg,
                replace(coalesce(s.endresp,'') || ' ' || coalesce(s.comresp,''), '''', '') as endereco,
                replace(s.bairesp, '''', '') as bairro,
                5213 as cidade,
                left(replace(s.cepresp,'-',''),8) as cep,
                replace(replace(replace(s.estcivresp,'''',''),'\',''),';','') as estadocivil,
                EXTRACT(YEAR FROM s.nascresp_p) || '-' || EXTRACT(MONTH FROM s.nascresp_p) || '-' || EXTRACT(DAY FROM s.nascresp_p) as dtnascimento,
                'BRASILEIRA' as nacionalidade,
                EXTRACT(YEAR FROM current_timestamp) || '-' || EXTRACT(MONTH FROM current_timestamp) || '-' || EXTRACT(DAY FROM current_timestamp) as dtcadastro,
                'MIGRACAO' as cadastradopor
                from siggeral s
                where s.cpfresp in ('');", conn2);

                FbDataAdapter adapter = new FbDataAdapter(MySelect);
                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();

                queryBuilder.Append("INSERT INTO mig_responsavel_siga (cpf,nomeresponsavel,profissao,telresid,celular,email,rg,endereco,bairro,cidade,cep,estadocivil,dtnascimento,nacionalidade,dtcadastro,cadastradopor) VALUES ");

                for (int i = 0; i < dtable.Rows.Count; i++)
                {
                    queryBuilder.Append($@"('{dtable.Rows[i]["cpf"]}' , '{dtable.Rows[i]["nomeresponsavel"]}' , '{dtable.Rows[i]["profissao"]}' , '{dtable.Rows[i]["telresid"]}' , '{dtable.Rows[i]["celular"]}' , '{dtable.Rows[i]["email"]}' , '{dtable.Rows[i]["rg"]}' , '{dtable.Rows[i]["endereco"]}' , '{dtable.Rows[i]["bairro"]}' , '{dtable.Rows[i]["cidade"]}' , '{dtable.Rows[i]["cep"]}' , '{dtable.Rows[i]["estadocivil"]}' , '{dtable.Rows[i]["dtnascimento"]}' , '{dtable.Rows[i]["nacionalidade"]}' , '{dtable.Rows[i]["dtcadastro"]}' , '{dtable.Rows[i]["cadastradopor"]}'), ");
                }
                queryBuilder.Remove(queryBuilder.Length - 2, 2);

                MySqlCommand query = new MySqlCommand(queryBuilder.ToString(), conn);
                query.ExecuteNonQuery();


                MySqlCommand update = new MySqlCommand(@$"UPDATE mig_responsavel_siga r 
                SET r.cpf = LPAD(r.cpf,11,'0')
                WHERE length(r.cpf) < 11;

                UPDATE mig_responsavel_siga set dtnascimento = NULL where dtnascimento = '0';
                UPDATE mig_responsavel_siga set dtcadastro = NULL where dtcadastro = '0';
                update mig_responsavel_siga set dtnascimento = null where dtnascimento = ''; 
                update mig_responsavel_siga set dtcadastro = null where dtcadastro = ''; 
                update mig_responsavel_siga set dtalteracao = null where dtalteracao = '';
                update mig_responsavel_siga set cidade = null where cidade = '';", conn);
                update.ExecuteNonQuery();

                MySqlCommand execute = new MySqlCommand($@" INSERT INTO responsavel (cpf, nomeresponsavel, profissao, localtrabalho, teltrabalho, telresid, celular, email, rg, endereco, bairro, cidade, uf, cep, rendaliquida, sexo, estadocivil, dtnascimento, escolaridade, pai, mae, enderecocom, bairrocom, cepcom, codcidadecom, cargo, senha, login, fotoportal, nacionalidade, naturalidade, obs, rgorgaoemissor, rgestado, rgdataemissao, codpaisorigem, codcidadenaturalidade, codescolaridade, codfatura, agrupamentofatura, placa, veiculo, fotoblob, dtcadastro, dtalteracao, cadastradopor, alteradopor, codbanco, agencia, conta, debitoautomaticoativo, clienteiugu, inscricaoestadual, tokenfirebase )
                (SELECT cpf, nomeresponsavel, profissao, localtrabalho, teltrabalho, telresid, celular, email, rg, endereco, bairro, cidade, uf, cep, rendaliquida, sexo, estadocivil, dtnascimento, escolaridade, pai, mae, enderecocom, bairrocom, cepcom, codcidadecom, cargo, senha, login, fotoportal, nacionalidade, naturalidade, obs, rgorgaoemissor, rgestado, rgdataemissao, codpaisorigem, codcidadenaturalidade, codescolaridade, codfatura, agrupamentofatura, placa, veiculo, fotoblob, dtcadastro, dtalteracao, cadastradopor, alteradopor, codbanco, agencia, conta, debitoautomaticoativo, clienteiugu, inscricaoestadual, tokenfirebase 
                FROM mig_responsavel_siga r
                  WHERE r.cpf NOT LIKE '%0%'
                GROUP BY r.cpf 
                );", conn);
                execute.ExecuteNonQuery();

                MessageBox.Show("Importação concluída com sucesso!");

            }
            catch (Exception err)
            {

                MessageBox.Show(err.Message);
            }
            finally
            {
                log.CommandText = "select count(1) from responsavel;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

        }
    }
}
