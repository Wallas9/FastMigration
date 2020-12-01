using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Funcionario")]
    public class ImportFuncionario : IImportArgs
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
                MySqlCommand create = new MySqlCommand($@"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS mig_funcionario_siga;
				DELETE FROM funcionariounidade;				
				DELETE FROM funcionario;
				CREATE TABLE `mig_funcionario_siga` (
	            `FUNCODIGO` INT(11) NOT NULL AUTO_INCREMENT,
	            `CODESCOLARIDADE` INT(11) NULL DEFAULT NULL,
	            `CODBANCO` INT(11) NULL DEFAULT NULL,
	            `funbancoagencia` VARCHAR(10) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `funbancoconta` VARCHAR(10) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `funbancooperacao` VARCHAR(10) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CODUF` INT(11) NULL DEFAULT NULL,
	            `NOME` VARCHAR(100) NOT NULL COLLATE 'latin1_swedish_ci',
	            `FOTO` VARCHAR(150) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `SEXO` CHAR(1) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `DTNASCIMENTO` varchar(100) NULL DEFAULT NULL,
	            `NATURALIDADE` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `ESTADOCIVIL` VARCHAR(15) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CONJUGE` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `DEPENDENTES` INT(11) NULL DEFAULT NULL,
	            `ESCOLARIDADE` INT(11) NULL DEFAULT NULL,
	            `ENDERECO` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `COMPLEMENTO` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `BAIRRO` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CIDADE` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CEP` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `FONERESID` VARCHAR(10) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CELULAR` VARCHAR(11) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `FONRECADO` VARCHAR(11) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `EMAIL` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `RG` VARCHAR(30) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CPF` VARCHAR(11) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CATRABALHO` VARCHAR(15) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `SERIECTRABALHO` VARCHAR(15) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `HABILITACAO` VARCHAR(15) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CATEGORIA` VARCHAR(2) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `DTADMISSAO` DATE NULL DEFAULT NULL,
	            `DTDESLIGAMENTO` DATE NULL DEFAULT NULL,
	            `MOTIVO` VARCHAR(1000) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `OBSERVACOES` VARCHAR(2000) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `ATIVO` CHAR(1) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CARTASSINADA` CHAR(1) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CAPACETE` INT(11) NULL DEFAULT NULL,
	            `TITELEITOR` VARCHAR(15) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `TITZONA` VARCHAR(10) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `HORAENTRADA` varchar(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `HORASAIDA` varchar(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `DESCANSO` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `FGTSOPTANTE` CHAR(3) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `FGTSDTOPCAO` DATE NULL DEFAULT NULL,
	            `FGTSDTRETRATACAO` DATE NULL DEFAULT NULL,
	            `NACIONALIDADE` VARCHAR(23) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `PAI` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `MAE` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `NUNCTMILIT` VARCHAR(15) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `SERIECTMILIT` VARCHAR(10) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CATEGCTMILIT` VARCHAR(10) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `PISDTCADASTRO` DATE NULL DEFAULT NULL,
	            `PISNUMERO` VARCHAR(15) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `PISENDERECOBANCO` VARCHAR(150) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `PISCODBANCO` VARCHAR(10) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `PISCODAGENCIA` VARCHAR(10) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `PISOBS` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `DTENTRADA` DATE NULL DEFAULT NULL,
	            `LIVREGISTRO` INT(11) NULL DEFAULT NULL,
	            `RESERVA` CHAR(1) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `CODCARGO` INT(11) NULL DEFAULT NULL,
	            `PISBANCO` INT(11) NULL DEFAULT NULL,
	            `FGTSBANCO` INT(11) NULL DEFAULT NULL,
	            `FOTOPORTAL` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `LOGIN` VARCHAR(30) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `SENHA` VARCHAR(15) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `EMAILPORTAL` VARCHAR(60) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `FONEPORTAL` VARCHAR(30) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `bloqueado` CHAR(1) NULL DEFAULT 'N' COLLATE 'latin1_swedish_ci',
	            `UF` CHAR(2) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `horariobloqueio` DATETIME NULL DEFAULT NULL,
	            `ipbloqueio` varchar(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `loginbloqueio` varchar(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `localbloqueio` varchar(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `SENHAADM` VARCHAR(10) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `fotoblob` BLOB NULL DEFAULT NULL,
	            `cor` varchar(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `codpaisorigem` SMALLINT(6) NULL DEFAULT NULL,
	            `codinep` VARCHAR(12) NULL DEFAULT NULL COMMENT 'codigo do profissional no INEP' COLLATE 'latin1_swedish_ci',
	            `codcidadenaturalidade` INT(11) NULL DEFAULT NULL,
	            `zonaresidencia4006` INT(1) NULL DEFAULT NULL COMMENT '1-Urbana/2-Rural',
	            `regimecontratacao` INT(1) NULL DEFAULT NULL COMMENT '1-Concursado / 2-Contrato temporario / 3-Contrato tercerizado / 4-Contrato CLT',
	            `numcartao` varchar(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `alcunha` VARCHAR(60) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `rf` VARCHAR(10) NULL DEFAULT NULL COMMENT 'Registro Funcional do Professor da Prefeitura Municipal de São Paulo' COLLATE 'latin1_swedish_ci',
	            `qpe` VARCHAR(5) NULL DEFAULT NULL COMMENT 'evolução que o Professor possui e indicativo do que ele precisa fazer para evoluir' COLLATE 'latin1_swedish_ci',
	            `avatoken` VARCHAR(200) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `tokenfirebase` VARCHAR(256) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `chat_ativo` CHAR(1) NULL DEFAULT 'N' COLLATE 'latin1_swedish_ci',
	            `idgoogledrive` VARCHAR(100) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `wakkeuserid` CHAR(36) NULL DEFAULT NULL COMMENT 'Identificador do funcionário na Wakke Class' COLLATE 'latin1_swedish_ci',
	            PRIMARY KEY (`FUNCODIGO`) USING BTREE,
	            UNIQUE INDEX `UK_funcionario_wakkeuserid` (`wakkeuserid`) USING BTREE,
	            INDEX `FK_CODESCOLARIDADE` (`CODESCOLARIDADE`) USING BTREE,
	            INDEX `funcionario_ibfk_1` (`CODCARGO`) USING BTREE)
                COLLATE='latin1_swedish_ci'
                ENGINE=InnoDB
                AUTO_INCREMENT=1;", conn);
                create.ExecuteNonQuery();

                DataTable dtable = new DataTable();

                FbCommand MySelect = new FbCommand(@"
                select
                (p.unidade || p.codigo) as funcodigo,
                replace(p.nome, '''', '') as nome,
                replace(p.sexo, '''', '') as sexo,
                replace(p.email, '''', '') as email,
                -- p.datanasc as dtnascimento,
                EXTRACT(YEAR FROM p.datanasc) || '-' || EXTRACT(MONTH FROM p.datanasc) || '-' || EXTRACT(DAY FROM p.datanasc) as dtnascimento,
                replace(p.naturalidade, '''', '') as naturalidade,
                replace((cast(p.endereco as varchar(100)) || cast(', ' as varchar(2)) || cast(COALESCE(p.numero, '') as varchar(10))), '''', '') as endereco,
                replace(left(p.complemento, 10), '''', '') as complemento,
                replace(p.bairro, '''', '') as bairro,
                -- p.cidade as cidadeoriginal,
				replace((select c.cod_cidade from cidade_tella c where c.nom_cidade like upper(p.cidade) and c.nom_estado = p.estado), '''', '') as cidade,
                replace(replace(replace(replace(replace(replace(p.cep, '(', ''), '.', ''), '-', ''), '/', ''), ')', ''), '''', '') as cep,
                1 as codcargo,
                'S' as ativo
                from sigprofe p
                where p.unidade is not null; ", conn2);

                FbDataAdapter adapter = new FbDataAdapter(MySelect);
                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();

                queryBuilder.Append("INSERT INTO mig_funcionario_siga(funcodigo, nome, sexo, email, dtnascimento, naturalidade, endereco, complemento, bairro, cidade, cep, codcargo, ativo) VALUES ");

                for (int i = 0; i < dtable.Rows.Count; i++)
                {

                    queryBuilder.Append($@"('{dtable.Rows[i]["FUNCODIGO"]}' , '{dtable.Rows[i]["nome"]}' , '{dtable.Rows[i]["sexo"]}' , '{dtable.Rows[i]["email"]}' , '{dtable.Rows[i]["dtnascimento"]}' , '{dtable.Rows[i]["naturalidade"]}' , '{dtable.Rows[i]["endereco"]}' , '{dtable.Rows[i]["complemento"]}' , '{dtable.Rows[i]["bairro"]}' , '{dtable.Rows[i]["cidade"]}' , '{dtable.Rows[i]["cep"]}' , '{dtable.Rows[i]["codcargo"]}' , '{dtable.Rows[i]["ativo"]}'), ");

                }

                queryBuilder.Remove(queryBuilder.Length - 2, 2);

                MySqlCommand query = new MySqlCommand(queryBuilder.ToString(), conn);
                query.ExecuteNonQuery();

                MySqlCommand MySelect2 = new MySqlCommand(@"SET FOREIGN_KEY_CHECKS = 0; 

				UPDATE mig_funcionario_siga SET nome = NULL WHERE nome = '';
				UPDATE mig_funcionario_siga SET sexo = NULL WHERE sexo = '';
				UPDATE mig_funcionario_siga SET email = NULL WHERE email = '';
				UPDATE mig_funcionario_siga SET dtnascimento = NULL WHERE dtnascimento = '';
				UPDATE mig_funcionario_siga SET naturalidade = NULL WHERE naturalidade = '';
				UPDATE mig_funcionario_siga SET endereco = NULL WHERE endereco = '';
				UPDATE mig_funcionario_siga SET complemento = NULL WHERE complemento = '';
				UPDATE mig_funcionario_siga SET bairro = NULL WHERE bairro = '';
				UPDATE mig_funcionario_siga SET cidade = NULL WHERE cidade = '';
				UPDATE mig_funcionario_siga SET cep = NULL WHERE cep = '';
				UPDATE mig_funcionario_siga SET codcargo = NULL WHERE codcargo = '';
				UPDATE mig_funcionario_siga SET ativo = NULL WHERE ativo = '';

				INSERT INTO funcionario (funcodigo, nome, sexo, email, dtnascimento,naturalidade, endereco, complemento, bairro, cidade, cep, codcargo, ativo)
				SELECT funcodigo, nome, sexo, email, dtnascimento,naturalidade, endereco, complemento, bairro, cidade, cep, codcargo, ativo FROM mig_funcionario_siga fs GROUP BY fs.nome;
				
				INSERT INTO funcionariounidade (codfuncionario,codunidade) (SELECT funcodigo, 1 FROM funcionario)", conn);

                MySelect2.ExecuteNonQuery();

                MessageBox.Show("Importação concluída com sucesso!");
            }
            catch (Exception err)
            {

                MessageBox.Show(err.Message);

            }
            finally
            {
                log.CommandText = "select count(1) from funcionario;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

        }
    }
}
