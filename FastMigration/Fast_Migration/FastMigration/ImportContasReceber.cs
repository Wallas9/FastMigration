using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using MySql.Data.MySqlClient;
using FirebirdSql.Data.FirebirdClient;
using System.Windows.Forms;
using FastMigration.Logs;
using System.IO;

namespace FastMigration.Metodos
{
    [ImportFor("ContasReceber")]
    public class ImportContasReceber : IImportArgs
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
                MySqlCommand mysql = new MySqlCommand("", conn);

                FbCommand exportfb = new FbCommand("", conn2);

                exportfb.CommandTimeout = 86400;
                mysql.CommandTimeout = 86400;

                DataTable dr = new DataTable();

                mysql.CommandText = $@"SET FOREIGN_KEY_CHECKS = 0; 
                                    DROP TABLE IF EXISTS mig_contasreceber_siga;
                                    DELETE FROM contasreceber;
                                    CREATE TABLE `mig_contasreceber_siga` (
	                                `codrec` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	                                `codmatricula` varchar(100) NULL DEFAULT NULL,
	                                `parcela` varchar(100) NULL DEFAULT NULL,
	                                `totparcela` varchar(100) NULL DEFAULT NULL,
	                                `valorparcela` varchar(100) NULL DEFAULT NULL,
	                                `vencimento` varchar(100) NULL DEFAULT NULL,
	                                `desconto` varchar(100) NULL DEFAULT NULL,
	                                `carenciadesconto` varchar(100) NULL DEFAULT NULL,
	                                `vencimentodesconto` varchar(100) NULL DEFAULT NULL,
	                                `desconto2` varchar(100) NULL DEFAULT NULL,
	                                `carenciadesconto2` varchar(100) NULL DEFAULT NULL,
	                                `vencimentodesconto2` varchar(100) NULL DEFAULT NULL,
	                                `multa` varchar(100) NULL DEFAULT NULL,
	                                `juros` varchar(100) NULL DEFAULT NULL,
	                                `carenciamulta` varchar(100) NULL DEFAULT NULL,
	                                `vencimentomulta` varchar(100) NULL DEFAULT NULL,
	                                `dtpagamento` varchar(100) NULL DEFAULT NULL,
	                                `valorpago` varchar(100) NULL DEFAULT NULL,
	                                `codunidade` varchar(100) NOT NULL,
	                                `anoletivo` varchar(100) NOT NULL,
	                                `codreceita` varchar(100) NULL DEFAULT NULL,
	                                `cobbancaria` CHAR(1) NULL DEFAULT NULL,
	                                `codaluno` varchar(100) NOT NULL,
	                                `nossonumero` VARCHAR(100) NULL DEFAULT NULL,
	                                 `avulsa` CHAR(1) NULL DEFAULT NULL,
	                                `cadastradopor` VARCHAR(100) NULL DEFAULT NULL,
	                                 `dtcadastro` varchar(100) NULL DEFAULT NULL,
	                                `valorparcelaoriginal` varchar(100) NULL DEFAULT NULL,
	                                `descontooriginal` varchar(100) NULL DEFAULT NULL,
  	                                `vencimentooriginal` varchar(100) NULL DEFAULT NULL,
	                                `valorbruto` varchar(100) NULL DEFAULT NULL,
	                                `descricao` VARCHAR(100) NULL DEFAULT NULL,
	                                 `cpf` VARCHAR(14) NULL DEFAULT NULL,
	                                 `receitaoriginal` varchar(100) NULL DEFAULT NULL,
	                                 `valor` varchar(100) NULL DEFAULT NULL,
	                                 `statusparcela` VARCHAR(20) NULL DEFAULT NULL,
	                                 `codfatura_aux` varchar(100) NULL DEFAULT NULL,
	                                `agencia_aux` varchar(100) NULL DEFAULT NULL,
	                                `conta_aux` varchar(100) NULL DEFAULT NULL,
	                                PRIMARY KEY (`codrec`) USING BTREE,
                                    INDEX `idx_cpf_aux` (`cpf`),
                                    INDEX `idx_codaluno_aux` (`codaluno`),
                                    INDEX `idx_agencia_aux` (`agencia_aux`),
                                    INDEX `idx_conta_aux` (`conta_aux`),
                                    INDEX `idx_anoletivo_aux` (`anoletivo`))
                                    COLLATE='latin1_swedish_ci'
                                    ENGINE=InnoDB
                                    AUTO_INCREMENT=1;";
                mysql.ExecuteNonQuery();

                exportfb.CommandText = @"select 
                                    sa.codigo as codrec,
                                    coalesce(max(cast(sa.codaluno as varchar(15))),null,'\N') as codmatricula,
                                    coalesce(max(sa.numero),NULL,'\N') as parcela,
                                    coalesce(cast(max(coalesce((select Max(iif(pp.parcelas = 0, 1, pp.parcelas)) from sigparce pp where pp.codigo = sa.tipo), 1)) as varchar(5)),NULL,'\N') as totparcela,
                                    coalesce(replace(cast(max(sa.valortit) as varchar(15)),',', '.'),NULL,'\N') as valorparcela,
                                    coalesce(max(EXTRACT(YEAR FROM sa.vencto1) || '-' || EXTRACT(MONTH FROM sa.vencto1) || '-' || EXTRACT(DAY FROM sa.vencto1)),NULL,'\N') as vencimento,
                                    coalesce(replace(cast(max((sa.valortit - sa.valor1)) as varchar(15)),',', '.'),NULL,'\N') as desconto,
                                    max(0) as carenciadesconto,
                                    coalesce(max(EXTRACT(YEAR FROM sa.vencto1) || '-' || EXTRACT(MONTH FROM sa.vencto1) || '-' || EXTRACT(DAY FROM sa.vencto1)),NULL,'\N') as vencimentodesconto,
                                    coalesce(replace(cast(max((sa.valortit - sa.valor2)) as varchar(15)),',', '.'),NULL,'\N') as desconto2,
                                    max(0) as carenciadesconto2,
                                    coalesce(max(EXTRACT(YEAR FROM sa.vencto2) || '-' || EXTRACT(MONTH FROM sa.vencto2) || '-' || EXTRACT(DAY FROM sa.vencto2)), NULL, '\N') as vencimentodesconto2,
                                    coalesce(replace(cast(max(sa.multa) as varchar(15)),',', '.'),NULL,'\N') as multa,
                                    coalesce(replace(cast(max(sa.jurosdia) as varchar(15)),',', '.'),NULL,'\N') as juros,
                                    max(0) as carenciamulta,
                                    coalesce(max(coalesce(EXTRACT(YEAR FROM sa.vencto2) || '-' || EXTRACT(MONTH FROM sa.vencto2) || '-' || EXTRACT(DAY FROM sa.vencto2)
                                    , EXTRACT(YEAR FROM sa.vencto1) || '-' || EXTRACT(MONTH FROM sa.vencto1) || '-' || EXTRACT(DAY FROM sa.vencto1))),NULL,'\N') as vencimentomulta,
                                    coalesce(max(iif(sp.estorno = 'S', NULL, EXTRACT(YEAR FROM sp.data) || '-' || EXTRACT(MONTH FROM sp.data) || '-' || EXTRACT(DAY FROM sp.data))),NULL,'\N') as dtpagamento,
                                    coalesce(replace(cast(max((select sum(spp.valor) from sigpagtos spp where spp.codparcela = sa.codigo)) as varchar(15)),',', '.'),NULL,'\N') as valorpago,
                                    coalesce(max(sa.unidade),NULL,'\N') as codunidade,
                                    coalesce(max(sa.anoletivo),NULL,'\N') as anoletivo,
                                    coalesce(max(sa.unidade || sa.tipo),NULL,'\N') as codreceita,
                                    coalesce(max(iif(st.agencia is null, 'N', 'S')),NULL,'\N') as cobbancaria,
                                    coalesce(max(replace(sa.prontuario, '-', '')),NULL,'\N') as codaluno,
                                    coalesce(max(st.bolnossonumero),NULL,'\N') as nossonumero,
                                    max('N') as avulsa,
                                    coalesce(max('MIGRACAO'),NULL,'\N') as cadastradopor,
                                    max(EXTRACT(YEAR FROM current_timestamp) || '-' || EXTRACT(MONTH FROM current_timestamp) || '-' || EXTRACT(DAY FROM current_timestamp)) as dtcadastro,
                                    coalesce(replace(cast(max(sa.valor1) as varchar(15)),',', '.'),NULL,'\N') as valorparcelaoriginal,
                                    coalesce(replace(cast(max((sa.valortit - sa.valor1)) as varchar(15)),',', '.'),NULL,'\N') as descontooriginal,
                                    coalesce(max(EXTRACT(YEAR FROM sa.vencto1) || '-' || EXTRACT(MONTH FROM sa.vencto1) || '-' || EXTRACT(DAY FROM sa.vencto1)),NULL,'\N') as vencimentooriginal,
                                    coalesce(replace(cast(max(sa.valoreal) as varchar(15)),',', '.'),NULL,'\N') as valorbruto,
                                    coalesce(MAX(cr.nome || '  ' || extract(MONTH from sa.vencto1) || ' / ' || extract(year from sa.vencto1)),NULL,'\N') as descricao,
                                    coalesce(max(coalesce(iif(m.cpfresp = '', '00000000000', replace(replace(m.cpfresp, '-', ''), '.', '')), '00000000000')),NULL,'\N') as cpf,
                                    coalesce(max(sa.unidade || sa.tipo),NULL,'\N') as receitaoriginal,
                                    coalesce(replace(cast(max(sa.valortit) as varchar(15)),',', '.'),NULL,'\N') as valor,
                                    coalesce(max(sa.situacao),NULL,'\N') as statusparcela,
                                    coalesce(max(sa.titulo),NULL,'\N') as codfatura_aux,
                                    coalesce(max(st.agencia),NULL,'\N') as agencia_aux,
                                    coalesce(max(st.conta),NULL,'\N') as conta_aux
                                    from sigalucx sa
                                    left
                                    join sigtitulos st on st.codparcela = sa.codigo
                                    left
                                    join sigpagtos sp on sp.codparcela = sa.codigo
                                    left
                                    join sigaluno m on (m.codigo = sa.codaluno and m.unidade = sa.unidade )
                                    left join sigparce cr on(cr.codigo = sa.tipo and cr.unidade = sa.unidade)
                                    where sa.unidade = 01
                                    and sa.codigo is not null
                                    and coalesce(sa.situacao,'Q') not in ('G')-- Essa sigla 'G' é referente as parcelas deletadas.
                                    group by sa.codigo;";
                FbDataAdapter d = new FbDataAdapter(exportfb);
                d.Fill(dr);

                string local = $"C:\\Users\\{Environment.UserName}\\Desktop\\teste.txt";

                using (StreamWriter s = new StreamWriter(local))
                {


                    for (int i = 0; i < dr.Rows.Count; i++)
                    {

                        s.WriteLine($@"{dr.Rows[i]["CODREC"]},{dr.Rows[i]["CODMATRICULA"]},{dr.Rows[i]["PARCELA"]},{dr.Rows[i]["TOTPARCELA"]},{dr.Rows[i]["VALORPARCELA"]},{dr.Rows[i]["VENCIMENTO"]},{dr.Rows[i]["DESCONTO"]},{dr.Rows[i]["CARENCIADESCONTO"]},{dr.Rows[i]["VENCIMENTODESCONTO"]},{dr.Rows[i]["DESCONTO2"]},{dr.Rows[i]["CARENCIADESCONTO2"]},{dr.Rows[i]["VENCIMENTODESCONTO2"]},{dr.Rows[i]["MULTA"]},{dr.Rows[i]["JUROS"]},{dr.Rows[i]["CARENCIAMULTA"]},{dr.Rows[i]["VENCIMENTOMULTA"]},{dr.Rows[i]["DTPAGAMENTO"]},{dr.Rows[i]["VALORPAGO"]},{dr.Rows[i]["CODUNIDADE"]},{dr.Rows[i]["ANOLETIVO"]},{dr.Rows[i]["CODRECEITA"]},{dr.Rows[i]["COBBANCARIA"]},{dr.Rows[i]["CODALUNO"]},{dr.Rows[i]["NOSSONUMERO"]},{dr.Rows[i]["AVULSA"]},{dr.Rows[i]["CADASTRADOPOR"]},{dr.Rows[i]["DTCADASTRO"]},{dr.Rows[i]["VALORPARCELAORIGINAL"]},{dr.Rows[i]["DESCONTOORIGINAL"]},{dr.Rows[i]["VENCIMENTOORIGINAL"]},{dr.Rows[i]["VALORBRUTO"]},{dr.Rows[i]["DESCRICAO"]},{dr.Rows[i]["CPF"]},{dr.Rows[i]["RECEITAORIGINAL"]},{dr.Rows[i]["VALOR"]},{dr.Rows[i]["STATUSPARCELA"]},{dr.Rows[i]["CODFATURA_AUX"]},{dr.Rows[i]["AGENCIA_AUX"]},{dr.Rows[i]["CONTA_AUX"]};");
                        
                    }

                    mysql.CommandText = $@"LOAD DATA INFILE 'C:/Users/{Environment.UserName}/Desktop/teste.txt' INTO TABLE mig_contasreceber_siga
                                    FIELDS TERMINATED BY ','
                                    LINES TERMINATED BY ';\r\n'";
                    mysql.ExecuteNonQuery();

                    s.Close();

                    File.Delete(local);
                }

                mysql.CommandText = $@"INSERT INTO contasreceber (CODREC, CODMATRICULA, PARCELA, TOTPARCELA, VALORPARCELA, VENCIMENTO, DESCONTO, CARENCIADESCONTO, VENCIMENTODESCONTO, DESCONTO2, CARENCIADESCONTO2, VENCIMENTODESCONTO2, MULTA, JUROS, CARENCIAMULTA, VENCIMENTOMULTA, DTPAGAMENTO, VALORPAGO, CODUNIDADE, ANOLETIVO, CODRECEITA, COBBANCARIA, CODALUNO, NOSSONUMERO, AVULSA, CADASTRADOPOR, DTCADASTRO, VALORPARCELAORIGINAL, DESCONTOORIGINAL, VENCIMENTOORIGINAL, VALORBRUTO, DESCRICAO, CPF, RECEITAORIGINAL, VALOR, STATUSPARCELA, CODFATURA_AUX, AGENCIA_AUX, CONTA_AUX)
                                    (SELECT CODREC, CODMATRICULA, PARCELA, TOTPARCELA, VALORPARCELA, VENCIMENTO, DESCONTO, CARENCIADESCONTO, VENCIMENTODESCONTO, DESCONTO2, CARENCIADESCONTO2, VENCIMENTODESCONTO2, MULTA, JUROS, CARENCIAMULTA, VENCIMENTOMULTA, DTPAGAMENTO, VALORPAGO, CODUNIDADE, ANOLETIVO, CODRECEITA, COBBANCARIA, CODALUNO, NOSSONUMERO, AVULSA, CADASTRADOPOR, DTCADASTRO, VALORPARCELAORIGINAL, DESCONTOORIGINAL, VENCIMENTOORIGINAL, VALORBRUTO, DESCRICAO, CPF, RECEITAORIGINAL, VALOR, STATUSPARCELA, CODFATURA_AUX, AGENCIA_AUX, CONTA_AUX
                                    FROM mig_contasreceber_siga c
                                    WHERE c.codaluno IN (SELECT a.codaluno FROM aluno a WHERE a.codaluno = c.codaluno))";
                mysql.ExecuteNonQuery();

                MessageBox.Show("Importação concluída com sucesso!");

            }
            catch (Exception err)
            {

                MessageBox.Show(err.Message);
            }
            finally
            {
                log.CommandText = "select count(1) from contasreceber;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }


        }

    }
}