using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Disciplina")]
    public class ImportDisciplina : IImportArgs
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
                DataTable dtable = new DataTable();

                FbCommand MySelect = new FbCommand(@"select
                d.codigo as coddisciplina,
                d.resumo as dscabreviada,
                d.descricao as dscdisciplina,
                'S' as ativo,
                1 as pesoreprovacao
                from sigdisci d;", conn2);

                //Crie um data adapter para trabalhar com datatables.
                //Datatables são mais fáceis de manipular que utilizar o método Read()
                FbDataAdapter adapter = new FbDataAdapter(MySelect);

                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.Append(@"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS mig_disciplina_siga;
                DELETE FROM disciplina;
                CREATE TABLE mig_disciplina_siga (`coddisciplina` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	            `codarea` INT(10) UNSIGNED NULL DEFAULT NULL,
	            `dscdisciplina` VARCHAR(120) NOT NULL COLLATE 'latin1_swedish_ci',
	            `dscabreviada` VARCHAR(5) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `ativo` CHAR(1) NULL DEFAULT NULL COLLATE 'latin1_swedish_ci',
	            `optativa` CHAR(1) NULL DEFAULT 'N' COLLATE 'latin1_swedish_ci',
	            `nucleo` varchar(100) NULL DEFAULT NULL COMMENT 'determina a base da disciplina; Comun ou diversificada.' COLLATE 'latin1_swedish_ci',
	            `coddisciplinacenso` INT(2) NULL DEFAULT NULL COMMENT 'disciplina a qual é referente no censo.',
	            `pesoreprovacao` DECIMAL(5,2) NULL DEFAULT '1.00' COMMENT 'determina o peso na disciplina pra calcular a quantidade de disciplinas reprovadas.',
                coddisciplina_tella int(11),
	            PRIMARY KEY (`coddisciplina`) USING BTREE,
	            INDEX `DISCIPLINA_FKIndex1` (`codarea`) USING BTREE);" +

                "INSERT INTO mig_disciplina_siga (coddisciplina,dscabreviada,dscdisciplina,ativo,pesoreprovacao) VALUES ");

                for (int i = 0; i < dtable.Rows.Count; i++)
                {
                    queryBuilder.Append($@"('{dtable.Rows[i]["coddisciplina"]}' , '{dtable.Rows[i]["dscabreviada"]}' , '{dtable.Rows[i]["dscdisciplina"]}' , '{dtable.Rows[i]["ativo"]}' , '{dtable.Rows[i]["pesoreprovacao"]}'), ");
                }

                //Remove a última vírgula da consulta, para evitar erros de sintaxe.
                queryBuilder.Remove(queryBuilder.Length - 2, 2);

                //O segredo da perfomace está aqui: Você somente executa a operação após construir toda a consulta.
                //Antes você estava executando uma chamada no banco de dados a cada iteração do while. E isto é um pecado, em relação a perfomace =D
                //var s = queryBuilder.ToString();
                MySqlCommand query = new MySqlCommand(queryBuilder.ToString(), conn);

                query.ExecuteNonQuery();


                MySqlCommand insert = new MySqlCommand(@"SET FOREIGN_KEY_CHECKS = 0; 
                INSERT INTO disciplina (coddisciplina,dscabreviada,dscdisciplina,ativo,pesoreprovacao)
                SELECT max(coddisciplina) as coddisciplina,dscabreviada,dscdisciplina,ativo,pesoreprovacao 
                FROM mig_disciplina_siga GROUP BY dscdisciplina;
                UPDATE mig_disciplina_siga ds JOIN disciplina d ON d.dscdisciplina = ds.dscdisciplina SET ds.coddisciplina_tella = d.coddisciplina;", conn);

                insert.ExecuteNonQuery();

                MessageBox.Show("Importação concluída com sucesso!");
            }
            catch (Exception err)
            {

                MessageBox.Show(err.Message);
            }
            finally
            {
                log.CommandText = "select count(1) from disciplina;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

            
        }
    }
}
