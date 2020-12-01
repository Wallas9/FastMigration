using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Conta")]
    public class ImportConta : IImportArgs
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
                FbCommand MySelect = new FbCommand(@"select c.banco as codbanco,
                c.nome as dscconta,
                c.conta as numconta,
                c.carteira as carteira,
                c.sequencia as sequencialremessa,
                c.agencia as agencia
                from sigbccta c;", conn2);


                //Crie um data adapter para trabalhar com datatables.
                //Datatables são mais fáceis de manipular que utilizar o método Read()
                FbDataAdapter adapter = new FbDataAdapter(MySelect);

                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.Append(@"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS mig_conta_siga;
                DELETE FROM conta;
                CREATE TABLE mig_conta_siga (
                CODBANCO VARCHAR(4),
                DSCCONTA VARCHAR(42),
                NUMCONTA VARCHAR(10),
                CARTEIRA VARCHAR(3),
                SEQUENCIALREMESSA varchar(100),
                AGENCIA VARCHAR(5));" +

                    "INSERT INTO mig_conta_siga (codbanco,dscconta,numconta,carteira, sequencialremessa,agencia) VALUES ");

                for (int i = 0; i < dtable.Rows.Count; i++)
                {
                    queryBuilder.Append($@"('{dtable.Rows[i]["codbanco"]}' , '{dtable.Rows[i]["dscconta"]}' , '{dtable.Rows[i]["numconta"]}' , '{dtable.Rows[i]["carteira"]}' , '{dtable.Rows[i]["sequencialremessa"]}' , '{dtable.Rows[i]["agencia"]}'), ");
                }

                //Remove a última vírgula da consulta, para evitar erros de sintaxe.
                queryBuilder.Remove(queryBuilder.Length - 2, 2);

                //O segredo da perfomace está aqui: Você somente executa a operação após construir toda a consulta.
                //Antes você estava executando uma chamada no banco de dados a cada iteração do while. E isto é um pecado, em relação a perfomace =D
                //var s = queryBuilder.ToString();
                MySqlCommand query = new MySqlCommand(queryBuilder.ToString(), conn);

                query.ExecuteNonQuery();

                MySqlCommand update = new MySqlCommand(@$"
                UPDATE mig_conta_siga SET sequencialremessa = NULL where sequencialremessa = '';
                UPDATE mig_conta_siga SET carteira = NULL where carteira = '';" , conn);
                update.ExecuteNonQuery();

                MySqlCommand insert = new MySqlCommand(@$"SET FOREIGN_KEY_CHECKS = 0; INSERT INTO conta (codbanco,dscconta,numconta,carteira,sequencialremessa,agencia)
                (SELECT right(if(codbanco = '0356','033',codbanco),3) as codbanco,dscconta,numconta,carteira,sequencialremessa,agencia FROM mig_conta_siga);
                INSERT INTO conta (dscconta) VALUES ('CONTA MIGRACAO');", conn); //usado no ifnull do contasreceber > insert pagamentoforma
                insert.ExecuteNonQuery();

                //para configurar o codempresa, é preciso ver com o cliente a qual EMPRESA a CONTA pertence
                MessageBox.Show("Importação concluída com sucesso, agora, configure o codempresa da tabela conta");

            }
            catch (Exception err)
            {
                MessageBox.Show(err.Message);
            }
            finally
            {
                log.CommandText = "select count(1) from conta;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

            
        }
    }
}
