using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("CentroReceita")]
    public class ImportCentroReceita : IImportArgs
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

                FbCommand MySelect = new FbCommand(@"select
                (cr.unidade || cr.codigo) as codreceita,
                cr.nome as dscreceita
                from sigparce cr;", conn2);

                //Crie um data adapter para trabalhar com datatables.
                //Datatables são mais fáceis de manipular que utilizar o método Read()
                DataTable dtable = new DataTable();

                FbDataAdapter adapter = new FbDataAdapter(MySelect);

                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.Append(@"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS mig_centroreceita_siga;
                DELETE FROM centroreceita;
                create table mig_centroreceita_siga(
                codreceita varchar(10),
                dscreceita varchar(100),
                codreceita_tella varchar(100),
                INDEX `idx_codreceita` (`codreceita`));" +

                "INSERT INTO mig_centroreceita_siga (codreceita,dscreceita) VALUES ");

                for (int i = 0; i < dtable.Rows.Count; i++)
                {
                    queryBuilder.Append($@"('{dtable.Rows[i]["codreceita"]}' , '{dtable.Rows[i]["dscreceita"]}'), ");
                }

                //Remove a última vírgula da consulta, para evitar erros de sintaxe.
                queryBuilder.Remove(queryBuilder.Length - 2, 2);

                //O segredo da perfomace está aqui: Você somente executa a operação após construir toda a consulta.
                //Antes você estava executando uma chamada no banco de dados a cada iteração do while. E isto é um pecado, em relação a perfomace =D
                //var s = queryBuilder.ToString();
                MySqlCommand query = new MySqlCommand(queryBuilder.ToString(), conn);
                query.ExecuteNonQuery();

                MySqlCommand insert = new MySqlCommand(@$"SET FOREIGN_KEY_CHECKS = 0; 
                INSERT INTO centroreceita (codreceita,dscreceita,ativo,multa,juros)
                SELECT codreceita,left(dscreceita,20) dscreceita,'S' ativo,'0.02' multa,'0.00033' juros FROM mig_centroreceita_siga  where codreceita > 0 GROUP BY dscreceita;               
                UPDATE mig_centroreceita_siga ca JOIN centroreceita c ON left(ca.dscreceita,20) = left(c.dscreceita,20)  SET ca.codreceita_tella = c.codreceita;", conn);
                insert.ExecuteNonQuery();

                MessageBox.Show("Importação concluída com sucesso!");
            }
            catch (Exception err)
            {
                MessageBox.Show(err.Message);
            }
            finally
            {

                log.CommandText = "select count(1) from centroreceita;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());
                
                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

        }
    }
}
