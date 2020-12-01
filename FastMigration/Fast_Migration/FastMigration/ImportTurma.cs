using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Turma")]
    public class ImportTurma : IImportArgs
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
                (COALESCE(sc.classe,'X') || COALESCE(sc.turno,'Y') || COALESCE(sc.periodo,'Z')) as dscturma
                from sigaluno sc
                group by (COALESCE(sc.classe,'X') || COALESCE(sc.turno,'Y') || COALESCE(sc.periodo,'Z'));", conn2);

                DataTable dtable = new DataTable();
                FbDataAdapter adapter = new FbDataAdapter(MySelect);
                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.Append(@"SET FOREIGN_KEY_CHECKS = 0; 
                DELETE FROM turma;        
                INSERT INTO turma (dscturma) VALUES ");

                for (int i = 0; i < dtable.Rows.Count; i++)
                {
                    queryBuilder.Append($@"('{dtable.Rows[i]["dscturma"]}'), ");
                }

                queryBuilder.Remove(queryBuilder.Length - 2, 2);

                MySqlCommand query = new MySqlCommand(queryBuilder.ToString(), conn);
                query.ExecuteNonQuery();

                DataTable dtable2 = new DataTable();

                StringBuilder queryBuilder2 = new StringBuilder();
                queryBuilder2.Append(@"create table turma_tella(
                 codturma int,
                 dscturma varchar(10),
                 turno char(10));");

                FbCommand createTable = new FbCommand(queryBuilder2.ToString(), conn2);
                createTable.ExecuteNonQuery();

                MySqlCommand createTurma = new MySqlCommand(@"select codturma, dscturma, turno from turma;", conn);

                MySqlDataAdapter adapter2 = new MySqlDataAdapter(createTurma);
                adapter2.Fill(dtable2);

                StringBuilder queryBuilder3 = new StringBuilder();

                for (int i = 0; i < dtable2.Rows.Count; i++)
                {
                    queryBuilder3.Append($@"insert into turma_tella (codturma, dscturma, turno) values ('{dtable2.Rows[i]["codturma"]}' ,'{dtable2.Rows[i]["dscturma"]}' , '{dtable2.Rows[i]["turno"]}');");

                    FbCommand insertTable = new FbCommand(queryBuilder3.ToString(), conn2);

                    insertTable.ExecuteNonQuery();

                    queryBuilder3.Clear();
                }

                MessageBox.Show("Importação concluída com sucesso!");

            }
            catch (Exception err)
            {

                MessageBox.Show(err.Message);

            }
            finally
            {
                log.CommandText = "select count(1) from turma;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());
                
                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }
            
        }
    }
}