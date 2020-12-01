using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Seriecurso")]
    public class ImportSeriecurso : IImportArgs
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

                FbCommand MySelect = new FbCommand(@"select  (s.unidade || s.curso || s.serie) as codseriecurso,
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
                group by s.unidade, s.curso, s.serie;", conn2);

                FbDataAdapter adapter = new FbDataAdapter(MySelect);
                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.Append("SET FOREIGN_KEY_CHECKS = 0; " +
                    "DELETE FROM seriecurso;" +
                    "DELETE FROM cursosunidade;" +
                    "INSERT INTO seriecurso (codseriecurso, codunidade, codcurso, codserie, dscserie, ativo, ordem, concluinte, cadastradopor) VALUES ");

                for (int i = 0; i < dtable.Rows.Count; i++)
                {
                    queryBuilder.Append($@"('{dtable.Rows[i]["codseriecurso"]}' , '{dtable.Rows[i]["codunidade"]}' , '{dtable.Rows[i]["codcurso"]}' , '{dtable.Rows[i]["codserie"]}' ,'{dtable.Rows[i]["dscserie"]}' , '{dtable.Rows[i]["ativo"]}' , '{dtable.Rows[i]["ordem"]}' , '{dtable.Rows[i]["concluinte"]}' , '{dtable.Rows[i]["cadastradopor"]}'), ");
                }

                queryBuilder.Remove(queryBuilder.Length - 2, 2);

                MySqlCommand query = new MySqlCommand(queryBuilder.ToString(), conn);
                query.ExecuteNonQuery();

                // CURSOS UNIDADE //
                DataTable dtable2 = new DataTable();

                MySqlCommand MySelect3 = new MySqlCommand(@"SELECT CODUNIDADE, CODCURSO, 'N' FROM seriecurso GROUP BY CODUNIDADE, CODCURSO;", conn);
                MySqlDataAdapter adapter2 = new MySqlDataAdapter(MySelect3);

                adapter2.Fill(dtable2);

                StringBuilder queryBuilder2 = new StringBuilder();
                queryBuilder2.Append("SET FOREIGN_KEY_CHECKS = 0; " +
                    "INSERT INTO cursosunidade (codunidade, codcurso, portalsimplificado) VALUES ");

                for (int i = 0; i < dtable2.Rows.Count; i++)
                {
                    queryBuilder2.Append($@"('{dtable2.Rows[i]["codunidade"]}' , '{dtable2.Rows[i]["codcurso"]}' , '{dtable2.Rows[i]["N"]}'), ");
                }

                queryBuilder2.Remove(queryBuilder2.Length - 2, 2);

                MySqlCommand query2 = new MySqlCommand(queryBuilder2.ToString(), conn);
                query2.ExecuteNonQuery();


                MessageBox.Show("Importação concluída com sucesso!");
            }
            catch (Exception err)
            {

                MessageBox.Show(err.Message);
            }
            finally
            {
                log.CommandText = "select count(1) from seriecurso;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }
            
        }
    }
}
