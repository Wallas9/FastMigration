using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Curso")]
    public class ImportCurso : IImportArgs
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

                FbCommand MySelect = new FbCommand(@"SELECT (C.unidade || C.codigo) as codcurso,
                max(C.descricao) as dsccurso,
                max(C.resumo) as dscabreviada,
                max(0) as cargahoraria,
                max('S') as ativo,
                max(IIF(c.tipo = 'L', 3, IIF(c.descricao like '%FUNDAM%', 1,2))) as codnivel,
                1 as formulamediaetapa
                FROM  sigcurso C
                group BY C.unidade, C.codigo;", conn2);

                FbDataAdapter adapter = new FbDataAdapter(MySelect);
                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();

                queryBuilder.Append("SET FOREIGN_KEY_CHECKS = 0; " +
                    "DELETE FROM curso;" +
                    "INSERT INTO curso (codcurso,dsccurso,dscabreviada,cargahoraria,ativo,codnivel,formulamediaetapa) VALUES ");
                for (int i = 0; i < dtable.Rows.Count; i++)
                {
                    queryBuilder.Append($@"('{dtable.Rows[i]["codcurso"]}' , '{dtable.Rows[i]["dsccurso"]}' , '{dtable.Rows[i]["dscabreviada"]}' , '{dtable.Rows[i]["cargahoraria"]}' , '{dtable.Rows[i]["ativo"]}' , '{dtable.Rows[i]["codnivel"]}' , '{dtable.Rows[i]["formulamediaetapa"]}'), ");
                }

                queryBuilder.Remove(queryBuilder.Length - 2, 2);

                MySqlCommand query = new MySqlCommand(queryBuilder.ToString(), conn);
                query.ExecuteNonQuery();

                MessageBox.Show("Importação concluída com sucesso!");
            }
            catch (Exception err)
            {

                MessageBox.Show(err.Message);
            }
            finally
            {
                log.CommandText = "select count(1) from curso;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

            
        }
    }
}
