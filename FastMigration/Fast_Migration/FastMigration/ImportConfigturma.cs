using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Configturma")]
    public class ImportConfigturma : IImportArgs
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

                FbCommand query = new FbCommand(@"select
                right(sc.anoletivo,2)||lpad((sc.unidade || sc.curso || sc.serie),6,0)||lpad(t.codturma,4,0) as codconfturma_aux,
                (sc.unidade || sc.curso || sc.serie) as codseriecurso,
                sc.descricao as dscturma,
                coalesce(EXTRACT(YEAR FROM(sc.datainicio )) || '-' || EXTRACT(MONTH FROM (sc.datainicio )) || '-' || EXTRACT(DAY FROM (sc.datainicio )),  sc.anoletivo || '-02-05') dtinicio,
                coalesce(EXTRACT(YEAR FROM(sc.datafim )) || '-' || EXTRACT(MONTH FROM (sc.datafim )) || '-' || EXTRACT(DAY FROM (sc.datafim )),   sc.anoletivo || '-12-20') as dtfim,
                sc.anoletivo as anoletivo,
                sc.turno as turno,
                coalesce(EXTRACT(YEAR FROM(current_timestamp)) || '-' || EXTRACT(MONTH FROM (current_timestamp)) || '-' || EXTRACT(DAY FROM (current_timestamp)),   sc.anoletivo || '-12-20') as dtcadastro,
                'MIGRACAO' as cadastradopor,
                t.codturma as codturma
                from sigclass sc
                join turma_tella t on t.dscturma = (COALESCE(sc.classe,'X') || COALESCE(sc.turno,'Y') || COALESCE(sc.periodo,'Z'));", conn2);

                FbDataAdapter adapter = new FbDataAdapter(query);
                adapter.Fill(dtable);

                StringBuilder query2 = new StringBuilder();

                query2.Append("ALTER TABLE configturma ADD COLUMN codconfturma_aux VARCHAR(30);" +
                    "SET FOREIGN_KEY_CHECKS = 0; " +
                    "DELETE FROM configturma;" +
                    "INSERT INTO configturma (codconfturma_aux,codseriecurso,dscturma,dtinicio,dtfim,anoletivo,turno,dtcadastro,cadastradopor,codturma) VALUES ");

                for (int i = 0; i < dtable.Rows.Count; i++)
                {
                    query2.Append($@"('{dtable.Rows[i]["codconfturma_aux"]}' , '{dtable.Rows[i]["codseriecurso"]}' , '{dtable.Rows[i]["dscturma"]}' , '{dtable.Rows[i]["dtinicio"]}' , '{dtable.Rows[i]["dtfim"]}' , '{dtable.Rows[i]["anoletivo"]}' , '{dtable.Rows[i]["turno"]}' , '{dtable.Rows[i]["dtcadastro"]}' , '{dtable.Rows[i]["cadastradopor"]}' , '{dtable.Rows[i]["codturma"]}'), ");
                }

                query2.Remove(query2.Length - 2, 2);

                MySqlCommand query3 = new MySqlCommand(query2.ToString(), conn);

                query3.ExecuteNonQuery();

                MessageBox.Show("Importação concluída com sucesso!");
            }
            catch (Exception err)
            {
                //para remover a coluna
                log.CommandText = "ALTER TABLE configturma DROP COLUMN codconfturma_aux;";
                log.ExecuteNonQuery();
                MessageBox.Show(err.Message);

            }
            finally
            {
                log.CommandText = "select count(1) from configturma;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

            
        }
    }
}
