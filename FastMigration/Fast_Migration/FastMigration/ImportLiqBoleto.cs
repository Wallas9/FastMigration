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

namespace FastMigration.Metodos
{
    [ImportFor("LiquidacaoBoleto")]
    public class ImportLiqBoleto : IImportArgs
    {
        public void ExecutarProcedimento(params object[] args)
        {
            int anoMenor = 0;
            int anoMaior = 0;

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
                ResetaValor.Resetar(out anoMenor, out anoMaior, FbConn);

                while (anoMenor <= anoMaior)
                {

                    MySqlCommand liquidacaoBoleto = new MySqlCommand($@"SET FOREIGN_KEY_CHECKS = 0; 
                DELETE FROM liquidacaoboleto;
                DELETE FROM pagamento;
                DELETE FROM pagamentoforma;

                -- LIQUIDACAOBOLETO 
                INSERT INTO liquidacaoboleto(codreceita  ,valorpago  ,dtpagamento  ,codaluno  ,anoletivo  ,parcela  ,conta  ,vencimento  ,nossonumero,
                  valorapagar  ,desconto    ,valorparcela, dtprocessamento   ,codrec  , usuario)
                  (
  
                  select c.codreceita, ifnull(c.valorpago,0), c.dtpagamento, c.codaluno, c.anoletivo, c.parcela,
                  c.codconta, c.vencimento, c.nossonumero, 
                  c.valorparcela, (c.desconto) as desconto, c.valorparcela, c.dtpagamento, c.codrec, 'MIGRACAO'
                  from contasreceber c
                  where c.dtpagamento is not null
                  and c.anoletivo = {anoMenor});

                -- pagamento
                INSERT INTO pagamento(codpagamento ,dtpagamento ,dtregistro ,registradopor ,codunidaderec ,totalpago ,obs ,codaluno ,cpfresp)
                select l.codliquidacao as codpagamento, l.dtpagamento, l.dtprocessamento , l.usuario, c.codunidade, l.valorpago, l.obspagamento, l.codaluno, c.cpf
                from liquidacaoboleto l
                join contasreceber c ON c.codrec = l.codrec
                where l.codpagamento is null
                and l.anoletivo = {anoMenor};

                -- pagamentoforma
                INSERT INTO pagamentoforma(codpagamento ,codconta ,valorpago ,codmeiopagamentoitem ,codforma)
                select l.codliquidacao as codpagamento, ifnull(c.codconta,(SELECT max(c.codconta) FROM conta c WHERE c.dscconta = 'CONTA MIGRACAO')) AS 'codconta', l.valorpago, null,
                (CASE l.formapagamento
                WHEN 'DI' THEN 1
                WHEN 'LI' THEN 2
                WHEN 'CD' THEN 4
                WHEN 'CC' THEN 5
                ELSE 1 END) AS codforma
                from liquidacaoboleto l
                join contasreceber c ON c.codrec = l.codrec
                where l.codpagamento is NULL
                and l.anoletivo = {anoMenor};", conn);

                    liquidacaoBoleto.ExecuteNonQuery();
                    anoMenor++;
                }

                ResetaValor.Resetar(out anoMenor, out anoMaior, FbConn);

                while (anoMenor <= anoMaior)
                {
                    MySqlCommand update = new MySqlCommand($@"
                -- seta codpagamento na liquidacaoboleto
                update liquidacaoboleto l
                set l.codpagamento = l.codliquidacao
                where l.codpagamento is null
                and l.anoletivo = {anoMenor};", conn);

                    update.ExecuteNonQuery();
                    anoMenor++;
                }

                MessageBox.Show("Importação concluída com sucesso!");
            }
            catch (Exception err)
            {
                MessageBox.Show(err.Message);
            }
            finally
            {
                log.CommandText = "select count(1) from liquidacaoboleto;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

        }

    }
}