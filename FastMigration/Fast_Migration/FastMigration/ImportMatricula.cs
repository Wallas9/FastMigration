using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{

    [ImportFor("Matricula")]
    public class ImportMatricula : IImportArgs
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

            MySqlCommand insert = new MySqlCommand("", conn);
            insert.CommandTimeout = 86400;

            try
            {
                MySqlCommand create = new MySqlCommand(@"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS mig_matricula_siga;
                DELETE FROM matricula;
                DELETE FROM login;
                CREATE TABLE `mig_matricula_siga` (
	            `codmatricula` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	            `codaluno` INT(10) UNSIGNED NOT NULL,
	            `codseriecurso` INT(11) NULL DEFAULT NULL,
	            `codturma` INT(10) UNSIGNED NOT NULL,
	            `dtmatricula` varchar(100) NULL DEFAULT NULL,
	            `anoletivo` INT(10) UNSIGNED NULL DEFAULT NULL,
	            `dtinicio` varchar(100) NULL DEFAULT NULL,
	            `dtfim` varchar(100) NULL DEFAULT NULL,
	            `cpfrespacademico` VARCHAR(14) NULL DEFAULT NULL,
	            `cpfrespfinanceiro` VARCHAR(14) NULL DEFAULT NULL,
	            `parentescorespacademico` varchar(100) NULL DEFAULT NULL,
	            `parentescorespfinanceiro` varchar(100) NULL DEFAULT NULL,
	            `rematricula` CHAR(1) NULL DEFAULT NULL,
	            `situacao` CHAR(1) NULL DEFAULT 'C',
	            `dtcadastro` varchar(100) NULL DEFAULT NULL,
	            `cadastradopor` INT(10) UNSIGNED NULL DEFAULT NULL,
	            `qtdparcela` INT(10) UNSIGNED NULL DEFAULT NULL,
	            `primeirovenc` DATE NULL DEFAULT NULL,
	            `valoranuidade` DECIMAL(15,2) NULL DEFAULT NULL,
	            `codescolaorigem` INT(11) NULL DEFAULT NULL,
	            `dtalteracao` varchar(100) NULL DEFAULT NULL,
	            `alteradopor` INT(11) NULL DEFAULT NULL,
	            `desconto` DECIMAL(5,2) NULL DEFAULT NULL,
	            `bolsista` CHAR(1) NULL DEFAULT 'N',
	            `codbolsa` INT(11) NULL DEFAULT NULL,
	            `alunorespacademico` CHAR(1) NULL DEFAULT 'N',
	            `alunorespfinanceiro` CHAR(1) NULL DEFAULT 'N',
	            `valordiscipadicional` DECIMAL(15,2) NULL DEFAULT NULL,
	            `dtTransferencia` varchar(100) NULL DEFAULT NULL,
	            `motivotransf` VARCHAR(1000) NULL DEFAULT NULL,
	            `codcurso` INT(11) NULL DEFAULT NULL,
	            `codunidade` INT(11) NULL DEFAULT NULL,
	            `situacaoaluno` varchar(100) NULL DEFAULT NULL,
	            `rematriculamassa` CHAR(1) NULL DEFAULT NULL,
	            `codconfturma` INT(11) NULL DEFAULT NULL,
	            `cpfrespfinanceiro2` VARCHAR(14) NULL DEFAULT NULL,
	            `parentescorespfinanceiro2` varchar(100) NULL DEFAULT NULL,
	            `financeiroporcurso` CHAR(1) NULL DEFAULT 'N',
	            `integral` CHAR(1) NULL DEFAULT NULL,
	            `tecnicococomitante` CHAR(1) NULL DEFAULT 'N',
	            `matonline` CHAR(1) NULL DEFAULT 'N',
	            `codconffatura` INT(11) NULL DEFAULT '1',
	            `intercambista` CHAR(1) NULL DEFAULT NULL COMMENT 'S - Sim / N- Nao',
	            `codconfturma_aux` varchar(100) NULL DEFAULT NULL,
	            codaluno_tella int(11),
	            PRIMARY KEY (`codmatricula`) USING BTREE,
                INDEX `idx_codconfturma_aux` (`CODCONFTURMA_AUX`),
                INDEX `idx_codaluno` (`CODALUNO`),
                INDEX `idx_codmatricula` (`CODMATRICULA`))
                COLLATE='latin1_swedish_ci'
                ENGINE=InnoDB
                AUTO_INCREMENT=1;", conn);
                create.ExecuteNonQuery();

                //cria a funcao gerasenha
                MySqlCommand function = new MySqlCommand($@"
                DROP FUNCTION IF EXISTS gerasenha;
                CREATE DEFINER=`root`@`%` FUNCTION `gerasenha`(`tamanho` int)
                RETURNS varchar(10) CHARSET latin1
                LANGUAGE SQL
                NOT DETERMINISTIC
                CONTAINS SQL
                SQL SECURITY DEFINER
                COMMENT ''
                BEGIN
                return (SELECT lower(CONVERT(SUBSTRING(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                MD5(RAND())
                ,'1','')
                ,'0','')
                ,'a','')
                ,'e','')
                ,'i','')
                ,'o','')
                ,'u','')
                FROM 1 FOR tamanho) USING latin1)));
                END", conn);
                function.ExecuteNonQuery();

                FbCommand MySelect = new FbCommand(@"select
                distinct(a.codigo) as codmatricula,
                replace(a.prontuario,'-','') as codaluno,
                right(sc.anoletivo,2)||lpad((sc.unidade || sc.curso || sc.serie),6,0)||lpad(t.codturma,4,0) as codconfturma_aux,
                (a.unidade || a.curso || a.serie) as codseriecurso,
                t.codturma as codturma,
                EXTRACT(YEAR FROM a.datamatric) || '-' || EXTRACT(MONTH FROM a.datamatric) || '-' || EXTRACT(DAY FROM a.datamatric) as dtmatricula,
                a.anoletivo as anoletivo,
                iif(replace(replace(iif(a.cpfresp='',null, a.cpfresp),'-',''),'.','') = ' ','00000000000',replace(replace(iif(a.cpfresp='',null, a.cpfresp),'-',''),'.','')) as cpfrespfinanceiro,
                iif(replace(replace(iif(a.cpf_rp ='',null, cpf_rp),'-',''),'.','') = ' ','00000000000', replace(replace(iif(a.cpf_rp ='',null, cpf_rp),'-',''),'.',''))  as cpfrespacademico,
                CASE a.situacao when 'C' then 'I' when 'A' then 'C' when'T' then 'T' when 'F' then 'F'  END as situacao,
                EXTRACT(YEAR FROM current_timestamp) || '-' || EXTRACT(MONTH FROM current_timestamp) || '-' || EXTRACT(DAY FROM current_timestamp) as dtcadastro,
                '1' as cadastradopor,
                (a.unidade || a.curso) as codcurso,
                a.unidade as codunidade,
                iif(a.situacao = 'C' or (a.situacao = 'T'),EXTRACT(YEAR FROM a.datasit) || '-' || EXTRACT(MONTH FROM a.datasit) || '-' || EXTRACT(DAY FROM a.datasit),null) as dttransferencia,
                -- a.anoletivo,
                -- t.codturma,
                -- a.turno,
                'S' as rematricula
                from sigaluno a
                left join sigclass sc on (sc.anoletivo = a.anoletivo
                and sc.unidade = a.unidade
                and sc.curso = a.curso
                and sc.serie = a.serie
                and sc.classe = iif(a.classe = '', 'A', a.classe)
                and sc.turno = a.turno)
                left join turma_tella t on t.dscturma = (COALESCE(sc.classe,'X') || COALESCE(sc.turno,'Y') || COALESCE(sc.periodo,'Z'))
                where t.codturma is not null
                and a.prontuario is not null", conn2);

                DataTable dtable = new DataTable();
                FbDataAdapter adapter = new FbDataAdapter(MySelect);

                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();

                try
                {
                    queryBuilder.Append("INSERT INTO mig_matricula_siga (codmatricula,codaluno,codconfturma_aux,codseriecurso,codturma,dtmatricula,anoletivo,cpfrespfinanceiro,cpfrespacademico,situacao,dtcadastro,cadastradopor,codcurso,codunidade,dttransferencia,rematricula) VALUES ");

                    for (int i = 0; i < dtable.Rows.Count; i++)
                    {
                        queryBuilder.Append($@"('{dtable.Rows[i]["codmatricula"]}' , '{dtable.Rows[i]["codaluno"]}' , '{dtable.Rows[i]["codconfturma_aux"]}' , '{dtable.Rows[i]["codseriecurso"]}' , '{dtable.Rows[i]["codturma"]}' , '{dtable.Rows[i]["dtmatricula"]}' , '{dtable.Rows[i]["anoletivo"]}' , '{dtable.Rows[i]["cpfrespfinanceiro"]}' , '{dtable.Rows[i]["cpfrespacademico"]}' , '{dtable.Rows[i]["situacao"]}' , '{dtable.Rows[i]["dtcadastro"]}' ,'{dtable.Rows[i]["cadastradopor"]}' , '{dtable.Rows[i]["codcurso"]}' , '{dtable.Rows[i]["codunidade"]}' , '{dtable.Rows[i]["dttransferencia"]}' , '{dtable.Rows[i]["rematricula"]}'), ");
                    }

                    queryBuilder.Remove(queryBuilder.Length - 2, 2);

                    insert = new MySqlCommand(queryBuilder.ToString(), conn);
                    insert.ExecuteNonQuery();

                }

                catch (Exception err)
                {
                    if (err.Message == "Packets larger than max_allowed_packet are not allowed.")
                    {
                        for (int i = 0; i < dtable.Rows.Count; i++)
                        {
                            insert.CommandText = $@"INSERT INTO mig_matricula_siga (codmatricula,codaluno,codconfturma_aux,codseriecurso,codturma,dtmatricula,anoletivo,cpfrespfinanceiro,cpfrespacademico,situacao,dtcadastro,cadastradopor,codcurso,codunidade,dttransferencia,rematricula) VALUES  
                                                     ('{dtable.Rows[i]["codmatricula"]}' , '{dtable.Rows[i]["codaluno"]}' , '{dtable.Rows[i]["codconfturma_aux"]}' , '{dtable.Rows[i]["codseriecurso"]}' , '{dtable.Rows[i]["codturma"]}' , '{dtable.Rows[i]["dtmatricula"]}' , '{dtable.Rows[i]["anoletivo"]}' , '{dtable.Rows[i]["cpfrespfinanceiro"]}' , '{dtable.Rows[i]["cpfrespacademico"]}' , '{dtable.Rows[i]["situacao"]}' , '{dtable.Rows[i]["dtcadastro"]}' ,'{dtable.Rows[i]["cadastradopor"]}' , '{dtable.Rows[i]["codcurso"]}' , '{dtable.Rows[i]["codunidade"]}' , '{dtable.Rows[i]["dttransferencia"]}' , '{dtable.Rows[i]["rematricula"]}');";
                            insert.ExecuteNonQuery();
                        }
                    }
                    else
                    {
                        MessageBox.Show(err.Message);
                        conn2.Close();
                        conn.Close();
                    }
                }
               
                MySqlCommand updates = new MySqlCommand(@"
                -- SETANDO O CODCONFTURMA 
                UPDATE mig_matricula_siga m
                join configturma c on c.codconfturma_aux = m.codconfturma_aux
                SET m.codconfturma = c.codconfturma;

                -- SETANDO O CODALUNO_TELLA
                UPDATE mig_matricula_siga m
                JOIN mig_aluno_siga a ON a.codaluno = m.codaluno
                SET m.codaluno_tella = a.codaluno_tella;

                -- ACERTANDO OS RESPS SEM CPF
                UPDATE mig_matricula_siga m
                SET  m.cpfrespfinanceiro = '00000000000'
                WHERE m.cpfrespfinanceiro IS NULL;

                UPDATE mig_matricula_siga m
                SET  m.cpfrespacademico = '00000000000'
                WHERE m.cpfrespacademico IS NULL;

                -- ACERTANDO OS CPFs INVALIDOS
                UPDATE mig_matricula_siga m 
                set m.cpfrespfinanceiro = '00000000000'
                WHERE m.cpfrespfinanceiro NOT IN (SELECT a.cpf FROM responsavel a WHERE a.cpf = m.cpfrespfinanceiro) ;

                UPDATE mig_matricula_siga m 
                SET m.cpfrespacademico = '00000000000'
                WHERE m.cpfrespacademico NOT IN (SELECT a.cpf FROM responsavel a WHERE a.cpf = m.cpfrespacademico);", conn);
                updates.ExecuteNonQuery();

                MySqlCommand update = new MySqlCommand(@$"
                UPDATE mig_matricula_siga set dtmatricula = NULL where dtmatricula = '';
                UPDATE mig_matricula_siga set dtinicio = NULL where dtinicio = '';
                update mig_matricula_siga set dtfim = null where dtfim = ''; 
                update mig_matricula_siga set dtcadastro = null where dtcadastro = ''; 
                update mig_matricula_siga set dtalteracao = null where dtalteracao = '';
                update mig_matricula_siga set dtTransferencia = null where dtTransferencia = '';", conn);
                update.ExecuteNonQuery();

                MySqlCommand insertMatricula = new MySqlCommand(@"SET FOREIGN_KEY_CHECKS = 0;                 
                INSERT INTO matricula (codmatricula, codaluno, codseriecurso, codturma, dtmatricula, anoletivo, dtinicio, dtfim, cpfrespacademico, cpfrespfinanceiro, parentescorespacademico, parentescorespfinanceiro, rematricula, situacao, dtcadastro, cadastradopor, qtdparcela, primeirovenc, valoranuidade, codescolaorigem, dtalteracao, alteradopor, desconto, bolsista, codbolsa, alunorespacademico, alunorespfinanceiro, valordiscipadicional, dtTransferencia, motivotransf, codcurso, codunidade, situacaoaluno, rematriculamassa, codconfturma, cpfrespfinanceiro2, parentescorespfinanceiro2, financeiroporcurso, integral, tecnicococomitante, matonline, codconffatura, intercambista )
                (SELECT codmatricula, codaluno_tella, codseriecurso, codturma, dtmatricula, anoletivo, dtinicio, dtfim, cpfrespacademico, cpfrespfinanceiro, parentescorespacademico, parentescorespfinanceiro, rematricula, situacao, dtcadastro, cadastradopor, qtdparcela, primeirovenc, valoranuidade, codescolaorigem, dtalteracao, alteradopor, desconto, bolsista, codbolsa, alunorespacademico, alunorespfinanceiro, valordiscipadicional, dtTransferencia, motivotransf, codcurso, codunidade, situacaoaluno, rematriculamassa, codconfturma, cpfrespfinanceiro2, parentescorespfinanceiro2, financeiroporcurso, integral, tecnicococomitante, matonline, codconffatura, intercambista 
                FROM mig_matricula_siga m
                WHERE m.codconfturma > 0 and m.codaluno_tella > 0);", conn);
                insertMatricula.ExecuteNonQuery();

                MySqlCommand query2 = new MySqlCommand(@"
                
                -- LOGINS
                -- Rodar os scripts abaixo para gerar as senhas dos funcionarios, alunos e responsáveis.
                UPDATE aluno a SET senha = gerasenha(6);
                UPDATE responsavel a SET senha = gerasenha(6);
                UPDATE funcionario a SET senha = gerasenha(6); 

                -- insere um registro para nao bugar os inserts debaixo                
                INSERT INTO login (id) VALUES (1);

                -- INSERE OS ALUNOS PENDENTES NA TABELA LOGIN. 
                INSERT INTO login (codaluno, login2, perfil, senha)
                SELECT a.codaluno, a.codaluno, 'A', IFNULL(a.senha, gerasenha(6)) AS senha
                FROM aluno a
                WHERE a.codaluno NOT IN (
                SELECT L.codaluno AS contador
                FROM login L
                WHERE L.codaluno = a.codaluno); 

                -- insere responsaveis 
                INSERT INTO login (login2, perfil, senha, codsoc)
                SELECT r.cpf, 'R', IFNULL(r.senha, gerasenha(6)) AS senha, r.codsoc
                FROM responsavel r
                WHERE r.codsoc NOT IN (
                SELECT L.codsoc AS contador
                FROM login L
                WHERE L.codsoc = r.codsoc); 

                -- insere funcionarios 
                INSERT INTO login (login2, perfil, senha, funcodigo)
                SELECT r.LOGIN, 'P', IFNULL(r.senha, gerasenha(6)) AS senha, r.funcodigo
                FROM funcionario r
                WHERE r.funcodigo NOT IN (
                SELECT L.funcodigo AS contador
                FROM login L
                WHERE L.funcodigo = r.funcodigo);

                UPDATE login, empresa SET login = CAST(CONCAT(login.perfil, SUBSTRING(empresa.codempresatella,3,4),empresa.codunidadetella, IFNULL(IFNULL(login.id,999),999)) AS CHAR);

                UPDATE login l JOIN aluno a ON a.codaluno = l.codaluno SET l.senha = a.senha;
                UPDATE login l JOIN funcionario f ON f.FUNCODIGO = l.funcodigo SET l.senha = f.SENHA;
                UPDATE login l JOIN responsavel r ON r.codsoc = l.codsoc SET l.senha = r.senha;
                
                UPDATE matricula m SET m.dttransferencia = NULL WHERE m.dttransferencia = 0;", conn);
                query2.CommandTimeout = 5000;
                query2.ExecuteNonQuery();

                MessageBox.Show("Talvez existam matrículas na tabela mig_matricula_siga que estejam com o codconfturma = 0," +
                    "e que por isso, não estão na tabela matrícula, verifique!");

                MessageBox.Show("Importação concluída com sucesso!");

            }
            catch (Exception err)
            {

                MessageBox.Show(err.Message);

            }
            finally
            {
                log.CommandText = "select count(1) from matricula;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }
           
        }
    }
}
