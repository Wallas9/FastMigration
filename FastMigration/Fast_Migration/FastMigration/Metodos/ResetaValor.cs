using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using MySql.Data.MySqlClient;
using FirebirdSql.Data.FirebirdClient;
using System.Windows.Forms;

namespace FastMigration.Metodos
{
    public class ResetaValor
    {
        public static void Resetar(out int a, out int b, string fbconn)
        {

            FbConnection conn2 = new FbConnection(fbconn);
            conn2.Open();

            FbCommand anoParcelaInicio = new FbCommand(@"select first 1 sa.anoletivo
                from sigalucx sa
                left join sigtitulos st on st.codparcela = sa.codigo
                left join sigpagtos sp on sp.codparcela = sa.codigo
                left join sigaluno m on (m.codigo = sa.codaluno and m.unidade = sa.unidade )
                left join sigparce cr on (cr.codigo = sa.tipo and cr.unidade = sa.unidade )
                where sa.codigo is not null
                and coalesce(sa.situacao,'Q') not in ('G') -- Essa sigla 'G' é referente as parcelas deletadas.
                and sa.anoletivo > 1000
                order by sa.anoletivo", conn2);

            FbCommand anoParcelaFim = new FbCommand(@"select first 1 sa.anoletivo
                from sigalucx sa
                left join sigtitulos st on st.codparcela = sa.codigo
                left join sigpagtos sp on sp.codparcela = sa.codigo
                left join sigaluno m on (m.codigo = sa.codaluno and m.unidade = sa.unidade )
                left join sigparce cr on (cr.codigo = sa.tipo and cr.unidade = sa.unidade )
                where sa.codigo is not null
                and coalesce(sa.situacao,'Q') not in ('G') -- Essa sigla 'G' é referente as parcelas deletadas.
                and sa.anoletivo > 1000
                order by sa.anoletivo DESC", conn2);

            Int32.TryParse(anoParcelaInicio.ExecuteScalar().ToString(), out a);
            Int32.TryParse(anoParcelaFim.ExecuteScalar().ToString(), out b);

        }
    }
}
