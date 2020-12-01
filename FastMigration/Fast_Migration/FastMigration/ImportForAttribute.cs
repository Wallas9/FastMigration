using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FastMigration
{
    [System.AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    sealed class ImportForAttribute : Attribute
    {
        public string NomeTabela { get; set; }
        // This is a positional argument
        public ImportForAttribute(string nomeTabela)
        {
            this.NomeTabela = nomeTabela;

        }
    }
}
