using FastMigration.Etapas;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;


namespace FastMigration
{
    public partial class Form2 : Form
    {
        Types t = new Types();
        Daily d = new Daily();
        Form1 f = new Form1();

        public Form2()
        {
            InitializeComponent();
        }

        private void Ok_btn_Click(object sender, EventArgs e)
        {

            t.Conn(f.textBox3.Text, f.textBox4.Text, f.textBox1.Text, f.textBox2.Text);
            d.Conn(f.textBox3.Text, f.textBox4.Text, f.textBox1.Text, f.textBox2.Text);
            t.Values(anoInicio_txt.Text, anoFim_txt.Text);
            d.Etapas(etapa1_num.Value, etapa2_num.Value);
            
            if (bimestre_rb.Checked)
            {
                t.Bimestre();
            }
            else if (trimestre_rb.Checked)
            {
                t.Trimestre();
            }

            d.DailyMigration();

            MessageBox.Show("Tudo pronto, pode conferir!");
        }
    
    }
}
