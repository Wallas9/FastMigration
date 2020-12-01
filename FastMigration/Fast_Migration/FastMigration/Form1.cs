using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Windows.Forms;
using FastMigration.Etapas;


namespace FastMigration
{
    public partial class Form1 : Form
    {

        Types t = new Types();
        BackgroundWorker bgw = new BackgroundWorker();

        public Form1()
        {

            InitializeComponent();

        }

        public void button1_Click(object sender, EventArgs e)
        {

            var tabela = ImportLookup.SearchForTableArgs(comboTabela.Text);
            tabela.ExecutarProcedimento(textBox1.Text, textBox3.Text, textBox4.Text,
                textBox5.Text, textBox6.Text, textBox7.Text, textBox8.Text, textBox2.Text, comboTabela.Text);
        }


        //faz a conversão de .fbk para .fdb
        private void button2_Click(object sender, EventArgs e)
        {
            string sysFolder = string.Format(@"C:\FastMigration\");
            Environment.GetFolderPath(Environment.SpecialFolder.System);
            ProcessStartInfo pInfo = new ProcessStartInfo();
            pInfo.FileName = sysFolder + @"\convert.bat";

            Process p = Process.Start(pInfo);

            p.WaitForExit();
            MessageBox.Show("Conversão concluida com sucesso!");
        }

        private void linkLabel1_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)
        {

            Process.Start("http://dontpad.com/asdkhfgjsadjfwoiertheworyherorewiethjwoieury2398yrweighdywpeoith2309ruhw984657weg");
        }

    }
}