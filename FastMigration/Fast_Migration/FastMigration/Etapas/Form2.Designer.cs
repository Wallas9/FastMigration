namespace FastMigration
{
    partial class Form2
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.label2 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.anoInicio_txt = new System.Windows.Forms.TextBox();
            this.anoFim_txt = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.Ok_btn = new System.Windows.Forms.Button();
            this.bimestre_rb = new System.Windows.Forms.RadioButton();
            this.trimestre_rb = new System.Windows.Forms.RadioButton();
            this.label5 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.label7 = new System.Windows.Forms.Label();
            this.etapa1_num = new System.Windows.Forms.NumericUpDown();
            this.etapa2_num = new System.Windows.Forms.NumericUpDown();
            ((System.ComponentModel.ISupportInitialize)(this.etapa1_num)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.etapa2_num)).BeginInit();
            this.SuspendLayout();
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.label2.Location = new System.Drawing.Point(17, 9);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(348, 20);
            this.label2.TabIndex = 2;
            this.label2.Text = "Defina os padrões de etapas que deseja migrar:";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.label1.Location = new System.Drawing.Point(5, 50);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(56, 20);
            this.label1.TabIndex = 3;
            this.label1.Text = "Etapa:";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.label3.Location = new System.Drawing.Point(125, 77);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(82, 20);
            this.label3.TabIndex = 5;
            this.label3.Text = "Ano letivo:";
            // 
            // anoInicio_txt
            // 
            this.anoInicio_txt.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.anoInicio_txt.Location = new System.Drawing.Point(129, 105);
            this.anoInicio_txt.Name = "anoInicio_txt";
            this.anoInicio_txt.Size = new System.Drawing.Size(100, 26);
            this.anoInicio_txt.TabIndex = 3;
            // 
            // anoFim_txt
            // 
            this.anoFim_txt.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.anoFim_txt.Location = new System.Drawing.Point(273, 105);
            this.anoFim_txt.Name = "anoFim_txt";
            this.anoFim_txt.Size = new System.Drawing.Size(100, 26);
            this.anoFim_txt.TabIndex = 4;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.label4.Location = new System.Drawing.Point(235, 109);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(32, 20);
            this.label4.TabIndex = 11;
            this.label4.Text = "até";
            // 
            // Ok_btn
            // 
            this.Ok_btn.Location = new System.Drawing.Point(315, 189);
            this.Ok_btn.Name = "Ok_btn";
            this.Ok_btn.Size = new System.Drawing.Size(58, 57);
            this.Ok_btn.TabIndex = 7;
            this.Ok_btn.Text = "OK";
            this.Ok_btn.UseVisualStyleBackColor = true;
            this.Ok_btn.Click += new System.EventHandler(this.Ok_btn_Click);
            // 
            // bimestre_rb
            // 
            this.bimestre_rb.AutoSize = true;
            this.bimestre_rb.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.bimestre_rb.Location = new System.Drawing.Point(9, 77);
            this.bimestre_rb.Name = "bimestre_rb";
            this.bimestre_rb.Size = new System.Drawing.Size(90, 24);
            this.bimestre_rb.TabIndex = 1;
            this.bimestre_rb.TabStop = true;
            this.bimestre_rb.Text = "Bimestre";
            this.bimestre_rb.UseVisualStyleBackColor = true;
            // 
            // trimestre_rb
            // 
            this.trimestre_rb.AutoSize = true;
            this.trimestre_rb.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.trimestre_rb.Location = new System.Drawing.Point(9, 107);
            this.trimestre_rb.Name = "trimestre_rb";
            this.trimestre_rb.Size = new System.Drawing.Size(93, 24);
            this.trimestre_rb.TabIndex = 2;
            this.trimestre_rb.TabStop = true;
            this.trimestre_rb.Text = "Trimestre";
            this.trimestre_rb.UseVisualStyleBackColor = true;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.label5.Location = new System.Drawing.Point(5, 158);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(277, 20);
            this.label5.TabIndex = 13;
            this.label5.Text = "Migrando os diários para o ano atual...";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.label6.Location = new System.Drawing.Point(5, 189);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(220, 20);
            this.label6.TabIndex = 14;
            this.label6.Text = "Primeira etapa a ser migrada?";
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.label7.Location = new System.Drawing.Point(5, 223);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(208, 20);
            this.label7.TabIndex = 16;
            this.label7.Text = "Última etapa a ser migrada?";
            // 
            // etapa1_num
            // 
            this.etapa1_num.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.etapa1_num.Location = new System.Drawing.Point(231, 189);
            this.etapa1_num.Name = "etapa1_num";
            this.etapa1_num.Size = new System.Drawing.Size(51, 26);
            this.etapa1_num.TabIndex = 5;
            // 
            // etapa2_num
            // 
            this.etapa2_num.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.etapa2_num.Location = new System.Drawing.Point(231, 220);
            this.etapa2_num.Name = "etapa2_num";
            this.etapa2_num.Size = new System.Drawing.Size(51, 26);
            this.etapa2_num.TabIndex = 6;
            // 
            // Form2
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(387, 259);
            this.Controls.Add(this.etapa2_num);
            this.Controls.Add(this.etapa1_num);
            this.Controls.Add(this.label7);
            this.Controls.Add(this.label6);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.trimestre_rb);
            this.Controls.Add(this.Ok_btn);
            this.Controls.Add(this.bimestre_rb);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.anoFim_txt);
            this.Controls.Add(this.anoInicio_txt);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.label2);
            this.Name = "Form2";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "FastMigration";
            ((System.ComponentModel.ISupportInitialize)(this.etapa1_num)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.etapa2_num)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Button Ok_btn;
        public System.Windows.Forms.TextBox anoInicio_txt;
        public System.Windows.Forms.TextBox anoFim_txt;
        public System.Windows.Forms.RadioButton bimestre_rb;
        public System.Windows.Forms.RadioButton trimestre_rb;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label7;
        public System.Windows.Forms.NumericUpDown etapa1_num;
        public System.Windows.Forms.NumericUpDown etapa2_num;
    }
}