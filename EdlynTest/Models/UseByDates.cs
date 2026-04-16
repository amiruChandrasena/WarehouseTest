using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class UseByDates
    {
        public int InvoiceNo { get; set; }
        public int PlistNo { get; set; }
        public string CatalogCode { get; set; }
        public double PalletQty { get; set; }
        public double CartonQty { get; set; }
        public string BatchNo { get; set; }
        public DateTime UseByDate { get; set; }
    }
}
