using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class CycleCountPallet
    {
        public int PalletNumber { get; set; }
        public string CatalogCode { get; set; }
        public string BestBefore { get; set; }
        public string OldBestBefore { get; set; }
        public int PalletUnits { get; set; }
        public int OldPalletUnits { get; set; } 
    }

    public class CycleCountRMPallet
    {
        public int PalletNumber { get; set; }
        public string CatalogCode { get; set; }
        public string Description { get; set; }
        public string Uom { get; set; }
        public string UomOrder { get; set; }
        public decimal OrderConversion { get; set; }
        public decimal StockConversion { get; set; }
        public string BestBefore { get; set; }
        public string OldBestBefore { get; set; }
        public double PalletUnits { get; set; }
        public double OldPalletUnits { get; set; }
        public double StockQuantity { get; set; }
        public double OldStockQuantity { get; set; }
    }
}
