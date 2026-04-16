using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PalletLabelModel
    {
        public string CatalogCode { get; set; }
        public DateTime PrintDate { get; set; }
        public string Description { get; set; }
        public int PalletNumber { get; set; }
        public int OldPalletNumber { get; set; }
        public int PlanNumber { get; set; }
        public string LineNumber { get; set; }
        public string Quality { get; set; }
        public string BestBefore { get; set; }
        public string Status { get; set; }
        public int BatchNumber { get; set; }
        public string WarehouseId { get; set; }
        public int PalletUnits { get; set; }
        public int OriginalPalletUnits { get; set; }
        public double PalletUnitsRm { get; set; }
        public double StockQuantity { get; set; }
        public string BinLocation { get; set; }
        public int DaysOld { get; set; }
        public int DaysLeft { get; set; }
        public string StockCount { get; set; }
        public string Uom { get; set; }
    }
}
