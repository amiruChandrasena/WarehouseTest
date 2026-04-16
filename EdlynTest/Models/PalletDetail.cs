using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PalletDetail
    {
        public int PalletNumber { get; set; }
        public int OldPalletNumber { get; set; }
        public string CatalogCode { get; set; }
        public string CatalogDescription { get; set; }
        public int BatchNumber { get; set; }
        public DateTime BestBefore { get; set; }
        public int OriginalPalletUnits { get; set; }
        public double PalletQuantity { get; set; }
        public int PalletUnits { get; set; }
        public int PicklistNumber { get; set; }
        public CatalogItem CatalogItem { get; set; }

        public string WarehouseId { get; set; } // Add By Irosh 2020/7/16 
        public string ManifestNo { get; set; } // Add By Irosh 2020/7/16 
        public double StockQty { get; set; } // Add By Irosh 2020/6/30
        public double IssueQty { get; set; } // Add By Irosh 2020/7/16 

    }

    //public class IssueTransferPalletDetail
    //{
    //    public int PalletNumber { get; set; }
    //    public int OldPalletNumber { get; set; }
    //    public string CatalogCode { get; set; }
    //    public string CatalogDescription { get; set; }
    //    public int BatchNumber { get; set; }
    //    public DateTime BestBefore { get; set; }
    //    public float OriginalPalletUnits { get; set; }
    //    public float PalletQuantity { get; set; }
    //    public float PalletUnits { get; set; }
    //    public int PicklistNumber { get; set; }
    //    public string WarehouseCode { get; set; }
    //    public string ManifestNo { get; set; }
    //    public float StockQty { get; set; }
    //    public float IssueQty { get; set; }
    //}
}
