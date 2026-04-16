using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class TransferPalletBRModel
    {
        public int ManifestNo { get; set; }
        public string WarehouseFrom { get; set; }
        public string WarehouseTo { get; set; }
        public string UserId { get; set; }
        public DateTime ManifestDate { get; set; }
        public List<PalletBRModel> PalletDetails { get; set; }
    }

    public class ReceivePalletBRModel
    {
        public int ManifestNo { get; set; }
        public string WarehouseFrom { get; set; }
        public string WarehouseTo { get; set; }
        public string UserId { get; set; }
        public DateTime ManifestDate { get; set; }
        public List<PalletBRModel> PalletDetails { get; set; }
    }

    public class PalletBRModel
    {
        public int PalletNumber { get; set; }
        public string CatalogCode { get; set; }
        public int OriginalPalletUnits { get; set; }
        public int OldPalletNumber { get; set; }
        public DateTime BestBefore { get; set; }
        public int PalletUnits { get; set; }
        public string BinLocation { get; set; }
        public int BatchNumber { get; set; }
        public int DaysLeft { get; set; }
        public int ManifestNo { get; set; }
        public string Status { get; set; }
        public string SellingCode { get; set; }
    }

    public class ManifestBRModel
    {
        public int ManifestNumber { get; set; }
        public DateTime ManifestDate { get; set; }
        
    }
    
    public class ManifestDetailsBRModel : ManifestBRModel
    {
        public string CatalogCode { get; set; }
        public string PalletNumber { get; set; }
        public int OrigPalletUnits { get; set; }
        public int PalletUnits { get; set; }
        public DateTime BestBefore { get; set; }
        public string OldPalletNumber { get; set; }
        public string BinLocation { get; set; }
        public int DaysLeft { get; set; }
        public string BatchNumber { get; set; }
        public string Status { get; set; }
    }

    public class StockDetailBRModel
    {
        public string CatalogCode { get; set; }
        public string WarehouseCode { get; set; }
        public float TransferQty { get; set; }
        public float OnHandQty { get; set; }
        public int Version { get; set; }
    }

    public class StockMovementBRModel
    {
        public string CatalogCode { get; set; }
        public string WarehouseCode { get; set; }
        public float MoveQty { get; set; }
        public string UserId { get; set; }
        public float OnHandQty { get; set; }
    }

    public class InsertManifestBRModel
    {
        
    }
}



