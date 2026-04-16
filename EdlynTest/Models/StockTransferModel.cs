using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class StockTransferRMHeaderModel
    {
        public StockTransferRMHeaderModel()
        {
            StockTransferDetails = new List<StockTransferRMDetailModel>();
        }

        public string Originator { get; set; }
        public string AuthCode { get; set; }
        public string CarrierCode { get; set; }
        public DateTime MoveDate { get; set; }
        public string MoveType { get; set; }
        public string Narration { get; set; }
        public string RefCode { get; set; }
        public string Status { get; set; }
        public int TransferNo { get; set; }
        public string WarehouseFrom { get; set; }
        public string WarehouseTo { get; set; }
        public int ManifestNo { get; set; }
        public string Picker { get; set; }
        public int OpenPalletNo { get; set; }
        public List<StockTransferRMDetailModel> StockTransferDetails { get; set; }
    }

    public class StockTransferRMDetailModel
    {
        public string CatalogCode { get; set; }
        public string Description { get; set; }
        public string PartNo { get; set; }
        public double MoveQty { get; set; }
        public double UnitPrice { get; set; }
        public double MoveQtyValue { get; set; }
        public string UomStock { get; set; }
        public int TransferNo { get; set; }
        public double IssueQty { get; set; }
        public double OnHandPreQty { get; set; }
        public double OnHandQtyWh2 { get; set; }
    }

    public class StockTransferRMNewPalletModel
    {
        public string originator { get; set; }
        public string TransitWarehouse { get; set; }
        public string ScannedPalletText { get; set; }
        public int OpenPalletNo { get; set; }
        public int TransferNumber { get; set; }
    }

    public class StockTransferRMClosePalletModel
    {
        public string originator { get; set; }
        public int PalletNo { get; set; }
        public int TransferNo { get; set; }
        public string TagId { get; set; }
    }
}
