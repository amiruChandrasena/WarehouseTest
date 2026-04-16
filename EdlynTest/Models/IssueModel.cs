using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class IssueTransferRMModel
    {
        public int TransferNo { get; set; }
        public string TagId { get; set; }
        public string CatalogCode { get; set; }
        public double IssueQty { get; set; }
        public double AvailableQty { get; set; }
        public double ReqQty { get; set; }
        public int OpenPalletNo { get; set; }
        public int OldPalletNo { get; set; }
        public string Originator { get; set; }
        public string WarehouseCode { get; set; }
        public string ManifestNo { get; set; }
    }

    public class IssuePalletRMModel
    {
        public string Originator { get; set; }
        public int PalletNo { get; set; }
        public string CatalogCode { get; set; }
        public double AvailableQty { get; set; }
        public double IssueQty { get; set; }
        public string BinLocation { get; set; }
        public string LocationIssue { get; set; }
        public string WarehouseFrom { get; set; }
        public string WarehouseTo { get; set; }
    }

    public class IssueRMModel
    {
        public string Originator { get; set; }
        public int PalletNo { get; set; }
        public string CatalogCode { get; set; }
        public double AvailableQty { get; set; }
        public double IssueQty { get; set; }
        public string BinLocation { get; set; }
        public string TagId { get; set; }
        public int BatchNo { get; set; }
        public string BestBefore { get; set; }
        public string WarehouseFrom { get; set; }
        public string WarehouseTo { get; set; }
    }
}
