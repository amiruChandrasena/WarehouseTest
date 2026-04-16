using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class StockDocketModel
    {
        public string CatalogCode { get; set; }
        public string Originator { get; set; }
        public int JobNo { get; set; }
        public string WarehouseId { get; set; }
        public string MoveType { get; set; }
        public double MoveQty { get; set; }
        public double RateTonne { get; set; }
        public double OnHandPre { get; set; }
        public string Narration { get; set; }
        public DateTime MoveDate { get; set; }
    }

    public class StockTransferModel
    {
        public string Originator { get; set; }
        public DateTime MoveDate { get; set; }
        public string MoveType { get; set; }
        public double Cost { get; set; }
        public string Narration { get; set; }
        public string WarehouseFrom { get; set; }
        public string WarehouseTo { get; set; }
        public List<StockTransferDetailModel> StockDetails { get; set; }
    }

    public class StockTransferDetailModel
    {
        public string CatalogCode { get; set; }
        public string BinLocationFrom { get; set; }
        public string BinLocationTo { get; set; }
        public double MoveQty { get; set; }
        public string RowStatus { get; set; }
        public double OnHandFromPre { get; set; }
        public double OnHandToPre { get; set; }
        public double UnitPrice { get; set; }
        
        public string NegTag { get; set; }
        public double OnHandQty { get; set; }
        public double OnHandFrom { get; set; }
        public double OnHandTo { get; set; }
        public double Cost { get; set; }
        public double StockVersion { get; set; }
        public double SvMoveQty { get; set; }
        public string RowState { get; set; }
    }

    public class PuStockDetailModel
    {
        public string BinLoc1 { get; set; }
        public string BinLoc2 { get; set; }
        public string BinLoc3 { get; set; }
        public string CatalogCode { get; set; }
        public double OnHandQty { get; set; }
        public string Status { get; set; }
        public string WarehouseId { get; set; }
    }

    public class PuStockMovementModel
    {
        public string CatalogCode { get; set; }
        public string WarehouseId { get; set; }
        public string BinLocation { get; set; }
        public double MoveQty { get; set; }
        public double OnhandPre { get; set; }
        public DateTime Timestamp { get; set; }
        public DateTime MoveDate { get; set; }
        public string MoveType { get; set; }
        public double OldCost { get; set; }
        public double NewCost { get; set; }
        public string AuthCode { get; set; }
        public string SourceCode { get; set; }
        public int SourceNo { get; set; }
        public string RefCode { get; set; }
        public string Narration { get; set; }
        public string AnalysisCode { get; set; }
        
        public string BatchNo { get; set; }
        public string CatalogCodeT { get; set; }
        public string CustCode { get; set; }
        public string DocNoPeer { get; set; }
        public string DocNoSupp { get; set; }
        public string MoveQtyS { get; set; }
        public string SalesNo { get; set; }
        public string SuppCode { get; set; }
        public string WarehouseIdT { get; set; }
    }
}