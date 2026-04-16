using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class JobHeaderRMModel
    {
        public JobHeaderRMModel()
        {
            JobDetails = new List<JobDetailsRMModel>();
        }

        public int JobNo { get; set; }
        public string Allergens { get; set; }
        public DateTime AvailTime { get; set; }
        public int BomVertion { get; set; }
        public string CatalogCode { get; set; }
        public string Ccp { get; set; }
        public int CcpStatus { get; set; }
        public DateTime EndTime { get; set; }
        public int JobSeq { get; set; }
        public DateTime LabelTime { get; set; }
        public DateTime PackTime { get; set; }
        public double PlanQty { get; set; }
        public double PlanQtyUnit { get; set; }
        public string PlanUom { get; set; }
        public string SavedBy { get; set; }
        public DateTime SavedOn { get; set; }
        public int ScheduleNo { get; set; }
        public int ScheduleSeq { get; set; }
        public string SellingCode { get; set; }
        public string ShelfLife { get; set; }
        public DateTime StartTime { get; set; }
        public string Status { get; set; }
        public string UOM { get; set; }
        public string CatalogDesc { get; set; }
        public string ByProductCode { get; set; }
        public double ByProductQty { get; set; }
        public string ByProductUom { get; set; }
        public string JobType { get; set; }
        public string ReWorkNo { get; set; }
        public string LineNo { get; set; }
        public string LineDescription { get; set; }
        public double ActualQtyUnit { get; set; }
        public double ActualQty { get; set; }
        public DateTime PackDate { get; set; }
        public DateTime EndDate { get; set; }
        public string ReworkInstructions { get; set; }
        public double Conversion { get; set; }
        public double Yeild { get; set; }
        public int GLUpdate { get; set; }
        public string BaseUOM { get; set; }
        public string BaseCode { get; set; }
        public double FinishedGoodsQty { get; set; }
        public List<JobDetailsRMModel> JobDetails { get; set; }
    }

    public class JobDetailsRMModel
    {
        public int JobNo { get; set; }
        public double ReqQty { get; set; }
        public string CatalogCode { get; set; }
        public string RMType { get; set; }
        public int CostItemNo { get; set; }
        public int LotId { get; set; }
        public double UsedQty { get; set; }
        public double RateTonne { get; set; }
        public string Uom { get; set; }
        public double RateUom { get; set; }
        public string MixBy { get; set; }
        public string Status { get; set; }
        public string StatusDescription { get; set; }
        public string Description { get; set; }
        public string WarehouseId { get; set; }

    }

    public class JobIssueModel
    {
        public int JobNo { get; set; }
        public int PalletNo { get; set; }
        public string CatalogCode { get; set; }
        public double IssueQty { get; set; }
        public double AvailableQty { get; set; }
        public double ReqQty { get; set; }
        public int CostItemNo { get; set; }
        public string Originator { get; set; }
        public string Currency { get; set; }
    }

    public class JobCostPeriodModel
    {
        public int CostYear { get; set; }
        public int CostPeriod { get; set; }
    }

    public class JobMixHeader
    {
        public JobMixHeader()
        {
            JobMixDetailList = new List<JobMixDetail>();
        }

        public string CatalogCode { get; set; }
        public int JobNo { get; set; }
        public DateTime MixDate { get; set; }
        public int MixNo { get; set; }
        public string Originator { get; set; }
        public string Type { get; set; }
        public List<JobMixDetail> JobMixDetailList { get; set; }
    }

    public class JobMixDetail
    {
        public int MixNo { get; set; }
        public string CatalogCode { get; set; }
        public int JobNo { get; set; }
        public int SplitNo { get; set; }
        public int LotId { get; set; }
        public double ReqQty { get; set; }
        public double IssueQty { get; set; }
    }

    public class JobOrderLogModel
    {
        public int JobNo { get; set; }
        public string Originator { get; set; }
        public string ItemType { get; set; }
        public int CostItemNo { get; set; }
        public string CatalogCode { get; set; }
        public int BatchNo { get; set; }
        public DateTime ActionDate { get; set; }
        public string Action { get; set; }
    }
}
