using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PalletFilterCriteriaModel
    {
        public string CatalogCode { get; set; }
        public int Rows { get; set; }
        public string Status { get; set; }
        public string WarehouseCode { get; set; }
        public DateTime BestBefore { get; set; }
        public int PalletNo { get; set; }
        public string BatchNo { get; set; }
        public string QualityWh { get; set; }
        public DateTime PrintFrom { get; set; }
        public DateTime PrintTo { get; set; }
        public int PlanNo { get; set; }
        public DateTime BestBeforeFrom { get; set; }
        public string LvBinLocation { get; set; }
        public string LvPickingLabel { get; set; }
    }
}
