using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class ReturnsModel
    {
        public int JobNo { get; set; }
        public string CatalogCode { get; set; }
        public int CostItemNo { get; set; }
        public string TagId { get; set; }
        public string WarehouseId { get; set; }
        public double RetQty { get; set; }
        public string Originator { get; set; }
    }
}
