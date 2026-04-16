using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class WarehouseRackCycle
    {
        public string SuggestedRack { get; set; }
        public string CountedBy { get; set; }
        public int SystemPalletCount { get; set; }
        public int ActualPalletCount { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime FinalisedTime { get; set; }
        public string Status { get; set; }
    }
}
