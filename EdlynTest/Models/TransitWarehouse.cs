using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class TransitWarehouse
    {
        public string FromWh { get; set; }
        public string ToWh{ get; set; }
        public string TransitWh { get; set; }
        public string ProductionWh { get; set; }
        public string QualityWh { get; set; }
    }
}
