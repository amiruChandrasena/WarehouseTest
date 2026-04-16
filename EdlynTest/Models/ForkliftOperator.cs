using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class ForkliftOperator
    {
        /* properties from database fields */
        public string UserId { get; set; }
        public string Name { get; set; }
        public string EmployeeNo { get; set; }
        public string Password { get; set; }
        public string DefaultWarehouse { get; set; }
        public string DefaultRackingZone { get; set; }

        /* extra properties */
        public bool IsSupervisor { get; set; }
        public bool CanScanWholeRack { get; set; }
    }
}
